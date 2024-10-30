defmodule Offerings.Live.Probability.BetOffer do
  use GenServer, restart: :transient
  require Logger

  alias Offerings.Live.Probability.{ActorReg, PubSubReg, Event, PendingUpdates}

  @status_open "OPEN"
  @status_closed "CLOSED"
  @status_suspended "SUSPENDED"

  @bet_offer_opened "bet_offer_opened"
  @bet_offer_closed "bet_offer_closed"

  # offering criterion IDs:
  @handicap 1001159512 # bet_offer_type_name: Handicap, criterion_name: Handicap - Including Overtime
  @money_line 1001159732 # bet_offer_type_name: 1 x 2, criterion_name: Including Overtime
  @over_under 1001159509 # bet_offer_type_name: Over/Under, criterion_name: Total Points - Including Overtime

  defstruct [:bet_offer, :event_group_id, :event_status, outcomes: %{}]

  def start_link({bet_offer, msg_meta}) do
    GenServer.start_link(__MODULE__, {bet_offer, msg_meta}, name: via(bet_offer["id"]), hibernate_after: 15_000)
  end

  @impl GenServer
  def init({bet_offer, msg_meta}) do
    # register for PubSub event notifications
    Registry.register(PubSubReg, bet_offer["eventId"], bet_offer["id"])
    # Don't block the call longer than necessary; run business logic as a continue.
    {:ok, %__MODULE__{}, {:continue, {:new, bet_offer, msg_meta}}}
  end

  @impl GenServer
  def handle_continue({:new, bet_offer, msg_meta}, state) do
    state = do_handle({:update, bet_offer, msg_meta}, state)

    if bet_offer["status"] == "OPEN" do
      @bet_offer_opened |> surface_outcome_odds(state, msg_meta)
    end

    state =
      case Event.get_event(bet_offer["eventId"]) do
        {:ok, nil} -> state
        {:ok, event} -> %{state | event_group_id: event["group"]["id"], event_status: event["status"]}
      end
      |> PendingUpdates.apply_to(bet_offer["id"], &do_handle/2)

    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:delete, _msg_meta}, state), do: {:stop, :normal, state}

  def handle_cast(msg, state), do: {:noreply, do_handle(msg, state)}

  @impl GenServer
  def handle_info({:event, event, _msg_meta}, state) do
    {:noreply, %{state | event_group_id: event["group"]["id"], event_status: event["status"]}}
  end

  def handle_info({:event_status, status, _msg_meta}, state) do
    {:noreply, put_in(state.event_status, status)}
  end

  defp do_handle({:update, bet_offer, _msg_meta}, state) do
    {bet_offer_outcomes, bet_offer} = bet_offer |> Map.pop!("outcomes")
    outcomes = bet_offer_outcomes |> Map.new(& {&1["id"], &1})
    # NOTE: merge since we observed an outcome_odds_update for unknown outcome
    %{state | bet_offer: bet_offer, outcomes: Map.merge(state.outcomes, outcomes)}
  end

  defp do_handle({:line_types_update, line_types, _msg_meta}, state) do
    put_in(state.bet_offer["lineType"], line_types)
  end

  defp do_handle({:status_update, status, msg_meta}, state) do
    if status != state.bet_offer["status"] do
      maybe_surface_outcome_odds(status, state, msg_meta)
      put_in(state.bet_offer["status"], status)
    else
      state
    end
  end

  defp do_handle({:outcome_status_update, status, outcome_id, _msg_meta}, state) do
    if Map.has_key?(state.outcomes, outcome_id) do
      put_in(state.outcomes[outcome_id]["status"], status)
    else
      Logger.warning("Offerings.Live.Probability.BetOffer #{state.bet_offer["id"]} outcomeStatusUpdate for unknown Outcome #{outcome_id}; adding what we know")
      outcome = %{"betOfferId" => state.bet_offer["id"], "id" => outcome_id, "status" => status, "odds" => []}
      %{state | outcomes: Map.put(state.outcomes, outcome_id, outcome)}
    end
  end

  defp do_handle({:outcome_odds_update, odds, outcome_id, msg_meta}, state) do
    if state.event_status == "STARTED", do: maybe_surface_in_game_odds_update(odds, outcome_id, msg_meta, state)

    if Map.has_key?(state.outcomes, outcome_id) do
      put_in(state.outcomes[outcome_id]["odds"], odds)
    else
      Logger.warning("Offerings.Live.Probability.BetOffer #{state.bet_offer["id"]} outcomeOddsUpdate for unknown Outcome #{outcome_id}; adding what we know")
      outcome = %{"betOfferId" => state.bet_offer["id"], "id" => outcome_id, "odds" => odds, "status" => nil}
      %{state | outcomes: Map.put(state.outcomes, outcome_id, outcome)}
    end
  end

  defp maybe_surface_in_game_odds_update(odds, outcome_id, msg_meta, state) do
    # event_status has already been established as "STARTED"
    bet_offer = state.bet_offer
    if is_mainline?(bet_offer["lineType"])
    and "OFFERED_LIVE" in bet_offer["tags"]
    and bet_offer["criterion"]["id"] in [@handicap, @over_under, @money_line] do
      # surface the in-game odds movement
      # Heads up!  If the internal usage of the shape_outcome_odds/4 "offerings" list parameter changes, this will break:
      outcomes = [%{"id" => outcome_id, "odds" => odds}]
      shaped_outcome_odds = shape_outcome_odds("in_game_odds_update", bet_offer, outcomes, msg_meta)
      surface({bet_offer["id"], shaped_outcome_odds})
    end
    :ok
  end

  defp maybe_surface_outcome_odds(@status_open, state, msg_meta) do
    surface_outcome_odds(@bet_offer_opened, state, msg_meta)
  end

  defp maybe_surface_outcome_odds(@status_closed, state, msg_meta) do
    surface_outcome_odds(@bet_offer_closed, state, msg_meta)
  end

  defp maybe_surface_outcome_odds(@status_suspended, _state, _msg_meta), do: :ok

  defp surface_outcome_odds(type, state, msg_meta) do
    shaped_outcome_odds = shape_outcome_odds(type, state.bet_offer, Map.values(state.outcomes), msg_meta)
    surface({state.bet_offer["id"], shaped_outcome_odds})
  end

  defp surface({id, data}, topic \\ "live_probability"), do: Phoenix.PubSub.local_broadcast(Offerings.PubSub, topic, {id, data})

  defp shape_outcome_odds(type, bet_offer, outcomes, msg_meta) do
    %{"type" => type,
      "event_id" => bet_offer["eventId"],
      "bet_offer_id" => bet_offer["id"],
      "criterion_id" => bet_offer["criterion"]["id"],
      "criterion_label" => bet_offer["criterion"]["label"]["defaultTranslation"],
      "is_mainline" => is_mainline?(bet_offer["lineType"]),
      "outcomes" => shape_outcomes(outcomes),
      "msg_flow" => msg_meta.msg_flow,
      "msg_datetime" => msg_meta.msg_datetime
    }
  end

  defp shape_outcomes(outcomes) do
    outcomes |> Enum.map(&  %{
      "outcome_id" => &1["id"],
      "odds" => shape_odds(&1["odds"]),
      "outcome_label" => get_in(&1, ["label", "defaultTranslation"]),
    })
  end

  defp shape_odds(nil), do: []
  defp shape_odds(outcome_odds) do
    Enum.map(outcome_odds, & %{
      "offerings" => &1["offerings"],
      "decimal" => get_in(&1, ["odds", "decimal"]),
      "jurisdiction_mask" => get_jurisdiction_mask(&1["offerings"])
    })
  end

  defp get_jurisdiction_mask(offerings) do
    Enum.reduce(offerings, 0, fn
      # Substituting client details w/ `jurisdictionN`, but the general idea is
      "jurisdiction1", mask -> mask + 1
      "jurisdiction2", mask -> mask + 2
      "jurisdiction3", mask -> mask + 4
      "jurisdiction4", mask -> mask + 8
      "jurisdiction5", mask -> mask + 16
      # etc.
    end)
  end

  defp is_mainline?(nil), do: false
  defp is_mainline?(line_types), do: "MAIN_LINE" in line_types

  defp via(offer_id) do
    {:via, Registry, {ActorReg, offer_id}}
  end
end
