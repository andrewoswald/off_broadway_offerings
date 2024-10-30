defmodule Offerings.Live.Probability.Event do
  use GenServer, restart: :transient

  require Logger

  alias Offerings.Live.Probability.{ActorReg, PubSubReg, PendingUpdates}

  defstruct [:event, :participants_hash]

  def start_link({event, msg_meta}) do
    GenServer.start_link(__MODULE__, {event, msg_meta}, name: via(event["id"]), hibernate_after: 15_000)
  end

  def get_event(id) do
    case Registry.lookup(ActorReg, id) do
      [{pid, _}] -> GenServer.call(pid, :event)
      [] -> {:ok, nil}
    end
  end

  @impl GenServer
  def init({event, msg_meta}) do
    # Don't block the call longer than necessary; run business logic as a continue.
    {:ok, %__MODULE__{}, {:continue, {:new, event, msg_meta}}}
  end

  @impl GenServer
  def handle_continue({:new, event, msg_meta}, state) do
    state =
      do_handle({:update, event, msg_meta}, state)
      |> PendingUpdates.apply_to(event["id"], &do_handle/2)

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:event, _from, state) do
    {:reply, {:ok, state.event}, state}
  end

  @impl GenServer
  def handle_cast({:delete, _msg_meta}, state), do: {:stop, :normal, state}

  def handle_cast(msg, state), do: {:noreply, do_handle(msg, state)}

  defp do_handle({:update, event, msg_meta}, state) do
    participants_hash = :erlang.phash2(event["participants"])
    if state.event == nil do
      surface_event_data(event, msg_meta)
    else
      if event["startsAt"] != state.event["startsAt"]
      or event["sport"] != state.event["sport"]
      or event["group"]["id"] != state.event["group"]["id"]
      or participants_hash != state.participants_hash do
        surface_event_data(event, msg_meta)
      end
    end
    publish_update({:event, event, msg_meta}, event["id"])
    %{state | event: event, participants_hash: participants_hash}
  end

  defp do_handle({:status_update, status, msg_meta}, state) do
    publish_update({:event_status, status, msg_meta}, state.event["id"])
    put_in(state.event["status"], status)
  end

  defp surface_event_data(event, msg_meta) do
    event_id = event["id"]
    surface({
      event_id,
      %{"type" => "event_data",
        "event_id" => event_id,
        "competition_name" => get_in(event, ["competitionName", "defaultTranslation"]),
        "starts_at" => event["startsAt"],
        "group_id" => event["group"]["id"],
        "sport" => event["sport"],
        "participants" => shape_participants(event["participants"]),
        "msg_flow" => msg_meta.msg_flow,
        "msg_datetime" => msg_meta.msg_datetime
      }
    })
  end

  defp surface({id, data}, topic \\ "live_probability"), do: Phoenix.PubSub.local_broadcast(Offerings.PubSub, topic, {id, data})

  defp shape_participants(nil), do: []
  defp shape_participants(participants) do
    Enum.map(participants, fn participant ->
      %{"participant_id" => participant["id"],
        "participant_name" => participant["name"]["defaultTranslation"],
        "participant_type" => participant["type"],
        "is_home_participant" => participant["homeParticipant"]
      }
    end)
  end

  defp publish_update(update, event_id) do
    Registry.dispatch(PubSubReg, event_id, fn entries ->
      for {pid, _} <- entries, do: send(pid, update)
    end)
  end

  defp via(event_id) do
    {:via, Registry, {ActorReg, event_id}}
  end
end
