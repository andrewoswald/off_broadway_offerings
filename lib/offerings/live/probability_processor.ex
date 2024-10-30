defmodule Offerings.Live.ProbabilityProcessor do

  require Logger

  alias Offerings.Live.Probability.{ActorReg, ActorSup, Event, BetOffer, PendingUpdates, PendingUpdatesReg}

  def process_batch(batch) do
    batch
    |> Enum.sort_by(& &1.data["offset"])
    |> Enum.each(&process(&1.data, &1.batch_key, &1.metadata))
  end

  defp process(%{"eventAdded" => event_added}, actor, msg_meta) do
    event = event_added["event"]
    unless {:update, event, msg_meta} |> sent?(actor) do
      Event |> new!({event, msg_meta})
    end
  end

  defp process(%{"eventStatusUpdated" => event_status_updated}, actor, msg_meta) do
    status = event_status_updated["status"]
    msg = {:status_update, status, msg_meta}
    unless msg |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received eventStatusUpdate [#{status}] for unknown Event #{actor}; msg_flow: #{msg_meta.msg_flow}; queueing update")
      actor |> queue_pending_update(msg)
    end
  end

  defp process(%{"eventUpdated" => event_updated}, actor, msg_meta) do
    event = event_updated["event"]
    unless {:update, event, msg_meta} |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received eventUpdated for unknown Event #{actor}; msg_flow: #{msg_meta.msg_flow}; adding new actor")
      Event |> new!({event, msg_meta})
    end
  end

  defp process(%{"eventDeleted" => _event_deleted}, actor, msg_meta) do
    unless {:delete, msg_meta} |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received eventDeleted for unknown Event #{actor}; msg_flow: #{msg_meta.msg_flow}")
    end
  end

  defp process(%{"betOfferAdded" => bet_offer_added}, actor, msg_meta) do
    bet_offer = bet_offer_added["betOffer"]
    unless {:update, bet_offer, msg_meta} |> sent?(actor) do
      BetOffer |> new!({bet_offer, msg_meta})
    end
  end

  defp process(%{"betOfferUpdated" => bet_offer_updated}, actor, msg_meta) do
    bet_offer = bet_offer_updated["betOffer"]
    unless {:update, bet_offer, msg_meta} |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received betOfferUpdated for unknown BetOffer #{actor}; msg_flow: #{msg_meta.msg_flow}; adding new actor")
      BetOffer |> new!({bet_offer, msg_meta})
    end
  end

  defp process(%{"betOfferLineTypesUpdated" => bet_offer_line_types_updated}, actor, msg_meta) do
    line_types = bet_offer_line_types_updated["lineTypes"]
    msg = {:line_types_update, line_types, msg_meta}
    unless msg |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received betOfferLineTypesUpdated for unknown BetOffer #{actor}; msg_flow: #{msg_meta.msg_flow}; queueing update")
      actor |> queue_pending_update(msg)
    end
  end

  defp process(%{"betOfferStatusUpdated" => bet_offer_status_updated}, actor, msg_meta) do
    status = bet_offer_status_updated["status"]
    msg = {:status_update, status, msg_meta}
    unless msg |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received betOfferStatusUpdated [#{status}] for unknown BetOffer #{actor}; msg_flow: #{msg_meta.msg_flow}; queueing update")
      actor |> queue_pending_update(msg)
    end
  end

  defp process(%{"betOfferDeleted" => _bet_offer_deleted}, actor, msg_meta) do
    unless {:delete, msg_meta} |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received betOfferDeleted for unknown BetOffer #{actor}; msg_flow: #{msg_meta.msg_flow}")
    end
  end

  defp process(%{"outcomeStatusUpdated" => outcome_status_updated}, actor, msg_meta) do
    status = outcome_status_updated["status"]
    outcome_id = outcome_status_updated["outcomeId"]
    msg = {:outcome_status_update, status, outcome_id, msg_meta}
    unless msg |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received outcomeStatusUpdated for unknown BetOffer #{actor}; msg_flow: #{msg_meta.msg_flow}; queueing update")
      actor |> queue_pending_update(msg)
    end
  end

  defp process(%{"outcomeOddsUpdated" => outcome_odds_updated}, actor, msg_meta) do
    odds = outcome_odds_updated["odds"]
    outcome_id = outcome_odds_updated["outcomeId"]
    msg = {:outcome_odds_update, odds, outcome_id, msg_meta}
    unless msg |> sent?(actor) do
      Logger.info("Offerings.Live.ProbabilityProcessor received outcomeOddsUpdated for unknown BetOffer #{actor}; msg_flow: #{msg_meta.msg_flow}; queueing update")
      actor |> queue_pending_update(msg)
    end
  end

  defp process(_data, _actor, _metadata), do: :ok

  defp sent?({:delete, _msg_meta} = payload, actor) do
    case Registry.lookup(ActorReg, actor) do
      [{pid, _}] ->
        # immediately unregister:
        Registry.unregister(ActorReg, actor)
        # but still send the msg so the actor can manage its state:
        GenServer.cast(pid, payload)
        # and since unregister can be "delayed", ensure its effect:
        ensure_unregistered(actor, Registry.lookup(ActorReg, actor))
      [] -> PendingUpdates.stop_for(actor); false
    end
  end

  defp sent?(payload, actor) do
    case Registry.lookup(ActorReg, actor) do
      [{pid, _}] -> GenServer.cast(pid, payload); true
      [] -> false
    end
  end

  defp ensure_unregistered(actor, [{_pid, _}]) do
    ensure_unregistered(actor, Registry.lookup(ActorReg, actor))
  end

  defp ensure_unregistered(_actor, []), do: true

  defp new!(module, term) do
    # Start an actor presumed to be registered under a unique key.
    DynamicSupervisor.start_child(ActorSup, {module, term})
  end

  defp queue_pending_update(actor, msg) do
    case Registry.lookup(PendingUpdatesReg, actor) do
      [{pid, _}] -> Agent.cast(pid, &([msg | &1]))
      [] -> PendingUpdates |> new!({actor, msg})
    end
  end
end
