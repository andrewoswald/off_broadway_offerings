defmodule Offerings.MessagePipeline do
  use Broadway

  require Logger

  @producer Offerings.MessageProducer

  alias Broadway.Message
  alias Offerings.{Bookkeeping, Live}

  def start_link(_opts) do
    options = [
      name: __MODULE__,
      producer: [
        module: {@producer, []},
        concurrency: 1
      ],
      processors: [
        default: [
          # concurrency: System.schedulers_online(),
          concurrency: 4,
          min_demand: 0,
          max_demand: 1
        ]
      ],
      batchers: [
        default: [
          # concurrency: System.schedulers_online()
          concurrency: 8
        ]
      ],
    ]

    Broadway.start_link(__MODULE__, options)
  end

  @impl Broadway
  def handle_message(_processor, message, _context) do
    message
    |> Message.update_data(&Jason.decode!/1)
    |> then(& %{&1 | metadata: Map.put(&1.metadata, :offset, &1.data["offset"])})
    |> then(&Message.put_batch_key(&1, actor_id(&1.data)))
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, _context) do
    # It is within this function that you do things such as
    # send data to Kinesis, write contents to a file for bulk
    # db insert, populate Phoenix Channels for soft-realtime
    # updates, etc.  The concurrency: value (above, in the
    # "batchers:" configuration stanza) controls how many
    # processes are dedicated to handling, in this case, the
    # default batcher.
    messages
    |> tap(&Live.ProbabilityProcessor.process_batch/1)
  end

  def ack(:realtime, successful, _failed) do
    successful
    |> List.last()
    |> Map.fetch!(:data)
    |> Map.fetch!("offset")
    |> Bookkeeping.set_offset()
  end

  defp actor_id(%{"eventAdded" => event_added}), do: event_added["event"]["id"]
  defp actor_id(%{"eventUpdated" => event_updated}), do: event_updated["event"]["id"]

  defp actor_id(%{"eventDeleted" => event_deleted}), do: event_deleted["id"]
  defp actor_id(%{"eventStatusUpdated" => event_status_updated}), do: event_status_updated["id"]
  defp actor_id(%{"eventCashOutStatusUpdated" => event_cash_out_status_updated}), do: event_cash_out_status_updated["id"]
  defp actor_id(%{"eventTagsUpdated" => event_tags_updated}), do: event_tags_updated["id"]

  defp actor_id(%{"eventParticipantsUpdated" => event_participants_updated}), do: event_participants_updated["eventId"]
  defp actor_id(%{"eventScoreUpdated" => event_score_updated}), do: event_score_updated["eventId"]
  defp actor_id(%{"matchClockUpdated" => match_clock_updated}), do: match_clock_updated["eventId"]

  defp actor_id(%{"betOfferAdded" => bet_offer_added}), do: bet_offer_added["betOffer"]["id"]
  defp actor_id(%{"betOfferUpdated" => bet_offer_updated}), do: bet_offer_updated["betOffer"]["id"]

  defp actor_id(%{"betOfferStatusUpdated" => bet_offer_status_updated}), do: bet_offer_status_updated["id"]
  defp actor_id(%{"betOfferLineTypesUpdated" => bet_offer_line_types_updated}), do: bet_offer_line_types_updated["id"]
  defp actor_id(%{"betOfferCashOutStatusUpdated" => bet_offer_cash_out_status_updated}), do: bet_offer_cash_out_status_updated["id"]

  defp actor_id(%{"betOfferDeleted" => bet_offer_deleted}), do: bet_offer_deleted["betOfferId"]
  defp actor_id(%{"outcomeOddsUpdated" => outcome_odds_updated}), do: outcome_odds_updated["betOfferId"]
  defp actor_id(%{"outcomeStatusUpdated" => outcome_status_updated}), do: outcome_status_updated["betOfferId"]
  defp actor_id(%{"outcomeCashoutStatusUpdated" => outcome_cashout_status_update}), do: outcome_cashout_status_update["betOfferId"]
end
