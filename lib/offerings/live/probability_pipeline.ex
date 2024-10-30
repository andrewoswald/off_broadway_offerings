defmodule Offerings.Live.ProbabilityPipeline do
  use Broadway

  require Logger

  alias Broadway.Message
  alias ExAws.Kinesis
  alias Offerings.Live.ProbabilityProducer

  def start_link(opts) do
    offerings_stream = Keyword.fetch!(opts, :offerings_stream)
    Logger.info("Kinesis OFFERINGS STREAM: #{offerings_stream}")
    options = [
      name: __MODULE__,
      producer: [
        module: {ProbabilityProducer, []},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []}
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
        kinesis: [
          # concurrency: System.schedulers_online()
          concurrency: 4,
          batch_size: 500
        ]
      ],
      context: %{offerings_stream: offerings_stream}
    ]

    Broadway.start_link(__MODULE__, options)
  end

  @impl Broadway
  def handle_message(_processor, message, _context) do
    message
    |> Message.update_data(& {elem(&1, 0), Jason.encode!(elem(&1, 1))})
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, context) do
    messages
    |> tap(&to_kinesis(&1, context.offerings_stream))
  end

  def transform(data, _options) do
    %Message{
      data: data,
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      batcher: :kinesis
    }
  end

  defp to_kinesis(messages, stream_name) do
    messages
    |> Stream.map(& &1.data)
    |> Stream.map(& %{partition_key: Integer.to_string(elem(&1, 0)), data: elem(&1, 1)})
    |> Enum.to_list
    |> then(&Kinesis.put_records(stream_name, &1))
    |> ExAws.request!
  end
end
