defmodule Offerings.MessageProducer do
  use GenStage
  require Logger

  @behaviour Broadway.Producer

  defstruct msg_flow: :replay

  # message handler handles snapshot completed offset assignment..
  @snapshot_acknowledger {Broadway.NoopAcknowledger, nil, nil}
  # message handler handles replay completed offset assignment..
  @replay_acknowledger {Broadway.NoopAcknowledger, nil, nil}
  # message pipeline ack/3 handles realtime offset assignment..
  @realtime_acknowledger {Offerings.MessagePipeline, :realtime, nil}

  @impl GenStage
  def init(_initial_state) do
    Logger.info("MessageProducer init")
    {:producer, %__MODULE__{}, buffer_size: 25_000}
  end

  @impl GenStage
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_cast({:msg_flow, flow}, state) when flow in [:replay, :snapshot, :realtime] do
    {:noreply, [], put_in(state.msg_flow, flow)}
  end

  def handle_cast({:add, msg, msg_datetime}, %{msg_flow: :snapshot} = state) do
    {:noreply, [transform(msg, @snapshot_acknowledger, :snapshot, msg_datetime)], state}
  end

  def handle_cast({:add, msg, msg_datetime}, %{msg_flow: :replay} = state) do
    {:noreply, [transform(msg, @replay_acknowledger, :replay, msg_datetime)], state}
  end

  def handle_cast({:add, msg, msg_datetime}, %{msg_flow: :realtime} = state) do
    {:noreply, [transform(msg, @realtime_acknowledger, :realtime, msg_datetime)], state}
  end

  defp transform(msg, acknowledger, msg_flow, msg_datetime) do
    %Broadway.Message{
      data: msg,
      acknowledger: acknowledger,
      metadata: %{msg_flow: msg_flow, msg_datetime: msg_datetime}
    }
  end
end
