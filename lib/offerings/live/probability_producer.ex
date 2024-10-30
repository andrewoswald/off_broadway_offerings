defmodule Offerings.Live.ProbabilityProducer do
  use GenStage
  require Logger

  @behaviour Broadway.Producer

  @impl GenStage
  def init(initial_state) do
    Logger.info("ProbabilityProducer init")
    Phoenix.PubSub.subscribe(Offerings.PubSub, "live_probability")
    {:producer, initial_state}
  end

  @impl GenStage
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({id, data}, state) do
    {:noreply, [{id, data}], state}
  end
end
