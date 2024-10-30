defmodule Offerings.Live.ProbabilitySup do
  use Supervisor

  alias Offerings.Live.Probability.{PendingUpdatesReg, ActorReg, PubSubReg, ActorSup}

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: PendingUpdatesReg},
      {Registry, keys: :unique, name: ActorReg},
      {Registry, keys: :duplicate, name: PubSubReg},
      {DynamicSupervisor, strategy: :one_for_one, max_restarts: 0, name: ActorSup}
    ]

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 0)
  end
end
