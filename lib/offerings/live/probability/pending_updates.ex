defmodule Offerings.Live.Probability.PendingUpdates do
  use Agent, restart: :transient

  require Logger

  alias Offerings.Live.Probability.PendingUpdatesReg

  # TODO pending updates _should_ resolve organically, but in case they don't
  #      there should be some sort of bookkeeping that will prune them

  def apply_to(state, actor, handler) do
    case get_for(actor) do
      [] -> state
      pending_updates ->
        stop_for(actor)
        Logger.info("Offerings.Live.Probability.PendingUpdates applying #{length(pending_updates)} updates to actor #{actor}")
        pending_updates |> List.foldl(state, handler)
    end
  end

  def get_for(actor) do
    case Registry.lookup(PendingUpdatesReg, actor) do
      [{pid, _}] ->
        Agent.get(pid, & &1)
        |> Enum.reverse()
      [] -> []
    end
  end

  def start_link({id, msg}), do: Agent.start_link(__MODULE__, :init, [msg], name: via(id), hibernate_after: 15_000)

  def init(msg), do: [msg]

  def stop_for(actor) do
    case Registry.lookup(PendingUpdatesReg, actor) do
      [{pid, _}] ->
        Logger.info("Offerings.Live.Probability.PendingUpdates stopping updates for actor #{actor}")
        Agent.stop(pid)
      [] -> :ok
    end
  end

  defp via(id), do: {:via, Registry, {PendingUpdatesReg, id, DateTime.utc_now}}
end
