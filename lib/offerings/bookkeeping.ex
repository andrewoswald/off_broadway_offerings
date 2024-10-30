defmodule Offerings.Bookkeeping do
  use GenServer

  require Logger

  defstruct [:last_ping, stagnant_cxns: []]

  @recurring_task :recurring_task
  @task_frequency 25_000

  alias Offerings.Connection

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def get_offset do
    :ets.lookup(__MODULE__, :offset)
  end

  def set_offset(offset) do
    :ets.insert(__MODULE__, {:offset, offset})
    :ok
  end

  def set_last_ping(time) do
    GenServer.cast(__MODULE__, {:last_ping, time})
  end

  def get_last_ping do
    GenServer.call(__MODULE__, :last_ping)
  end

  def get_stagnant_cxn_times do
    GenServer.call(__MODULE__, :stagnant_cxns)
  end

  @impl GenServer
  def init(_args) do
    :ets.new(__MODULE__, [:named_table, :public])
    schedule_recurring_task()
    {:ok, %__MODULE__{}}
  end

  @impl GenServer
  def handle_cast({:last_ping, time}, state) do
    state = %{state | last_ping: time}
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:last_ping, _from, state) do
    {:reply, state.last_ping, state}
  end

  @impl GenServer
  def handle_call(:stagnant_cxns, _from, state) do
    {:reply, state.stagnant_cxns, state}
  end

  @impl GenServer
  def handle_info(@recurring_task, state) do
    # if stagnant cxn, state's stagnant_cxns list get updated w/ timestamp
    state = maybe_handle_stagnant_cxn(state)
    schedule_recurring_task()
    {:noreply, state}
  end

  defp maybe_handle_stagnant_cxn(%{last_ping: last_ping} = state) when last_ping != nil do
    utc_now = DateTime.utc_now()
    # check if the last ping is older than 20 seconds (2x the SLA)
    if DateTime.diff(utc_now, last_ping) > 20 do
      # stagnant connection; stop it to have the supervisor restart things..
      Logger.warning("Bookkeeping detected stagnant connection")
      Connection.stop()
      %{state | stagnant_cxns: [utc_now | state.stagnant_cxns], last_ping: nil}
    else
      state
    end
  end

  defp maybe_handle_stagnant_cxn(state), do: state

  defp schedule_recurring_task, do: Process.send_after(self(), @recurring_task, @task_frequency)
end
