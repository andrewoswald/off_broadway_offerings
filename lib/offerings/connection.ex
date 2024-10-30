defmodule Offerings.Connection do
  @behaviour :gen_statem

  require Logger

  defstruct [
    :auth_url,
    :http_opts,
    :jwt,
    :replica_host,
    :replica_port,
    :stream_handler,
    :socket,
    :socket_opts,
    :producer
  ]

  @recv_timeout 5_000

  alias Offerings.{MessageHandler, StreamHandler}

  def start_link(opts) do
    auth_url = Keyword.fetch!(opts, :auth_url)
    cacertfile = Keyword.fetch!(opts, :cacertfile)
    keyfile = Keyword.fetch!(opts, :keyfile)
    certfile = Keyword.fetch!(opts, :certfile)
    replica_host = Keyword.fetch!(opts, :replica_host)
    replica_port = Keyword.fetch!(opts, :replica_port)
    pipeline = Keyword.fetch!(opts, :pipeline)
    :gen_statem.start_link(__MODULE__, {auth_url, cacertfile, keyfile, certfile, replica_host, replica_port, pipeline}, [])
  end

  def stop do
    :gen_statem.stop(:cxn, :normal, 5000)
  end

  @impl :gen_statem
  def callback_mode() do
    # meaning, whatever state happens to be active, that's the
    # function that will be invoked (pattern match on specifics)
    :state_functions
  end

  @impl :gen_statem
  def init({auth_url, cacertfile, keyfile, certfile, replica_host, replica_port, pipeline}) do
    Process.register(self(), :cxn)
    message_producer =
      pipeline
      |> Broadway.producer_names
      |> List.first
    Logger.info("Kambi ORA message pipeline Broadway producer: #{message_producer}")

    socket_opts = [
      :binary,
      reuse_sessions: true,
      nodelay: true,
      active: false,
      verify: :verify_peer,
      cacertfile: cacertfile,
      customize_hostname_check: [
        match_fun: fn
          {:dns_id, ^replica_host}, {:dNSName, '*.kambi.com'} -> true
          _, _ -> :default
        end
      ],
      log_level: :info
    ]

    http_opts = [
      ssl: [
        verify: :verify_peer,
        cacertfile: cacertfile |> String.to_charlist(),
        certfile: certfile |> String.to_charlist(),
        keyfile: keyfile |> String.to_charlist(),
        customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
      ],
      timeout: 4_000
    ]

    data = %__MODULE__{
      auth_url: auth_url,
      http_opts: http_opts,
      replica_host: replica_host,
      replica_port: replica_port,
      socket_opts: socket_opts,
      producer: message_producer
    }

    actions = [{:next_event, :internal, :jwt}]
    {:ok, :disconnected, data, actions}
  end

  def disconnected(:internal, :jwt, data) do
    Logger.info("state is disconnected; action is jwt")
    request = {data.auth_url, [{'Connection', 'close'}]}
    case :httpc.request(:get, request, data.http_opts, []) do
      {:ok, {_status_line, _headers, jwt}} ->
        data = %{data | jwt: to_string(jwt)}
        actions = [{:next_event, :internal, :connect}]
        {:keep_state, data, actions}

      {:error, reason} ->
        # NOTE: logging the reason may crash the process, so pipe it ot IO.inspect/2
        reason |> IO.inspect(label: "Error retrieving JWT; are you on VPN or whitelisted ip addr?")
        actions = [{{:timeout, :jwt}, 1_000, nil}]
        {:keep_state_and_data, actions}
    end
  end

  def disconnected({:timeout, :jwt}, _content, data) do
    actions = [{:next_event, :internal, :jwt}]
    {:keep_state, data, actions}
  end

  def disconnected(:internal, :connect, data) do
    Logger.info("state is disconnected; action is connect")

    case :ssl.connect(data.replica_host, data.replica_port, data.socket_opts, 5_000) do
      {:ok, socket} ->
        msg_handler_args = %{socket: socket, jwt: data.jwt, producer: data.producer}
        stream_handler = StreamHandler.init(&MessageHandler.handle_message(&1, msg_handler_args))

        data = %{data | socket: socket, stream_handler: stream_handler}
        actions = [{:next_event, :cast, :consume}]
        {:next_state, :connected, data, actions}

      {:error, error} ->
        # perhaps do an incremental backoff here...
        Logger.error("Connection failed: #{:inet.format_error(error)}")
        actions = [{{:timeout, :reconnect}, 1_000, nil}]
        {:keep_state_and_data, actions}
    end
  end

  def disconnected({:timeout, :reconnect}, _content, data) do
    actions = [{:next_event, :internal, :connect}]
    {:keep_state, data, actions}
  end

  def connected(:cast, :consume, data) do
    case :ssl.recv(data.socket, 0, @recv_timeout) do
      {:ok, packet} ->
        :gen_statem.cast(self(), :consume)
        {:keep_state, %{data | stream_handler: data.stream_handler.(packet)}}

      {:error, :timeout} ->
        :gen_statem.cast(self(), :consume)
        :keep_state_and_data

      {:error, error} ->
        Logger.error("Connected connection failed: #{:inet.format_error(error)} #{error}")
        actions = [{:next_event, :internal, :jwt}]
        {:next_state, :disconnected, data, actions}
    end
  end
end
