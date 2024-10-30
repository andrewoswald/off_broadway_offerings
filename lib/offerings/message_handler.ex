defmodule Offerings.MessageHandler do
  require Logger

  alias Offerings.Bookkeeping

  @session_welcome ~s({"type":"SESSION_WELCOME")
  @session_initialized ~s({"type":"SESSION_INITIALIZED")
  @session_snapshot_completed ~s({"type":"SESSION_SNAPSHOT_COMPLETED")
  @session_replay_completed ~s({"type":"SESSION_REPLAY_COMPLETED")
  @session_ping ~s({"type":"SESSION_PING")
  @session_pong ~s({"type":"SESSION_PONG")
  @session_error ~s({"type":"SESSION_ERROR")

  # (see send_payload/2 below)
  @canned_ping ~s({"type":"SESSION_PING","sessionPing":{"id":"png-test"}})
  @canned_ping_len <<byte_size(@canned_ping)::32>>

  def handle_message(<<@session_welcome, _::binary>> = msg, %{jwt: jwt, socket: socket}) do
    Logger.info("WELCOME MESSAGE: #{msg}")
    # respond w/ the SESSION_INITIALIZE command...
    jwt
    |> session_init_command
    |> Jason.encode!
    |> send_payload(socket)
    |> case do
      :ok -> Logger.info("sent SESSION_INITIALIZE command")
    end
  end

  def handle_message(<<@session_initialized, _::binary>> = msg, %{socket: socket, producer: producer}) do
    Logger.info("SESSION INITIALIZED: #{msg}")
    # if we've got an offset, attempt a replay, otherwise request a snapshot...
    Bookkeeping.get_offset |> start_msg_flow(producer, socket)
  end

  def handle_message(<<@session_snapshot_completed, _::binary>> = msg, %{producer: producer}) do
    Logger.info("SESSION SNAPSHOT COMPLETED: #{msg}")
    GenStage.cast(producer, {:msg_flow, :replay})
    msg
    |> Jason.decode!
    |> Map.fetch!("sessionSnapshotCompleted")
    |> Map.fetch!("offset")
    |> Bookkeeping.set_offset
  end

  def handle_message(<<@session_replay_completed, _::binary>> = msg, %{producer: producer}) do
    Logger.info("SESSION REPLAY COMPLETED: #{msg}")
    GenStage.cast(producer, {:msg_flow, :realtime})
    msg
    |> Jason.decode!
    |> Map.fetch!("sessionReplayCompleted")
    |> Map.fetch!("offset")
    |> Bookkeeping.set_offset
  end

  def handle_message(<<@session_ping, _::binary>> = msg, %{socket: socket}) do
    ping_time = DateTime.utc_now()
    Logger.debug("SESSION_PING: #{msg}")
    msg
    |> Jason.decode!
    |> Map.fetch!("sessionPing")
    |> Map.fetch!("id")
    |> pong_command
    |> Jason.encode!
    |> send_payload(socket)
    |> case do
      :ok -> Logger.debug("sent SESSION_PONG command")
    end
    Bookkeeping.set_last_ping(ping_time)
  end

  def handle_message(<<@session_pong, _::binary>>, _), do: :ok

  def handle_message(<<@session_error, _::binary>> = msg, %{socket: socket, producer: producer}) do
    # msg's "code" will be one of "INVALID_PROTOCOL_VERSION", "INVALID_OFFERING", "INVALID_TOKEN", or
    # "OFFSET_TOO_OLD".  If it's "INVALID_TOKEN", simply close the socket which will have the effect of
    # going back to the "jwt" state.  If too old message, invoke the snapshot.  Any other errors indicate
    # we've got an issue with canned initialization data and are thusly doomed.
    msg
    |> Jason.decode!
    |> Map.fetch!("sessionError")
    |> Map.fetch!("code")
    |> case do
      "INVALID_TOKEN" -> :ssl.close(socket)
      "OFFSET_TOO_OLD" ->
        Logger.info("received offset too old indication; about to send snapshot command...")
        GenStage.cast(producer, {:msg_flow, :snapshot})
        snapshot_command()
        |> Jason.encode!
        |> send_payload(socket)
        |> case do
          :ok -> Logger.info("sent SESSION_SNAPSHOT command")
        end
      _ ->
        Logger.critical("Terminating application due to SESSION_ERROR: #{msg}")
        System.stop()
    end
  end

  def handle_message(message, %{producer: producer}), do: GenStage.cast(producer, {:add, message, DateTime.utc_now})

  defp start_msg_flow([offset: offset], producer, socket) do
    GenStage.cast(producer, {:msg_flow, :replay})
    offset
    |> replay_command
    |> Jason.encode!
    |> send_payload(socket)
    |> case do
      :ok -> Logger.info("sent SESSION_REPLAY command")
    end
  end

  defp start_msg_flow([], producer, socket) do
    GenStage.cast(producer, {:msg_flow, :snapshot})
    snapshot_command()
    |> Jason.encode!
    |> send_payload(socket)
    |> case do
      :ok -> Logger.info("sent SESSION_SNAPSHOT command")
    end
  end

  defp send_payload(payload, socket) do
    # NOTE: this is a bit of a hack, but it seems despite having nagle turned off and 'psh' bit set to true,
    #       tiny messages such as snapshot and pong simply don't make it to the server.  Wireshark as well as
    #       output from the 'log_level' socket option set to 'debug' indicate the packet as being sent, but my
    #       guess is that it's still lingering in the OS' socket buffer.  I didn't get a chance to mess around
    #       with socket buffer sizes or attempting to manipulate the tcp window size.  The hack here is to
    #       arbitrarily attach a client sourced ping command to any outgoing commands, which has the effect of
    #       forcing the requests to the server.
    :ssl.send(socket, :zlib.gzip([<<byte_size(payload)::32>>, payload, @canned_ping_len, @canned_ping]))
  end

  defp session_init_command(jwt) do
    %{type: "SESSION_INITIALIZE", sessionInitialize: %{
      protocolVersion: "V1",
      offerings: [
        # This stanza contained client specifics which I cannot include.
      ],
      token: jwt
    }}
  end

  defp snapshot_command, do: %{type: "SESSION_SNAPSHOT", sessionSnapshot: %{}}

  defp replay_command(offset), do: %{type: "SESSION_REPLAY", sessionReplay: %{offset: offset}}

  defp pong_command(id), do: %{type: "SESSION_PONG", sessionPong: %{id: id}}
end
