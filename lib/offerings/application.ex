defmodule Offerings.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @default_cacertfile "/etc/ssl/certs/ca-certificates.crt"
  @message_pipeline Offerings.MessagePipeline
  @live_probability_pipeline Offerings.Live.ProbabilityPipeline

  alias Offerings.{Bookkeeping, BookkeepingSup, Connection, Live}

  @impl true
  def start(_type, _args) do
    priv_dir = :code.priv_dir(:off_broadway_offerings)

    cxn_opts = [
      auth_url: Application.fetch_env!(:off_broadway_offerings, :auth_url),
      cacertfile: Application.get_env(:off_broadway_offerings, :cacertfile, @default_cacertfile),
      keyfile: Path.join([priv_dir, Application.fetch_env!(:off_broadway_offerings, :key_pem)]),
      certfile: Path.join([priv_dir, Application.fetch_env!(:off_broadway_offerings, :cert_pem)]),
      replica_host: Application.fetch_env!(:off_broadway_offerings, :replica_host),
      replica_port: Application.fetch_env!(:off_broadway_offerings, :replica_port),
      pipeline: @message_pipeline
    ]

    bookkeeping_start_spec = {
      Supervisor,
      :start_link,
      [
        [Bookkeeping],
        [strategy: :one_for_one]
      ]
    }

    children = [
      # Start the Telemetry supervisor
      OfferingsWeb.Telemetry,
      # Start the PubSub system
      {Phoenix.PubSub, name: Offerings.PubSub},
      # Start the Endpoint (http/https)
      # OfferingsWeb.Endpoint,
      # Pipeline for surfacing enriched, real-time messages to Kinesis (the :offerings_stream)
      {@live_probability_pipeline, [offerings_stream: Application.fetch_env!(:off_broadway_offerings, :offerings_stream)]},
      # A :one_for_all w/ max_restarts 0, consisting of actor registry and pub-sub, and dynamic supervisor
      Live.ProbabilitySup,
      # A supervisor at :one_for_one for bookkeeping endeavors
      %{id: BookkeepingSup, start: bookkeeping_start_spec, type: :supervisor},
      # Pipeline for Kambi ORA message processing
      @message_pipeline,
      # State machine for managing the communication w/ Kambi
      %{id: Connection, start: {Connection, :start_link, [cxn_opts]}}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :rest_for_one, name: Offerings.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  # @impl true
  # def config_change(changed, _new, removed) do
  #   OfferingsWeb.Endpoint.config_change(changed, removed)
  #   :ok
  # end
end
