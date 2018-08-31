defmodule Cluster.Strategy.Postgres do
  @moduledoc """
  config :libcluster,
    topologies: [
      postgres_example: [
        strategy: #{__MODULE__},
        config: [
          hostname: "yourdbhost",
          database: "example",
          username: "username",
          password: "password",
        ]
  """

  use Supervisor

  alias Postgrex.Notifications
  alias Cluster.Strategy.State
  alias Cluster.Strategy.Postgres.Worker
  alias Cluster.Strategy.Postgres.Backoff

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init([%State{config: config} = state]) do
    connection = __MODULE__.Connection
    notifications = __MODULE__.Notifications
    meta = %{
      connection: connection,
      notifications: notifications,
    }
    state = %State{state | :meta => meta}

    {:ok, _} = Application.ensure_all_started(:postgrex)

    children = [
      worker(Postgrex, [Keyword.put(config, :name, connection)]),
      worker(Backoff, [Notifications, Keyword.put(config, :name, notifications)], id: :notifications),
      worker(Backoff, [Worker, state], id: :worker)
    ]

    supervise(children, strategy: :one_for_all)
  end
end
