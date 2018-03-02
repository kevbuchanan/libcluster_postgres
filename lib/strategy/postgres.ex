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

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    connection = __MODULE__.Connection
    notifications = __MODULE__.Notifications

    worker_opts =
      opts
      |> Keyword.put(:connection, connection)
      |> Keyword.put(:notifications, notifications)

    {:ok, _} = Application.ensure_all_started(:postgrex)

    children = [
      worker(Postgrex, [Keyword.put(config, :name, connection)]),
      worker(Postgrex.Notifications, [Keyword.put(config, :name, notifications)]),
      worker(__MODULE__.Worker, [worker_opts])
    ]

    supervise(children, strategy: :rest_for_one)
  end

  defmodule Worker do
    use GenServer
    use Cluster.Strategy

    alias Cluster.Logger
    alias Cluster.Strategy.State

    @default_channel "cluster"

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def init(opts) do
      state = %State{
        topology: Keyword.fetch!(opts, :topology),
        connect: Keyword.fetch!(opts, :connect),
        disconnect: Keyword.fetch!(opts, :disconnect),
        list_nodes: Keyword.fetch!(opts, :list_nodes),
        config: Keyword.fetch!(opts, :config)
      }

      connection = Keyword.fetch!(opts, :connection)
      notifications = Keyword.fetch!(opts, :notifications)
      channel = Keyword.get(state.config, :channel, @default_channel)

      subscription = Postgrex.Notifications.listen!(notifications, channel)

      meta = %{
        connection: connection,
        notifications: notifications,
        channel: channel,
        subscription: subscription
      }

      {:ok, %{state | :meta => meta}, 0}
    end

    def handle_info(:timeout, state), do: handle_info(:heartbeat, state)

    def handle_info(:heartbeat, %State{meta: %{connection: connection, channel: channel}} = state) do
      Logger.debug(state.topology, "heartbeat")
      payload = heartbeat(node())
      {:ok, _} = Postgrex.query(connection, "NOTIFY #{channel}, '#{payload}'", [])
      Process.send_after(self(), :heartbeat, :rand.uniform(5_000))
      {:noreply, state}
    end

    def handle_info({:notification, _, _, _channel, payload}, state) do
      handle_heartbeat(state, payload)
      {:noreply, state}
    end

    def terminate(_type, _reason, %State{
          meta: %{notifications: notifications, subscription: subscription}
        }) do
      Postgrex.Notifications.unlisten(notifications, subscription)
      :ok
    end

    defp heartbeat(node_name) do
      "heartbeat::#{node_name}"
    end

    defp handle_heartbeat(state, "heartbeat::" <> rest) do
      self = node()

      case :"#{rest}" do
        ^self ->
          :ok

        n ->
          Logger.debug(state.topology, "received heartbeat from #{n}")
          Cluster.Strategy.connect_nodes(state.topology, state.connect, state.list_nodes, [n])
          :ok
      end
    end

    defp handle_heartbeat(_state, _packet) do
      :ok
    end
  end
end
