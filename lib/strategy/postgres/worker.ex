defmodule Cluster.Strategy.Postgres.Worker do
  use GenServer
  use Cluster.Strategy

  alias Postgrex.Notifications
  alias Cluster.Logger
  alias Cluster.Strategy.State

  @default_channel "cluster"

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(%State{config: config, meta: meta} = state) do
    channel = Keyword.get(config, :channel, @default_channel)
    notifications = Map.fetch!(meta, :notifications)
    subscription = Notifications.listen!(notifications, channel, timeout: 10_000)
    meta =
      meta
      |> Map.put(:channel, channel)
      |> Map.put(:subscription, subscription)

    {:ok, %State{state | :meta => meta}, 0}
  end

  def handle_info(:timeout, state) do
    Process.send_after(self(), :heartbeat, 0)
    Process.send_after(self(), :sync, 0)
    {:noreply, state}
  end

  def handle_info(:heartbeat, %State{meta: %{connection: connection, channel: channel}} = state) do
    Logger.debug(state.topology, "heartbeat")
    :ok = notify(state.topology, connection, channel)
    Process.send_after(self(), :heartbeat, :rand.uniform(5_000))
    {:noreply, state}
  end

  # We have to do this because Azure PostgreSQL doesn't push notifications
  # to the client for some reason. Sending anything to the server will cause
  # the notifications to be pushed, but Postgrex.Notifications only exposes listen,
  # so we force a LISTEN query to an usused channel.
  # https://github.com/elixir-ecto/postgrex/issues/375
  def handle_info(:sync, %State{meta: %{notifications: notifications}} = state) do
    case Notifications.listen(notifications, "sync") do
      {:ok, subscription} ->
        Notifications.unlisten(notifications, subscription)

      {:error, reason} ->
        Logger.info(state.topology, "sync failed: #{reason}")
    end

    Process.send_after(self(), :sync, :rand.uniform(5_000))
    {:noreply, state}
  end

  def handle_info({:notification, _, _, _channel, payload}, state) do
    handle_heartbeat(state, payload)
    {:noreply, state}
  end

  def terminate(_type, _reason, %State{
        meta: %{notifications: notifications, subscription: subscription}
      }) do
    Notifications.unlisten(notifications, subscription)
    :ok
  end

  defp heartbeat(node_name) do
    "heartbeat::#{node_name}"
  end

  defp notify(topology, connection, channel) do
    payload = heartbeat(node())

    case Postgrex.query(connection, "NOTIFY #{channel}, '#{payload}'", []) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.info(topology, "notify failed: #{reason}")
        :ok
    end
  rescue
    e ->
      Logger.info(topology, "notify failed: #{inspect(e)}")
      :ok
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
