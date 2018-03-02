defmodule Cluster.Strategy.PostgresTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  def connect(caller, result \\ true, node) do
    send(caller, {:connect, node})
    result
  end

  def disconnect(caller, result \\ true, node) do
    send(caller, {:disconnect, node})
    result
  end

  def list_nodes(nodes) do
    nodes
  end

  def start do
    start_supervised!({
      Cluster.Strategy.Postgres,
      [
        topology: [],
        connect: {__MODULE__, :connect, [self()]},
        disconnect: {__MODULE__, :disconnect, [self()]},
        list_nodes: {__MODULE__, :list_nodes, [[]]},
        config: [
          hostname: "localhost",
          database: "libcluster_postgres_test",
          username: "libcluster_postgres",
          password: "libcluster_postgres"
        ]
      ]
    })
  end

  @cmds [
    ["-c", "DROP DATABASE IF EXISTS libcluster_postgres_test;"],
    ["-c", "CREATE DATABASE libcluster_postgres_test;"],
    ["-d", "libcluster_postgres_test", "-c", "DROP ROLE IF EXISTS libcluster_postgres;"],
    ["-d", "libcluster_postgres_test", "-c", "CREATE USER libcluster_postgres;"]
  ]
  @psql_env Map.put_new(System.get_env(), "PGUSER", "postgres")

  setup_all do
    for cmd <- @cmds do
      {_, 0} = System.cmd("psql", cmd, stderr_to_stdout: true, env: @psql_env)
    end

    :ok
  end

  test "notifies the channel" do
    capture_log(fn ->
      start()

      {:ok, _} =
        Postgrex.Notifications.listen(
          Cluster.Strategy.Postgres.Notifications,
          "cluster"
        )

      heartbeat = "heartbeat::#{node()}"
      assert_receive {:notification, _, _, "cluster", ^heartbeat}
    end)
  end

  test "connects on notification" do
    capture_log(fn ->
      start()

      {:ok, _} =
        Postgrex.query(
          Cluster.Strategy.Postgres.Connection,
          "NOTIFY cluster, 'heartbeat::testhost@testnode'",
          []
        )

      assert_receive {:connect, :testhost@testnode}
    end)
  end
end
