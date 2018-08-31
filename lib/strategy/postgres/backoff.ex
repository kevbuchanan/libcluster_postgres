defmodule Cluster.Strategy.Postgres.Backoff do
  use GenServer

  @default_backoff 5_000

  def start_link(mod, opts) do
    GenServer.start_link(__MODULE__, {mod, opts})
  end

  def init({mod, opts}) do
    Process.flag(:trap_exit, true)

    state = %{mod: mod, opts: opts, backoff: @default_backoff}

    case mod.start_link(opts) do
      {:ok, pid} ->
        {:ok, Map.put(state, :pid, pid)}

      error ->
        {:ok, backoff(error, state)}
    end
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:noreply, backoff(reason, state)}
  end

  def handle_info({:stop, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(msg, state) do
    send(state.pid, msg)
    {:noreply, state}
  end

  def terminate(_, %{pid: nil}), do: :ok

  def terminate(reason, %{pid: pid}) do
    Process.exit(pid, reason)

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      100 ->
        Process.exit(pid, :kill)

        receive do
          {:EXIT, ^pid, _} -> :ok
        end
    end
  end

  defp backoff(error, state) do
    Process.send_after(self(), {:stop, error}, state.backoff)
    Map.put(state, :pid, nil)
  end
end
