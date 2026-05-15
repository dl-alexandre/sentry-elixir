defmodule Sentry.Test.Registry do
  @moduledoc false

  use GenServer

  require Logger

  # Bypass and Plug.Conn may not be available at compile time (optional deps).
  @compile {:no_warn_undefined,
            [Bypass, Bypass.Instance, Bypass.Supervisor, Plug.Conn, NimbleOwnership]}

  @ownership_server Sentry.Test.OwnershipServer
  @scope_key :sentry_test_scope

  # Single merged ETS table replacing the previous
  # `:sentry_test_scope_allows` (allowed_pid -> owner_pid) and
  # `:sentry_test_allowed_pid_processor_routing` (allowed_pid ->
  # processor_name) tables. Rows are 3-tuples
  # `{allowed_pid, owner_pid_or_nil, processor_name_or_nil}`. The 3-tuple
  # shape keeps `:ets.match_delete` patterns simple.
  @routing_table :sentry_test_pid_routing

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link([] = _opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc false
  @spec default_dsn :: String.t() | nil
  def default_dsn do
    :persistent_term.get(:sentry_test_default_bypass_dsn, nil)
  end

  @doc """
  Atomic claim of `allowed_pid` for `owner_pid`'s scope. Backed by
  `NimbleOwnership.allow/4` against the `:sentry_test_scope` key — the
  ownership server serializes the conflict check, so two concurrent
  async tests cannot both pass a check-and-then-write race for the
  same `allowed_pid`.

  `mode`:
    * `:strict` — return `{:error, {:taken, existing_owner}}` when a
      live peer scope already owns `allowed_pid` (used by the public
      `Sentry.Test.Config.allow/2`, surfaced as `Scope.AllowConflictError`).
    * `:soft`   — return `:skipped` in the same situation (used by the
      auto-allow of globally-supervised pids in `Config.put/1`).

  Idempotent: re-claiming a pid you already own returns `:ok`.

  This routes through the Registry GenServer so that owner monitoring
  (for ETS row cleanup on owner DOWN) and the cache-row write happen
  atomically with the NimbleOwnership claim.
  """
  @spec claim_allow(pid(), pid(), :strict | :soft) ::
          :ok | :skipped | {:error, {:taken, pid()}}
  def claim_allow(owner_pid, allowed_pid, mode)
      when is_pid(owner_pid) and is_pid(allowed_pid) and mode in [:strict, :soft] do
    GenServer.call(__MODULE__, {:claim_allow, owner_pid, allowed_pid, mode})
  end

  @doc """
  Ensures `owner_pid` is monitored by the registry so that the
  `:DOWN` handler runs cleanup (routing-table prune + scope-state
  erase via `Sentry.Test.Scope.Registry.handle_owner_down/1`) when
  the owner exits. Idempotent.

  Called from `Sentry.Test.Scope.Registry.update/1` on first scope
  creation so cleanup does not depend on `claim_allow` ever being
  invoked for this owner.
  """
  @spec monitor_owner(pid()) :: :ok
  def monitor_owner(owner_pid) when is_pid(owner_pid) do
    GenServer.call(__MODULE__, {:monitor_owner, owner_pid})
  end

  @doc """
  Direct ETS read of the owner that has allowed `allowed_pid`. Returns
  the owner pid if it is still alive and still owns the entry, or `nil`
  for missing/stale claims. Reads bypass the GenServer because ETS
  lookups are atomic and need to be cheap on the config read path.
  """
  @spec lookup_allow_owner(pid()) :: pid() | nil
  def lookup_allow_owner(allowed_pid) when is_pid(allowed_pid) do
    case :ets.whereis(@routing_table) do
      :undefined ->
        nil

      _ref ->
        case :ets.lookup(@routing_table, allowed_pid) do
          [{^allowed_pid, owner, _processor}] when is_pid(owner) ->
            if Process.alive?(owner), do: owner, else: nil

          _ ->
            nil
        end
    end
  end

  @doc """
  Tags `allowed_pid` so that buffered events (logs, metrics) emitted
  from it are routed to `processor_name` rather than the global
  `Sentry.TelemetryProcessor`. Written by `allow_sentry_reports/2`
  and consulted by `Sentry.TelemetryProcessor.processor_name/0`.

  Updates the existing routing row's processor field; if no row exists
  yet (defensive), inserts a row with `nil` owner. Direct ETS write —
  atomic, no GenServer round-trip.
  """
  @spec tag_processor_for(pid(), atom()) :: :ok
  def tag_processor_for(allowed_pid, processor_name)
      when is_pid(allowed_pid) and is_atom(processor_name) do
    if :ets.whereis(@routing_table) != :undefined do
      unless :ets.update_element(@routing_table, allowed_pid, {3, processor_name}) do
        :ets.insert(@routing_table, {allowed_pid, nil, processor_name})
      end
    end

    :ok
  end

  @doc """
  Returns the per-test processor name that should receive buffered
  events from `allowed_pid`, or `nil` if the pid is not tagged or
  the routing table is not started (production).
  """
  @spec lookup_processor_for(pid()) :: atom() | nil
  def lookup_processor_for(allowed_pid) when is_pid(allowed_pid) do
    case :ets.whereis(@routing_table) do
      :undefined ->
        nil

      _ ->
        case :ets.lookup(@routing_table, allowed_pid) do
          [{^allowed_pid, _owner, processor_name}]
          when is_atom(processor_name) and not is_nil(processor_name) ->
            processor_name

          _ ->
            nil
        end
    end
  end

  @doc """
  Clears the processor field on every routing row that points at
  `processor_name`. Used by `setup_collector/1`'s `on_exit/1` so a
  test that exits before its allowed pids do does not leave stale
  routing rows pointing at a stopped per-test processor. The owner
  field is preserved so the allow remains intact (subsequent
  buffered events from those pids fall back to the global
  processor — matching pre-change behaviour).
  """
  @spec drop_processor_routing_for(atom()) :: :ok
  def drop_processor_routing_for(processor_name) when is_atom(processor_name) do
    if :ets.whereis(@routing_table) != :undefined do
      ms = [{{:"$1", :"$2", processor_name}, [], [{{:"$1", :"$2", nil}}]}]
      _ = :ets.select_replace(@routing_table, ms)
    end

    :ok
  end

  @impl true
  def init(nil) do
    _routing_table = :ets.new(@routing_table, [:named_table, :public, :set])
    maybe_start_default_bypass()
    {:ok, %{owner_monitors: %{}}}
  end

  # Serialization note: every claim funnels through this single named
  # GenServer and holds it across TWO blocking round-trips to the
  # ownership server — `ensure_scope_owner/1`'s
  # `NimbleOwnership.get_and_update/4` and `NimbleOwnership.allow/4`.
  # This is the deliberate price of atomicity (no two concurrent async
  # tests can both pass a check-then-write race for the same
  # `allowed_pid`). It is acceptable because claims happen at test
  # setup, not per event, and the hot config/buffer read paths
  # (`lookup_allow_owner/1`, `lookup_processor_for/1`) bypass this
  # GenServer with lock-free direct ETS reads.
  @impl true
  def handle_call({:claim_allow, owner_pid, allowed_pid, mode}, _from, state) do
    state = ensure_owner_monitored(state, owner_pid)

    reply =
      case ensure_scope_owner(owner_pid) do
        {:error, {:taken, existing_owner}} ->
          if mode == :strict, do: {:error, {:taken, existing_owner}}, else: :skipped

        :ok ->
          case NimbleOwnership.allow(@ownership_server, owner_pid, allowed_pid, @scope_key) do
            :ok ->
              upsert_owner(allowed_pid, owner_pid)
              :ok

            {:error, %{reason: {:already_allowed, ^owner_pid}}} ->
              upsert_owner(allowed_pid, owner_pid)
              :ok

            {:error, %{reason: {:already_allowed, other}}} ->
              if mode == :strict, do: {:error, {:taken, other}}, else: :skipped

            {:error, %{reason: :already_an_owner}} ->
              # `allowed_pid` is itself a scope owner — treat as a conflict.
              if mode == :strict, do: {:error, {:taken, allowed_pid}}, else: :skipped

            {:error, %{reason: :not_allowed}} ->
              if mode == :strict, do: {:error, {:taken, allowed_pid}}, else: :skipped
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:monitor_owner, owner_pid}, _from, state) do
    state = ensure_owner_monitored(state, owner_pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    if :ets.whereis(@routing_table) != :undefined do
      :ets.match_delete(@routing_table, {:_, pid, :_})
    end

    Sentry.Test.Scope.Registry.handle_owner_down(pid)

    {:noreply, %{state | owner_monitors: Map.delete(state.owner_monitors, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private helpers

  defp ensure_owner_monitored(%{owner_monitors: monitors} = state, pid) do
    if Map.has_key?(monitors, pid) do
      state
    else
      ref = Process.monitor(pid)
      %{state | owner_monitors: Map.put(monitors, pid, ref)}
    end
  end

  # Lazily registers `owner_pid` as the NimbleOwnership owner of
  # `:sentry_test_scope` so subsequent `NimbleOwnership.allow/4` calls
  # against this owner succeed even when the test never went through
  # `Sentry.Test.setup_collector/1` (e.g. a test that uses
  # `Sentry.Test.Config.put/1` standalone). When the owner already
  # owns the key, the existing metadata is preserved.
  #
  # INVARIANT: the `:sentry_test_scope` key's metadata is overloaded —
  # `Sentry.Test.setup_collector/1` stores the per-test collector ETS
  # table name (an atom) under it, while this function stores a bare
  # `%{}` marker for collector-less scopes. `Sentry.Test`'s
  # `owner_collecting?/1` distinguishes the two purely by value type
  # (atom = collecting, map = not). Therefore the update fun below MUST
  # preserve an existing value (`current -> {:ok, current}`) and MUST
  # NOT overwrite it with `%{}`; doing so would silently turn a
  # collecting scope into a non-collecting one with no type error.
  defp ensure_scope_owner(owner_pid) do
    case NimbleOwnership.get_and_update(
           @ownership_server,
           owner_pid,
           @scope_key,
           # Metadata MUST be non-nil so that NimbleOwnership treats
           # `owner_pid` as a key owner (its `cond` in `allow/4` checks
           # truthiness of the metadata). Preserve any existing value
           # (see the INVARIANT above — never clobber a collector atom).
           fn
             nil -> {:ok, %{}}
             current -> {:ok, current}
           end
         ) do
      {:ok, _} ->
        :ok

      {:error, %{reason: {:already_allowed, existing_owner}}} ->
        {:error, {:taken, existing_owner}}

      {:error, _} ->
        :ok
    end
  end

  defp upsert_owner(allowed_pid, owner_pid) do
    unless :ets.update_element(@routing_table, allowed_pid, {2, owner_pid}) do
      :ets.insert(@routing_table, {allowed_pid, owner_pid, nil})
    end

    :ok
  end

  # Starts a global Bypass instance that acts as a silent HTTP sink for all tests.
  # This ensures every test has a valid DSN even without calling setup_sentry/1,
  # preserving backward compatibility where capture_* returns {:ok, ""}.
  #
  # In test mode we always override any externally-configured DSN (for example
  # one leaking in from the SENTRY_DSN environment variable), so that running
  # the test suite can never accidentally ship synthetic events to a real
  # Sentry endpoint. When an override happens, we emit a Logger.warning so the
  # developer sees exactly what is being replaced and why.
  defp maybe_start_default_bypass do
    if Code.ensure_loaded?(Bypass) do
      {:ok, _apps} = Application.ensure_all_started(:bypass)

      {:ok, pid} =
        DynamicSupervisor.start_child(
          Bypass.Supervisor,
          Bypass.Instance.child_spec([])
        )

      port = Bypass.Instance.call(pid, :port)
      bypass = struct!(Bypass, pid: pid, port: port)

      # Stub with empty ID to match master's {:ok, ""} return value
      Bypass.stub(bypass, "POST", "/api/1/envelope/", fn conn ->
        Plug.Conn.resp(conn, 200, ~s<{"id": ""}>)
      end)

      dsn_string = "http://public:secret@localhost:#{port}/1"
      maybe_warn_about_dsn_override(dsn_string)

      :persistent_term.put(:sentry_test_default_bypass_dsn, dsn_string)
      Sentry.put_config(:dsn, dsn_string)
    end
  end

  @doc false
  @spec maybe_warn_about_dsn_override(String.t()) :: :ok
  def maybe_warn_about_dsn_override(new_dsn) do
    case Sentry.Config.dsn() do
      %Sentry.DSN{original_dsn: existing} ->
        Logger.warning("""
        [Sentry] test_mode is enabled but a DSN was already configured \
        (#{inspect(existing)}). Overriding it with the local Bypass sink at \
        #{new_dsn} to prevent test events from being sent to a real Sentry \
        endpoint. If this DSN came from the SENTRY_DSN environment variable, \
        unset it for test runs or set :dsn explicitly in your test config.\
        """)

        :ok

      nil ->
        :ok

      _other ->
        :ok
    end
  end
end
