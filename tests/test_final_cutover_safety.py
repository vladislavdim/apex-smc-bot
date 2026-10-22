import asyncio
import math
import signal
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

from apex.app.runtime import RuntimeSupervisor
from apex.app.shutdown import ShutdownDependencies, shutdown_production
from apex.risk.clusters import assess_cluster_exposure


def returns_rows(changes, boundary=1800000000 // 3600 * 3600):
    price = 100
    rows = []
    for i, value in enumerate(changes):
        price *= math.exp(value)
        rows.append(
            {
                "timestamp": boundary - (len(changes) - i) * 3600,
                "close": price,
                "is_closed": True,
            }
        )
    return rows


def test_cluster_counts_observed_correlated_exposure_and_unknown_conservatively():
    boundary = 1800000000 // 3600 * 3600
    x = [math.sin(i * 1.7) * 0.01 + math.cos(i * 0.3) * 0.003 for i in range(170)]
    y = [v * 2 for v in x]
    data = {"BTC": returns_rows(x, boundary), "ETH": returns_rows(y, boundary)}
    assessed = assess_cluster_exposure(
        "BTC", {"ETH": 12}, as_of=boundary, candles=data.__getitem__
    )
    assert assessed.risk_quote == 12
    assert assessed.evidence["status"] == "OBSERVED"
    assert assessed.evidence["correlations"]["BTC:ETH"] > 0.99
    unknown = assess_cluster_exposure(
        "BTC", {"ETH": 12}, as_of=boundary, candles=lambda _: []
    )
    assert unknown.risk_quote == 12
    assert unknown.evidence["status"] == "CONSERVATIVE_UNKNOWN"


def test_cluster_separates_unrelated_observed_returns_without_hedge_credit():
    boundary = 1800000000 // 3600 * 3600
    x = [math.sin(i * 1.7) * 0.01 for i in range(170)]
    y = [math.cos(i * 0.3) * 0.01 for i in range(170)]
    data = {"BTC": returns_rows(x, boundary), "OTHER": returns_rows(y, boundary)}
    assessed = assess_cluster_exposure(
        "BTC", {"OTHER": 12}, as_of=boundary, candles=data.__getitem__
    )
    assert assessed.risk_quote == 0
    data["OTHER"] = returns_rows([-v for v in x], boundary)
    assert (
        assess_cluster_exposure(
            "BTC", {"OTHER": 12}, as_of=boundary, candles=data.__getitem__
        ).risk_quote
        == 12
    )


def test_stopped_runtime_does_not_restore_test_compatibility_authority():
    runtime = RuntimeSupervisor()
    runtime.activate()
    runtime.set_instance_lease(
        1, (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
    )
    assert runtime.owns_exchange_management
    runtime.stop()
    assert not runtime.owns_exchange_management
    assert not runtime.allows_new_entries
    runtime.evaluate_readiness()
    assert not runtime.owns_exchange_management


def test_shutdown_deadline_retains_lease_while_shielded_job_is_alive():
    async def verify():
        gate = asyncio.Event()
        released = AsyncMock()

        async def drain():
            await gate.wait()

        runtime = RuntimeSupervisor()
        runtime.activate()
        started = time.monotonic()
        result = await shutdown_production(
            ShutdownDependencies(
                runtime,
                "unused",
                "test",
                drain=drain,
                timeout_seconds=0.15,
                drain_seconds=0.03,
                release_lease=released,
            ),
            "SIGTERM",
        )
        assert time.monotonic() - started < 0.4
        assert "drain" in result.timed_out
        assert not result.lease_released
        released.assert_not_awaited()
        assert not runtime.owns_exchange_management
        gate.set()
        await asyncio.sleep(0.01)

    asyncio.run(verify())


def test_real_sigterm_budget_exits_despite_slow_executor_and_keeps_lease(tmp_path):
    marker = tmp_path / "lease-released"
    script = textwrap.dedent("""
        import asyncio,os,signal,time
        from pathlib import Path
        from apex.app.runtime import RuntimeSupervisor
        from apex.app.shutdown import ShutdownDependencies,shutdown_production
        async def main():
            event=asyncio.Event()
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM,event.set)
            print('ready',flush=True)
            await event.wait()
            async def drain(): await asyncio.to_thread(time.sleep,60)
            async def release(): Path(MARKER).write_text('unsafe release')
            runtime=RuntimeSupervisor();runtime.activate()
            await shutdown_production(ShutdownDependencies(runtime,'unused','child',
                drain=drain,release_lease=release,timeout_seconds=0.2,drain_seconds=0.05,
                hard_exit=lambda:os._exit(0)),'SIGTERM')
        asyncio.run(main())
    """).replace("MARKER", repr(str(marker)))
    child = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "ready"
        started = time.monotonic()
        child.send_signal(signal.SIGTERM)
        child.communicate(timeout=3)
        assert child.returncode == 0 and time.monotonic() - started < 2
        assert not marker.exists()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def test_shutdown_failed_checkpoint_cannot_release_lease():
    async def verify():
        released = AsyncMock()
        result = await shutdown_production(
            ShutdownDependencies(
                RuntimeSupervisor(),
                "unused",
                "test",
                backup=AsyncMock(return_value=False),
                release_lease=released,
                timeout_seconds=1,
            ),
            "SIGTERM",
        )
        assert not result.completed
        assert not result.lease_released
        released.assert_not_awaited()

    asyncio.run(verify())


def test_execution_admission_applies_actual_cluster_budget(monkeypatch):
    from apex.execution import orders
    from apex.execution.orders import ExecutionConfig

    repository = Mock()
    repository.risk_exposure.return_value = {
        "portfolio_risk_usdt": 8,
        "same_side_risk_usdt": 8,
    }
    repository.risk_by_symbol.return_value = {"ETHUSDT": 8}
    monkeypatch.setattr(orders, "_execution_state_repository", lambda: repository)
    monkeypatch.setattr(orders, "_CLUSTER_CANDLES", lambda symbol: [])
    candidate = {
        "symbol": "BTCUSDT",
        "strategy": "MTF",
        "direction": "BULLISH",
        "entry": 100,
        "sl": 99,
        "tp": 103,
        "rr": 3,
    }
    decision, _ = orders._v3_risk_admission(
        candidate,
        42,
        ExecutionConfig(max_cluster_risk_pct=0.6),
        equity_quote=1000,
        daily_loss_locked=False,
    )
    assert decision.decision == "BLOCK"
    assert "CLUSTER_RISK_LIMIT" in decision.reason_codes
    assert candidate["_v3_cluster_risk"]["risk_pct"] == 0.8
    assert candidate["_v3_cluster_risk"]["status"] == "CONSERVATIVE_UNKNOWN"
