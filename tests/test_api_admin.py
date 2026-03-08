from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.api.app import create_app
from src.streaming.state_store import SnapshotStore, build_snapshot_payload


def _write_api_config(tmp_path: Path, snapshot_path: Path) -> str:
    cfg = tmp_path / "test_api.yaml"
    cfg.write_text(
        "\n".join(
            [
                "api:",
                "  start_background_stream: false",
                "security:",
                "  admin_api_key: test-key",
                "state:",
                "  restore_on_startup: true",
                "  snapshot:",
                "    enabled: true",
                f"    path: {snapshot_path}",
                "    interval_seconds: 60",
            ]
        ),
        encoding="utf-8",
    )
    return str(cfg)


def test_state_and_health_expose_restore_and_runtime_fields(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshots" / "state.json"
    config_path = _write_api_config(tmp_path, snapshot_path)

    app = create_app(config_path)
    state = app.state.service  # type: ignore[attr-defined]
    state.threshold.threshold = 0.8123
    state.runtime.dropped_events_total = 7
    state.runtime.offered_events_total = 100
    store = SnapshotStore(path=str(snapshot_path))
    store.save(build_snapshot_payload(state))

    app = create_app(config_path)
    with TestClient(app) as client:
        health = client.get("/health")
        state_resp = client.get("/state")

    assert health.status_code == 200
    assert state_resp.status_code == 200
    health_body = health.json()
    state_body = state_resp.json()
    assert health_body["restored_state"] is True
    assert health_body["last_snapshot_time"] is not None
    assert state_body["restored_state"] is True
    assert "threshold" in state_body
    assert "anomaly_rate" in state_body
    assert "drift_active" in state_body
    assert "queue_depth" in state_body
    assert "processing_lag_p95_seconds" in state_body
    assert state_body["dropped_events_total"] == 7


def test_admin_endpoints_require_api_key_and_emit_state_changes(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshots" / "state.json"
    config_path = _write_api_config(tmp_path, snapshot_path)
    app = create_app(config_path)

    with TestClient(app) as client:
        unauthorized = client.post("/admin/freeze_adaptation")
        assert unauthorized.status_code == 401

        freeze = client.post(
            "/admin/freeze_adaptation?reason=incident",
            headers={"x-api-key": "test-key", "x-admin-actor": "ops-user"},
        )
        assert freeze.status_code == 200
        assert freeze.json()["adaptation_frozen"] is True
        assert freeze.json()["actor"] == "ops-user"

        state_after_freeze = client.get("/state").json()
        assert state_after_freeze["adaptation_frozen"] is True

        refresh = client.post(
            "/admin/refresh_reference?reason=postmortem-approved",
            headers={"x-api-key": "test-key", "x-admin-actor": "ops-user"},
        )
        assert refresh.status_code == 200
        assert refresh.json()["reference_refreshed"] is True

        unfreeze = client.post(
            "/admin/unfreeze_adaptation?reason=incident-resolved",
            headers={"x-api-key": "test-key", "x-admin-actor": "ops-user"},
        )
        assert unfreeze.status_code == 200
        assert unfreeze.json()["adaptation_frozen"] is False
