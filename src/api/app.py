from __future__ import annotations

import asyncio
from typing import Optional

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from src.schemas.event_schema import Event
from src.streaming.runner import ServiceState, build_state, process_event


def create_app(config_path: Optional[str] = None) -> FastAPI:
    state = build_state(config_path=config_path)

    app = FastAPI(
        title="realtime-ml-drift",
        version="0.1.0",
        default_response_class=ORJSONResponse,
    )

    app.state.service = state  # type: ignore[attr-defined]

    @app.on_event("startup")
    async def _startup() -> None:
        cfg = state.config
        if bool(cfg["api"].get("start_background_stream", False)):
            # Start streaming in the background (dev/prod convenience).
            app.state.stream_task = asyncio.create_task(_bg_stream(state))  # type: ignore[attr-defined]
            state.logger.info("bg_stream_started")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        task = getattr(app.state, "stream_task", None)
        if task:
            task.cancel()

    @app.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "model_ready": bool(state.scorer.ready),
            "env": str(state.config["app"]["env"]),
        }

    @app.post("/score")
    async def score(e: Event) -> dict:
        # Ad-hoc scoring endpoint (same pipeline).
        return await process_event(state, e, source="api")

    @app.get("/alerts")
    async def alerts(limit: int = 200) -> list[dict]:
        cfg_lim = int(state.config["api"].get("max_alerts_returned", 200))
        limit = min(int(limit), cfg_lim)
        return [a.model_dump() for a in state.alerts.list(limit=limit)]

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


async def _bg_stream(state: ServiceState) -> None:
    from src.streaming.sources import DriftScenario, JSONLReplaySource, SyntheticTransactionSource

    cfg = state.config
    scfg = cfg["streaming"]
    drift_cfg = scfg.get("drift", {})
    drift = DriftScenario(
        enabled=bool(drift_cfg.get("enabled", True)),
        drift_start_event=int(drift_cfg.get("drift_start_event", 0)),
        drift_type=str(drift_cfg.get("drift_type", "mean_shift_amount")),
    )

    if scfg["source"] == "synthetic":
        src = SyntheticTransactionSource(
            seed=int(cfg["app"]["seed"]),
            event_rate_per_sec=float(scfg["event_rate_per_sec"]),
            entity_cardinality=int(scfg["entity_cardinality"]),
            max_events=int(scfg["max_events"]),
            drift=drift,
        )
    else:
        src = JSONLReplaySource(path=str(scfg["replay_path"]), speedup=20.0 if cfg["app"]["env"] == "dev" else 1.0)

    state.logger.info("bg_stream_loop", source=scfg["source"])
    async for e in src.stream():
        try:
            await process_event(state, e, source="stream")
        except Exception as ex:  # noqa: BLE001
            state.logger.error("bg_stream_error", error=str(ex), event_id=e.event_id)


def run_api(config_path: Optional[str] = None) -> None:
    app = create_app(config_path=config_path)
    state: ServiceState = app.state.service  # type: ignore[attr-defined]
    host = str(state.config["api"]["host"])
    port = int(state.config["api"]["port"])

    uvicorn.run(app, host=host, port=port, log_level="warning")
