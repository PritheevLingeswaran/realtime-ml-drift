from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict, deque

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import FileResponse, Response

from src.monitoring import metrics as m
from src.schemas.event_schema import Event
from src.streaming.runner import ServiceState, _build_source, build_state, process_event, state_view


class _RateLimiter:
    def __init__(self, rpm_limit: int) -> None:
        self.rpm_limit = max(1, int(rpm_limit))
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, now: float) -> bool:
        q = self._hits[key]
        cutoff = now - 60.0
        while q and q[0] < cutoff:
            q.popleft()
        if len(q) >= self.rpm_limit:
            return False
        q.append(now)
        return True


def _require_admin_key(state: ServiceState, api_key: str | None) -> None:
    expected = str(os.getenv("RTML_ADMIN_API_KEY") or state.config.get("security", {}).get("admin_api_key", ""))
    if not expected:
        raise HTTPException(status_code=503, detail="admin_api_key_not_configured")
    if api_key != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


def create_app(config_path: str | None = None) -> FastAPI:
    state = build_state(config_path=config_path)

    app = FastAPI(
        title="realtime-ml-drift",
        version="0.1.0",
        default_response_class=ORJSONResponse,
    )

    # CORS middleware for frontend dashboard
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.service = state  # type: ignore[attr-defined]

    api_cfg = state.config.get("api", {})
    max_body_bytes = int(api_cfg.get("max_request_bytes", 131072))
    rpm_limit = int(api_cfg.get("max_requests_per_minute_per_ip", 1200))
    limiter = _RateLimiter(rpm_limit=rpm_limit)

    @app.middleware("http")
    async def _enforce_basic_limits(request: Request, call_next):  # type: ignore[no-untyped-def]
        # Why: basic request guards prevent accidental memory amplification from oversized payloads.
        clen = request.headers.get("content-length")
        if clen is not None and int(clen) > max_body_bytes:
            return ORJSONResponse({"detail": "request_too_large"}, status_code=413)
        ip = request.client.host if request.client else "unknown"
        if not limiter.allow(ip, time.time()):
            m.ERRORS_TOTAL.labels(component="api").inc()
            return ORJSONResponse({"detail": "rate_limited"}, status_code=429)
        return await call_next(request)

    @app.on_event("startup")
    async def _startup() -> None:
        cfg = state.config
        if bool(cfg["api"].get("start_background_stream", False)):
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
            "restored": bool(state.runtime.restored),
            "restored_state": bool(state.runtime.restored),
            "last_snapshot_unix": state.runtime.last_snapshot_unix,
            "last_snapshot_time": state.runtime.last_snapshot_unix,
        }

    @app.get("/state")
    async def get_state() -> dict:
        return state_view(state)

    @app.post("/score")
    async def score(e: Event) -> dict:
        return await process_event(state, e, source="api", ingest_wall_ts=time.time())

    @app.post("/predict")
    async def predict(e: Event) -> dict:
        return await process_event(state, e, source="api", ingest_wall_ts=time.time())

    @app.post("/detect_drift")
    async def detect_drift(e: Event) -> dict:
        out = await process_event(state, e, source="api", ingest_wall_ts=time.time())
        return {
            "status": str(out.get("status", "unknown")),
            "event_id": str(out.get("event_id", "")),
            "drift_active": bool(out.get("drift_active", False)),
            "is_anomaly": bool(out.get("is_anomaly", False)),
            "score": float(out.get("score", 0.0)),
            "threshold": float(out.get("threshold", 0.0)),
        }

    @app.post("/admin/freeze_adaptation")
    async def freeze_adaptation(
        x_api_key: str | None = Header(default=None),
        x_admin_actor: str | None = Header(default=None),
        reason: str = Query(default="manual_freeze"),
    ) -> dict:
        _require_admin_key(state, x_api_key)
        state.threshold.freeze("admin")
        m.ADMIN_ACTIONS_TOTAL.labels(action="freeze_adaptation", result="success").inc()
        actor = x_admin_actor or "unknown"
        state.logger.info(
            "admin_action",
            audit_type="admin",
            action="freeze_adaptation",
            actor=actor,
            reason=reason,
        )
        return {"status": "ok", "adaptation_frozen": True, "actor": actor, "reason": reason}

    @app.post("/admin/unfreeze_adaptation")
    async def unfreeze_adaptation(
        x_api_key: str | None = Header(default=None),
        x_admin_actor: str | None = Header(default=None),
        reason: str = Query(default="manual_unfreeze"),
    ) -> dict:
        _require_admin_key(state, x_api_key)
        state.threshold.unfreeze()
        state.runtime.lag_circuit_open = False
        m.ADMIN_ACTIONS_TOTAL.labels(action="unfreeze_adaptation", result="success").inc()
        actor = x_admin_actor or "unknown"
        state.logger.info(
            "admin_action",
            audit_type="admin",
            action="unfreeze_adaptation",
            actor=actor,
            reason=reason,
        )
        return {"status": "ok", "adaptation_frozen": False, "actor": actor, "reason": reason}

    @app.post("/admin/refresh_reference")
    async def refresh_reference(
        x_api_key: str | None = Header(default=None),
        x_admin_actor: str | None = Header(default=None),
        reason: str = Query(default="manual_reference_refresh"),
    ) -> dict:
        _require_admin_key(state, x_api_key)
        state.drift.refresh_reference()
        m.ADMIN_ACTIONS_TOTAL.labels(action="refresh_reference", result="success").inc()
        actor = x_admin_actor or "unknown"
        state.logger.info(
            "admin_action",
            audit_type="admin",
            action="refresh_reference",
            actor=actor,
            reason=reason,
        )
        return {"status": "ok", "reference_refreshed": True, "actor": actor, "reason": reason}

    @app.get("/alerts")
    async def alerts(limit: int = 200) -> list[dict]:
        cfg_lim = int(state.config["api"].get("max_alerts_returned", 200))
        limit = min(int(limit), cfg_lim)
        return [a.model_dump() for a in state.alerts.list(limit=limit)]

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # Serve frontend dashboard
    frontend_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "frontend")
    if os.path.isdir(frontend_dir):
        @app.get("/")
        async def dashboard() -> FileResponse:
            return FileResponse(os.path.join(frontend_dir, "index.html"))

        app.mount("/static", StaticFiles(directory=frontend_dir), name="frontend")

    return app


async def _bg_stream(state: ServiceState) -> None:
    src, is_kafka = _build_source(state.config)
    state.logger.info("bg_stream_loop", source=state.config["streaming"]["source"])
    from src.streaming.runner import process_source_with_backpressure

    try:
        await process_source_with_backpressure(state, src, is_kafka=is_kafka)
    except Exception as ex:  # noqa: BLE001
        state.logger.error("bg_stream_error", error=str(ex))


def run_api(config_path: str | None = None) -> None:
    app = create_app(config_path=config_path)
    state: ServiceState = app.state.service  # type: ignore[attr-defined]
    host = str(state.config["api"]["host"])
    port = int(state.config["api"]["port"])

    uvicorn.run(app, host=host, port=port, log_level="warning")
