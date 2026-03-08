from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class Alert(BaseModel):
    alert_id: str
    ts: float
    entity_id: str
    event_id: str
    score: float = Field(..., ge=0.0, le=1.0)
    threshold: float = Field(..., ge=0.0, le=1.0)
    severity: Literal["low", "medium", "high", "critical"]
    reason: str
    drift_state: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] | None = None
