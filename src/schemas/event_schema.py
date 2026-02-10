from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class Event(BaseModel):
    event_id: str = Field(..., description="Unique event id (stable for replay)")
    ts: float = Field(..., description="Unix timestamp (seconds)")
    entity_id: str = Field(..., description="Entity key (e.g., user/account/device)")

    # Domain-like fields (transaction/log style)
    amount: float = Field(..., ge=0)
    merchant_id: str
    merchant_category: str
    country: str
    channel: Literal["web", "mobile", "pos"]
    device_type: Literal["ios", "android", "desktop", "unknown"] = "unknown"

    # Optional: used by generator/eval to know ground-truth drift intervals
    drift_tag: Optional[str] = Field(default=None, description="Generator label for evaluation only")
