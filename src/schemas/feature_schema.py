from __future__ import annotations

from pydantic import BaseModel, Field


class FeatureVector(BaseModel):
    entity_id: str
    ts: float
    features: dict[str, float] = Field(default_factory=dict)
