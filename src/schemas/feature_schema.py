from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field


class FeatureVector(BaseModel):
    entity_id: str
    ts: float
    features: Dict[str, float] = Field(default_factory=dict)
