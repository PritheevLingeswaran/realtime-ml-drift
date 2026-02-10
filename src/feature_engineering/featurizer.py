from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from src.feature_engineering.window_store import EntityWindowStore
from src.schemas.event_schema import Event


@dataclass
class Featurizer:
    store: EntityWindowStore
    enabled_features: List[str]

    def ingest_and_featurize(self, e: Event) -> Dict[str, float]:
        # Streaming-safe: update state first, then compute using *prior* (see store implementation)
        self.store.add(e)
        feats = self.store.compute_features(e)
        # Config-driven output: only emit requested features
        return {k: float(feats[k]) for k in self.enabled_features}
