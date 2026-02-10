from __future__ import annotations

import os
from typing import Optional

import orjson

from evaluation.drift_detection import DriftMetrics, update_confusion
from src.schemas.event_schema import Event
from src.streaming.runner import build_state, process_event
from src.utils.config import load_config


def run_evaluation(config_path: Optional[str] = None) -> None:
    cfg = load_config(config_path).raw
    path = str(cfg["streaming"]["replay_path"])
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Replay path not found: {path}. Generate with scripts/generate_stream.py first."
        )

    state = build_state(config_path=config_path)
    drift_metrics = DriftMetrics()

    # Replay fast, no sleeps
    with open(path, "rb") as f:
        for line in f:
            e = Event(**orjson.loads(line))
            out = _run_one(state, e)
            predicted = bool(out.get("drift_active", False))
            true = bool(e.drift_tag is not None)
            update_confusion(drift_metrics, predicted_drift=predicted, true_drift=true)

    md = _render_md(drift_metrics, path)
    os.makedirs("docs", exist_ok=True)
    with open("docs/evaluation_results.md", "w", encoding="utf-8") as fp:
        fp.write(md)

    print(md)


def _run_one(state, e: Event) -> dict:
    import asyncio

    # process_event is async. For evaluation, run in a loop-free way.
    return asyncio.run(process_event(state, e, source="eval"))


def _render_md(m: DriftMetrics, path: str) -> str:
    return f"""# Evaluation Results (sample)

Replay file: `{path}`

## Drift detection (binary)
- TP: {m.tp}
- FP: {m.fp}
- TN: {m.tn}
- FN: {m.fn}

- Precision: {m.precision():.4f}
- Recall: {m.recall():.4f}
- F1: {m.f1():.4f}

## Notes
- This is a **sanity-check** evaluation using generator-provided drift tags (not real ground truth).
- In production, you'd use incident labels, delayed outcomes, or proxy signals to evaluate drift/alert quality.
"""
