from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

# Why: allow running `python scripts/benchmark.py` on Windows/Linux without
# relying on shell-level PYTHONPATH tweaks.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.monitoring.benchmarking import (  # noqa: E402
    ResourceSampler,
    check_target_anomaly_tolerance,
    compute_alert_metrics,
    compute_detection_delay,
    compute_fp_reduction_percent,
    environment_info,
    percentile,
    rolling_alerts_per_minute,
    safety_check_anomaly_rate,
)
from src.schemas.event_schema import Event  # noqa: E402
from src.streaming.runner import build_state, process_event, process_events_batch  # noqa: E402
from src.utils.config import load_config  # noqa: E402


@dataclass
class PassResult:
    name: str
    scored_events: int
    runtime_seconds: float
    events_per_sec: float
    events_per_min: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    detection_delay_seconds: float | None
    detection_delay_events: int | None
    alerts: int
    false_alerts: int | None
    false_alert_rate: float | None
    anomaly_rate: float
    anomaly_rate_denominator: int
    labels_available: bool
    alerts_during_drift: int | None
    alerts_during_non_drift: int | None
    alert_rate_drift_segment: float | None
    alert_rate_non_drift_segment: float | None
    tp: int | None
    fp: int | None
    tn: int | None
    fn: int | None
    avg_alerts_per_minute: float
    max_alerts_per_minute: float
    alert_cap_breached: bool
    cpu_avg_percent: float
    cpu_p95_percent: float
    peak_rss_mb: float
    adaptation_stats: dict[str, Any]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="End-to-end streaming benchmark runner.")
    p.add_argument("--config", default="configs/dev.yaml")
    p.add_argument("--replay", default="data/raw/streams/dev_stream.jsonl")
    p.add_argument("--duration_sec", type=int, default=600)
    p.add_argument("--report_md", default="docs/benchmark_report.md")
    p.add_argument(
        "--report_json", default="data/processed/snapshots/benchmark_report.json"
    )
    return p.parse_args()


def load_replay_events(path: Path) -> tuple[list[Event], dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Replay file not found: {path}")
    out: list[Event] = []
    drift_tag_present_all = True
    line_count = 0
    with open(path, "rb") as f:
        for line in f:
            line_count += 1
            raw = orjson.loads(line)
            if "drift_tag" not in raw:
                drift_tag_present_all = False
            out.append(Event(**raw))
    if not out:
        raise ValueError(f"Replay file has no events: {path}")
    return out, {
        "drift_tag_present_all": drift_tag_present_all,
        "lines_total": line_count,
    }


def _apply_benchmark_overrides(state: Any, cfg: dict[str, Any], adaptation_enabled: bool) -> dict[str, Any]:
    bcfg = cfg.get("benchmarking", {})

    # Why: pass-level toggle lets us compare fixed baseline vs adapted controller
    # under identical workload.
    state.threshold.cfg.enabled = bool(adaptation_enabled)
    if adaptation_enabled and bcfg.get("target_anomaly_rate") is not None:
        state.threshold.cfg.target_anomaly_rate = float(bcfg["target_anomaly_rate"])

    # Why: these are the detector knobs most likely to affect throughput/quality.
    if bcfg.get("min_samples") is not None:
        state.drift.cfg.min_samples = int(bcfg["min_samples"])
    if bcfg.get("window_size") is not None:
        state.drift.cfg.window_size = int(bcfg["window_size"])
    if bcfg.get("evaluation_interval") is not None:
        state.drift.cfg.evaluation_interval = int(bcfg["evaluation_interval"])

    drift_scenario = bcfg.get("drift_scenario") or cfg.get("streaming", {}).get("drift", {})

    return {
        "target_anomaly_rate": float(state.threshold.cfg.target_anomaly_rate),
        "adaptation_enabled": bool(state.threshold.cfg.enabled),
        "min_samples": int(state.drift.cfg.min_samples),
        "window_size": int(state.drift.cfg.window_size),
        "evaluation_interval": int(state.drift.cfg.evaluation_interval),
        "max_alerts_per_minute": float(bcfg.get("max_alerts_per_minute", 120.0)),
        "drift_scenario": drift_scenario,
    }


def run_pass(
    pass_name: str,
    config_path: str,
    cfg_raw: dict[str, Any],
    replay_events: list[Event],
    duration_sec: int,
    adaptation_enabled: bool,
    use_micro_batch: bool = False,
    micro_batch_size: int = 64,
    target_events_per_sec: float | None = None,
    labels_available: bool = True,
) -> tuple[PassResult, dict[str, Any]]:
    state = build_state(config_path)
    applied = _apply_benchmark_overrides(state, cfg_raw, adaptation_enabled)

    latencies_ms: list[float] = []
    drift_flags: list[bool] | None = [] if labels_available else None
    alert_flags: list[bool] = []

    first_drift_idx = None
    first_detect_idx = None
    first_drift_ts = None
    first_detect_ts = None

    scored = 0
    event_cursor = 0
    alert_times = deque()
    alert_rate_samples: list[float] = []
    alert_cap_breached = False
    max_alerts_per_min = 0.0

    sampler = ResourceSampler(sample_sec=1.0)
    sampler.start()
    start = time.perf_counter()

    with asyncio.Runner() as runner:
        while (time.perf_counter() - start) < float(duration_sec):
            if use_micro_batch:
                batch: list[Event] = []
                for _ in range(max(1, micro_batch_size)):
                    batch.append(replay_events[event_cursor % len(replay_events)])
                    event_cursor += 1
                ev_t0 = time.perf_counter()
                outs = runner.run(process_events_batch(state, batch, source="benchmark"))
                elapsed_ms = (time.perf_counter() - ev_t0) * 1000.0
                each_ms = elapsed_ms / max(1, len(batch))
                latencies_ms.extend([each_ms] * len(batch))
                iter_rows = zip(batch, outs, strict=False)
            else:
                e = replay_events[event_cursor % len(replay_events)]
                event_cursor += 1
                ev_t0 = time.perf_counter()
                out = runner.run(process_event(state, e, source="benchmark"))
                latencies_ms.append((time.perf_counter() - ev_t0) * 1000.0)
                iter_rows = [(e, out)]

            for e, out in iter_rows:
                if out.get("status") != "scored":
                    continue

                scored += 1
                is_drift = bool(e.drift_tag)
                if drift_flags is not None:
                    drift_flags.append(is_drift)
                is_alert = bool(out.get("is_anomaly", False))
                alert_flags.append(is_alert)

                if labels_available and first_drift_idx is None and is_drift:
                    first_drift_idx = scored
                    first_drift_ts = float(e.ts)

                if first_detect_idx is None and bool(out.get("drift_active", False)):
                    first_detect_idx = scored
                    first_detect_ts = float(e.ts)

                if is_alert:
                    now_wall = time.perf_counter()
                    alert_times.append(now_wall)
                    apm = rolling_alerts_per_minute(alert_times, now_wall)
                    alert_rate_samples.append(apm)
                    max_alerts_per_min = max(max_alerts_per_min, apm)
                    if apm > float(applied["max_alerts_per_minute"]):
                        alert_cap_breached = True

            if target_events_per_sec and target_events_per_sec > 0.0 and scored > 0:
                expected_elapsed = scored / float(target_events_per_sec)
                actual_elapsed = time.perf_counter() - start
                if expected_elapsed > actual_elapsed:
                    time.sleep(expected_elapsed - actual_elapsed)

    runtime = max(1e-9, time.perf_counter() - start)
    sampler.stop()

    events_per_sec = scored / runtime
    delay = compute_detection_delay(
        drift_start_event_idx=first_drift_idx,
        detect_event_idx=first_detect_idx,
        events_per_sec=events_per_sec,
        drift_start_ts=first_drift_ts,
        detect_ts=first_detect_ts,
        replay_uses_real_sleep=False,
    )

    alerts = compute_alert_metrics(alert_flags=alert_flags, drift_flags=drift_flags)
    resource = sampler.stats()

    pass_result = PassResult(
        name=pass_name,
        scored_events=scored,
        runtime_seconds=runtime,
        events_per_sec=events_per_sec,
        events_per_min=events_per_sec * 60.0,
        latency_p50_ms=percentile(latencies_ms, 0.50),
        latency_p95_ms=percentile(latencies_ms, 0.95),
        latency_p99_ms=percentile(latencies_ms, 0.99),
        detection_delay_seconds=delay.seconds_to_detect,
        detection_delay_events=delay.events_to_detect,
        alerts=alerts.alerts,
        false_alerts=alerts.false_alerts,
        false_alert_rate=alerts.false_alert_rate,
        anomaly_rate=alerts.anomaly_rate,
        anomaly_rate_denominator=alerts.anomaly_rate_denominator,
        labels_available=alerts.labels_available,
        alerts_during_drift=alerts.alerts_during_drift,
        alerts_during_non_drift=alerts.alerts_during_non_drift,
        alert_rate_drift_segment=alerts.alert_rate_drift_segment,
        alert_rate_non_drift_segment=alerts.alert_rate_non_drift_segment,
        tp=alerts.tp,
        fp=alerts.fp,
        tn=alerts.tn,
        fn=alerts.fn,
        avg_alerts_per_minute=(sum(alert_rate_samples) / len(alert_rate_samples))
        if alert_rate_samples
        else 0.0,
        max_alerts_per_minute=max_alerts_per_min,
        alert_cap_breached=alert_cap_breached,
        cpu_avg_percent=float(resource["cpu_avg_percent"]),
        cpu_p95_percent=float(resource["cpu_p95_percent"]),
        peak_rss_mb=float(resource["peak_rss_mb"]),
        adaptation_stats=state.threshold.stats(),
    )

    return pass_result, applied


def asdict_pass(p: PassResult) -> dict[str, Any]:
    return {
        "name": p.name,
        "scored_events": p.scored_events,
        "runtime_seconds": p.runtime_seconds,
        "throughput": {
            "events_per_sec": p.events_per_sec,
            "events_per_min": p.events_per_min,
        },
        "pipeline_latency_ms": {
            "p50": p.latency_p50_ms,
            "p95": p.latency_p95_ms,
            "p99": p.latency_p99_ms,
        },
        "drift_detection_latency": {
            "seconds_to_detect": p.detection_delay_seconds,
            "events_to_detect": p.detection_delay_events,
            "definition": "Detection delay: drift onset to first drift_active=True event",
        },
        "alerts": {
            "count": p.alerts,
            "false_alert_count": p.false_alerts,
            "false_alert_rate": p.false_alert_rate,
            "false_alert_definition": "false alert = alert fired when drift_tag is None (non-drift segment)",
            "labels_available": p.labels_available,
            "alerts_during_drift": p.alerts_during_drift,
            "alerts_during_non_drift": p.alerts_during_non_drift,
            "alert_rate_drift_segment": p.alert_rate_drift_segment,
            "alert_rate_non_drift_segment": p.alert_rate_non_drift_segment,
            "anomaly_rate": p.anomaly_rate,
            "anomaly_rate_denominator": p.anomaly_rate_denominator,
            "avg_alerts_per_minute": p.avg_alerts_per_minute,
            "max_alerts_per_minute": p.max_alerts_per_minute,
            "alert_cap_breached": p.alert_cap_breached,
        },
        "drift_confusion": {
            "tp": p.tp,
            "fp": p.fp,
            "tn": p.tn,
            "fn": p.fn,
            "definition": "Confusion is computed event-wise with prediction=is_anomaly and truth=(drift_tag is not None)",
        },
        "resource_efficiency": {
            "cpu_avg_percent": p.cpu_avg_percent,
            "cpu_p95_percent": p.cpu_p95_percent,
            "peak_rss_mb": p.peak_rss_mb,
        },
        "adaptation_stats": p.adaptation_stats,
    }


def render_markdown(report: dict[str, Any]) -> str:
    base = report["passes"]["baseline"]
    tuned = report["passes"]["tuned"]
    cmp_ = report["comparison"]
    safety = report["safety"]
    perf = report.get("performance_comparison")

    perf_section = ""
    if perf and perf.get("enabled"):
        perf_section = f"""
## Performance Comparison (Before vs After Micro-Batching)
- Before CPU avg/p95: `{perf['before']['resource_efficiency']['cpu_avg_percent']:.4f}% / {perf['before']['resource_efficiency']['cpu_p95_percent']:.4f}%`
- After CPU avg/p95: `{perf['after']['resource_efficiency']['cpu_avg_percent']:.4f}% / {perf['after']['resource_efficiency']['cpu_p95_percent']:.4f}%`
- CPU avg delta (after-before): `{perf['cpu_avg_percent_delta']:.4f}%`
- Throughput before/after: `{perf['before']['throughput']['events_per_sec']:.4f}` / `{perf['after']['throughput']['events_per_sec']:.4f}` events/sec
- Throughput delta (after-before): `{perf['events_per_sec_delta']:.4f}` events/sec
"""
    labels_note = ""
    if not report.get("integrity", {}).get("drift_tag_present_all", True):
        labels_note = "\n- Integrity: `drift_tag` missing for at least one replay event; false-alert and confusion metrics are reported as `N/A`."

    def _fmt(v: Any, fmt: str = ".4f") -> str:
        if v is None:
            return "N/A"
        if isinstance(v, (float, int)):
            return format(v, fmt)
        return str(v)

    return f"""# Benchmark Report

## Run Context
- Config: `{report['inputs']['config']}`
- Replay: `{report['inputs']['replay']}`
- Duration per pass: `{report['inputs']['duration_sec']} sec`
- Host OS: `{report['environment']['os']}`
- Python: `{report['environment']['python_version']}`
- CPU logical cores: `{report['environment']['cpu_logical_cores']}`
- RAM total (GB): `{report['environment']['ram_total_gb']}`
{labels_note}

## Baseline Pass (adaptation disabled)
- Throughput: `{base['throughput']['events_per_sec']:.4f} events/sec` (`{base['throughput']['events_per_min']:.2f} events/min`)
- Pipeline latency (ms): p50 `{base['pipeline_latency_ms']['p50']:.4f}`, p95 `{base['pipeline_latency_ms']['p95']:.4f}`, p99 `{base['pipeline_latency_ms']['p99']:.4f}`
- Drift detection latency: `{base['drift_detection_latency']['seconds_to_detect']}` sec, `{base['drift_detection_latency']['events_to_detect']}` events
- Alerts: `{base['alerts']['count']}` (false: `{_fmt(base['alerts']['false_alert_count'], '.0f')}`; false alert rate: `{_fmt(base['alerts']['false_alert_rate'])}`)
- Segment alerts (drift/non-drift): `{_fmt(base['alerts']['alerts_during_drift'], '.0f')}` / `{_fmt(base['alerts']['alerts_during_non_drift'], '.0f')}`
- Segment alert rates (drift/non-drift): `{_fmt(base['alerts']['alert_rate_drift_segment'])}` / `{_fmt(base['alerts']['alert_rate_non_drift_segment'])}`
- Drift confusion (TP/FP/TN/FN): `{_fmt(base['drift_confusion']['tp'], '.0f')}` / `{_fmt(base['drift_confusion']['fp'], '.0f')}` / `{_fmt(base['drift_confusion']['tn'], '.0f')}` / `{_fmt(base['drift_confusion']['fn'], '.0f')}`
- Anomaly rate: `{base['alerts']['anomaly_rate']:.4f}` (denominator: `{base['alerts']['anomaly_rate_denominator']}` scored events)
- CPU avg/p95: `{base['resource_efficiency']['cpu_avg_percent']:.4f}% / {base['resource_efficiency']['cpu_p95_percent']:.4f}%`
- Peak RSS: `{base['resource_efficiency']['peak_rss_mb']:.4f} MB`

## Tuned Pass (adaptation enabled)
- Throughput: `{tuned['throughput']['events_per_sec']:.4f} events/sec` (`{tuned['throughput']['events_per_min']:.2f} events/min`)
- Pipeline latency (ms): p50 `{tuned['pipeline_latency_ms']['p50']:.4f}`, p95 `{tuned['pipeline_latency_ms']['p95']:.4f}`, p99 `{tuned['pipeline_latency_ms']['p99']:.4f}`
- Drift detection latency: `{tuned['drift_detection_latency']['seconds_to_detect']}` sec, `{tuned['drift_detection_latency']['events_to_detect']}` events
- Alerts: `{tuned['alerts']['count']}` (false: `{_fmt(tuned['alerts']['false_alert_count'], '.0f')}`; false alert rate: `{_fmt(tuned['alerts']['false_alert_rate'])}`)
- Segment alerts (drift/non-drift): `{_fmt(tuned['alerts']['alerts_during_drift'], '.0f')}` / `{_fmt(tuned['alerts']['alerts_during_non_drift'], '.0f')}`
- Segment alert rates (drift/non-drift): `{_fmt(tuned['alerts']['alert_rate_drift_segment'])}` / `{_fmt(tuned['alerts']['alert_rate_non_drift_segment'])}`
- Drift confusion (TP/FP/TN/FN): `{_fmt(tuned['drift_confusion']['tp'], '.0f')}` / `{_fmt(tuned['drift_confusion']['fp'], '.0f')}` / `{_fmt(tuned['drift_confusion']['tn'], '.0f')}` / `{_fmt(tuned['drift_confusion']['fn'], '.0f')}`
- Anomaly rate: `{tuned['alerts']['anomaly_rate']:.4f}` (denominator: `{tuned['alerts']['anomaly_rate_denominator']}` scored events)
- CPU avg/p95: `{tuned['resource_efficiency']['cpu_avg_percent']:.4f}% / {tuned['resource_efficiency']['cpu_p95_percent']:.4f}%`
- Peak RSS: `{tuned['resource_efficiency']['peak_rss_mb']:.4f} MB`

## Baseline vs Tuned
- False positive reduction: `{_fmt(cmp_['false_positive_reduction_percent'])}%`
- Anomaly rate change (tuned-baseline): `{cmp_['anomaly_rate_change']:.6f}`
- Drift detection delay change seconds (tuned-baseline): `{cmp_['detection_delay_seconds_change']}`
- Drift detection delay change events (tuned-baseline): `{cmp_['detection_delay_events_change']}`
{perf_section}

## Safety Gate
- Status: `{'PASS' if safety['passed'] else 'FAIL'}`
- Detail: `{safety['message']}`

## Reality Check
- False-alert definition used here: alert fired when `drift_tag is None` (non-drift segment).
- Drift detection is probabilistic. False positives and false negatives are unavoidable.
- Automation can suppress incidents when thresholds are misconfigured; human review remains required.
- Adaptation is rate-control, not root-cause remediation; retraining and data quality interventions are still manual decisions.
"""


def main() -> int:
    args = parse_args()
    cfg = load_config(args.config).raw
    replay_events, replay_meta = load_replay_events(Path(args.replay))
    labels_available = bool(replay_meta.get("drift_tag_present_all", False))

    baseline, applied_baseline = run_pass(
        pass_name="baseline",
        config_path=args.config,
        cfg_raw=cfg,
        replay_events=replay_events,
        duration_sec=int(args.duration_sec),
        adaptation_enabled=False,
        labels_available=labels_available,
    )
    tuned, applied_tuned = run_pass(
        pass_name="tuned",
        config_path=args.config,
        cfg_raw=cfg,
        replay_events=replay_events,
        duration_sec=int(args.duration_sec),
        adaptation_enabled=True,
        labels_available=labels_available,
    )

    bcfg = cfg.get("benchmarking", {})
    perf_compare_enabled = bool(bcfg.get("performance_compare_enabled", False))
    perf_compare: dict[str, Any] | None = None
    if perf_compare_enabled:
        perf_before, _ = run_pass(
            pass_name="before_micro_batch",
            config_path=args.config,
            cfg_raw=cfg,
            replay_events=replay_events,
            duration_sec=int(args.duration_sec),
            adaptation_enabled=False,
            use_micro_batch=False,
            target_events_per_sec=float(bcfg.get("performance_compare_target_eps", 900.0)),
            labels_available=labels_available,
        )
        perf_after, _ = run_pass(
            pass_name="after_micro_batch",
            config_path=args.config,
            cfg_raw=cfg,
            replay_events=replay_events,
            duration_sec=int(args.duration_sec),
            adaptation_enabled=False,
            use_micro_batch=True,
            micro_batch_size=int(bcfg.get("micro_batch_size", 64)),
            target_events_per_sec=float(bcfg.get("performance_compare_target_eps", 900.0)),
            labels_available=labels_available,
        )
        perf_compare = {
            "enabled": True,
            "before": asdict_pass(perf_before),
            "after": asdict_pass(perf_after),
            "cpu_avg_percent_delta": perf_after.cpu_avg_percent - perf_before.cpu_avg_percent,
            "cpu_p95_percent_delta": perf_after.cpu_p95_percent - perf_before.cpu_p95_percent,
            "events_per_sec_delta": perf_after.events_per_sec - perf_before.events_per_sec,
        }

    fp_reduction = (
        compute_fp_reduction_percent(
            fp_baseline=int(baseline.false_alerts),
            fp_tuned=int(tuned.false_alerts),
        )
        if (baseline.false_alerts is not None and tuned.false_alerts is not None)
        else None
    )

    detection_seconds_delta = None
    if baseline.detection_delay_seconds is not None and tuned.detection_delay_seconds is not None:
        detection_seconds_delta = tuned.detection_delay_seconds - baseline.detection_delay_seconds

    detection_events_delta = None
    if baseline.detection_delay_events is not None and tuned.detection_delay_events is not None:
        detection_events_delta = tuned.detection_delay_events - baseline.detection_delay_events

    # Hard resume-safety gate: if tuned anomaly rate is excessive, report must fail.
    anomaly_passed, anomaly_msg = safety_check_anomaly_rate(tuned.anomaly_rate, hard_limit=0.05)

    target_rate = float(applied_tuned["target_anomaly_rate"])
    tolerance_abs = 0.005
    stats = tuned.adaptation_stats
    guardrail_blocked = (
        int(stats.get("bounds_limited", 0)) > 0
        or int(stats.get("step_limited", 0)) > 0
        or (
            int(stats.get("updates_applied", 0)) == 0
            and (
                int(stats.get("skipped_drift", 0)) > 0
                or int(stats.get("skipped_cooldown", 0)) > 0
                or int(stats.get("skipped_history", 0)) > 0
            )
        )
    )
    target_passed, target_msg = check_target_anomaly_tolerance(
        observed_anomaly_rate=tuned.anomaly_rate,
        target_anomaly_rate=target_rate,
        tolerance_abs=tolerance_abs,
        guardrail_blocked=guardrail_blocked,
    )
    passed = anomaly_passed and target_passed

    report = {
        "inputs": {
            "config": args.config,
            "replay": args.replay,
            "duration_sec": int(args.duration_sec),
        },
        "integrity": {
            "drift_tag_present_all": labels_available,
            "drift_truth_required_for_false_alert_reporting": True,
            "message": (
                "drift_tag available for all replay events"
                if labels_available
                else "drift_tag missing for at least one replay event; false-alert/confusion metrics set to N/A"
            ),
        },
        "environment": environment_info(),
        "benchmark_options": {
            "baseline": applied_baseline,
            "tuned": applied_tuned,
        },
        "passes": {
            "baseline": asdict_pass(baseline),
            "tuned": asdict_pass(tuned),
        },
        "comparison": {
            "false_positive_reduction_percent": fp_reduction,
            "anomaly_rate_change": tuned.anomaly_rate - baseline.anomaly_rate,
            "detection_delay_seconds_change": detection_seconds_delta,
            "detection_delay_events_change": detection_events_delta,
        },
        "performance_comparison": perf_compare,
        "safety": {
            "passed": passed,
            "checks": {
                "anomaly_rate_hard_limit": {
                    "passed": anomaly_passed,
                    "message": anomaly_msg,
                },
                "target_anomaly_tolerance": {
                    "passed": target_passed,
                    "message": target_msg,
                    "target_anomaly_rate": target_rate,
                    "tolerance_abs": tolerance_abs,
                    "guardrail_blocked": guardrail_blocked,
                },
            },
            "message": f"{anomaly_msg} | {target_msg}",
        },
        "generated_at_unix": time.time(),
    }

    md_path = Path(args.report_md)
    json_path = Path(args.report_json)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    md = render_markdown(report)
    md_path.write_text(md, encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(md)
    print(
        "\nPass counters: "
        f"baseline scored_events={baseline.scored_events}, alerts={baseline.alerts}, "
        f"anomaly_rate_denominator={baseline.anomaly_rate_denominator}, labels_available={baseline.labels_available}; "
        f"tuned scored_events={tuned.scored_events}, alerts={tuned.alerts}, "
        f"anomaly_rate_denominator={tuned.anomaly_rate_denominator}, labels_available={tuned.labels_available}"
    )
    print(f"\nWrote Markdown report: {md_path}")
    print(f"Wrote JSON report: {json_path}")

    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
