from __future__ import annotations

import os
import platform
import resource
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

try:
    import psutil  # type: ignore
except Exception:  # noqa: BLE001
    psutil = None


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    idx = max(0.0, min(1.0, q)) * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


@dataclass
class DetectionDelay:
    events_to_detect: int | None
    seconds_to_detect: float | None


def compute_detection_delay(
    drift_start_event_idx: int | None,
    detect_event_idx: int | None,
    events_per_sec: float,
    drift_start_ts: float | None,
    detect_ts: float | None,
    replay_uses_real_sleep: bool,
) -> DetectionDelay:
    if drift_start_event_idx is None or detect_event_idx is None:
        return DetectionDelay(events_to_detect=None, seconds_to_detect=None)

    events_to_detect = max(0, int(detect_event_idx) - int(drift_start_event_idx))

    # Why: replay benchmarks often run without wall-clock sleeps. In that mode,
    # timestamp delta is meaningless and we convert event delay into seconds by throughput.
    if replay_uses_real_sleep and drift_start_ts is not None and detect_ts is not None:
        seconds = max(0.0, float(detect_ts) - float(drift_start_ts))
    else:
        seconds = events_to_detect / max(1e-9, float(events_per_sec))

    return DetectionDelay(events_to_detect=events_to_detect, seconds_to_detect=seconds)


@dataclass
class AlertMetrics:
    alerts: int
    false_alerts: int | None
    false_alert_rate: float | None
    anomaly_rate: float
    anomaly_rate_denominator: int
    labels_available: bool
    alerts_during_drift: int | None
    alerts_during_non_drift: int | None
    drift_event_count: int | None
    non_drift_event_count: int | None
    alert_rate_drift_segment: float | None
    alert_rate_non_drift_segment: float | None
    tp: int | None
    fp: int | None
    tn: int | None
    fn: int | None


def compute_alert_metrics(alert_flags: list[bool], drift_flags: list[bool] | None) -> AlertMetrics:
    alerts = 0
    for alert in alert_flags:
        if alert:
            alerts += 1

    denominator = len(alert_flags)
    anomaly_rate = alerts / denominator if denominator else 0.0
    if drift_flags is None:
        return AlertMetrics(
            alerts=alerts,
            false_alerts=None,
            false_alert_rate=None,
            anomaly_rate=anomaly_rate,
            anomaly_rate_denominator=denominator,
            labels_available=False,
            alerts_during_drift=None,
            alerts_during_non_drift=None,
            drift_event_count=None,
            non_drift_event_count=None,
            alert_rate_drift_segment=None,
            alert_rate_non_drift_segment=None,
            tp=None,
            fp=None,
            tn=None,
            fn=None,
        )

    tp = fp = tn = fn = 0
    alerts_during_drift = 0
    alerts_during_non_drift = 0
    drift_event_count = 0
    non_drift_event_count = 0
    for alert, is_drift in zip(alert_flags, drift_flags, strict=False):
        if is_drift:
            drift_event_count += 1
        else:
            non_drift_event_count += 1

        if alert and is_drift:
            tp += 1
            alerts_during_drift += 1
        elif alert and not is_drift:
            fp += 1
            alerts_during_non_drift += 1
        elif (not alert) and is_drift:
            fn += 1
        else:
            tn += 1

    false_alert_rate = fp / alerts if alerts else 0.0
    alert_rate_drift_segment = (
        alerts_during_drift / drift_event_count if drift_event_count else 0.0
    )
    alert_rate_non_drift_segment = (
        alerts_during_non_drift / non_drift_event_count if non_drift_event_count else 0.0
    )
    return AlertMetrics(
        alerts=alerts,
        false_alerts=fp,
        false_alert_rate=false_alert_rate,
        anomaly_rate=anomaly_rate,
        anomaly_rate_denominator=denominator,
        labels_available=True,
        alerts_during_drift=alerts_during_drift,
        alerts_during_non_drift=alerts_during_non_drift,
        drift_event_count=drift_event_count,
        non_drift_event_count=non_drift_event_count,
        alert_rate_drift_segment=alert_rate_drift_segment,
        alert_rate_non_drift_segment=alert_rate_non_drift_segment,
        tp=tp,
        fp=fp,
        tn=tn,
        fn=fn,
    )


def compute_fp_reduction_percent(fp_baseline: int, fp_tuned: int) -> float:
    if fp_baseline <= 0:
        return 0.0
    return ((fp_baseline - fp_tuned) / float(fp_baseline)) * 100.0


def safety_check_anomaly_rate(anomaly_rate: float, hard_limit: float = 0.05) -> tuple[bool, str]:
    if anomaly_rate > hard_limit:
        msg = (
            f"FAIL: anomaly_rate={anomaly_rate:.4f} exceeds hard safety limit {hard_limit:.4f}. "
            "This alert volume is not resume-safe and needs retuning."
        )
        return False, msg
    return True, f"PASS: anomaly_rate={anomaly_rate:.4f} within safety limit {hard_limit:.4f}."


def check_target_anomaly_tolerance(
    observed_anomaly_rate: float,
    target_anomaly_rate: float,
    tolerance_abs: float = 0.005,
    *,
    guardrail_blocked: bool = False,
) -> tuple[bool, str]:
    delta = abs(float(observed_anomaly_rate) - float(target_anomaly_rate))
    if delta <= float(tolerance_abs):
        return (
            True,
            f"PASS: tuned anomaly_rate={observed_anomaly_rate:.4f} within "
            f"+/-{tolerance_abs:.4f} of target {target_anomaly_rate:.4f}.",
        )
    if guardrail_blocked:
        return (
            True,
            f"PASS (guardrail waiver): tuned anomaly_rate={observed_anomaly_rate:.4f} is outside "
            f"target {target_anomaly_rate:.4f} +/-{tolerance_abs:.4f}, but adaptation was blocked "
            "by guardrails.",
        )
    return (
        False,
        f"FAIL: tuned anomaly_rate={observed_anomaly_rate:.4f} is outside target "
        f"{target_anomaly_rate:.4f} +/-{tolerance_abs:.4f}.",
    )


@dataclass
class ResourceSample:
    ts: float
    cpu_percent: float
    rss_mb: float


class ResourceSampler:
    """Samples CPU/memory at fixed interval in-process.

    Why: benchmark claims are not defensible without machine resource context,
    especially for streaming systems where latency can look good by burning CPU.
    """

    def __init__(self, sample_sec: float = 1.0, pid: int | None = None) -> None:
        self.sample_sec = max(0.1, float(sample_sec))
        self.pid = pid or os.getpid()
        self.samples: list[ResourceSample] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._proc = None
        self._last_wall = time.perf_counter()
        self._last_cpu = time.process_time()

        if psutil is not None:
            try:
                self._proc = psutil.Process(self.pid)
                # Prime percent measurement; first call is always 0.0 by design.
                self._proc.cpu_percent(interval=None)
            except Exception:  # noqa: BLE001
                self._proc = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop.set()
        self._thread.join(timeout=2.0)
        self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            ts = time.time()
            cpu = 0.0
            rss = 0.0
            if self._proc is not None:
                try:
                    cpu = float(self._proc.cpu_percent(interval=None))
                    rss = float(self._proc.memory_info().rss) / (1024.0 * 1024.0)
                except Exception:  # noqa: BLE001
                    cpu = 0.0
                    rss = 0.0
            else:
                # Why: network-restricted envs may miss psutil; keep benchmark usable with stdlib fallback.
                now_wall = time.perf_counter()
                now_cpu = time.process_time()
                d_wall = max(1e-9, now_wall - self._last_wall)
                d_cpu = max(0.0, now_cpu - self._last_cpu)
                cpu = (d_cpu / d_wall) * 100.0
                self._last_wall = now_wall
                self._last_cpu = now_cpu
                # ru_maxrss is KB on Linux, bytes on macOS; normalize to MB.
                ru_maxrss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                if platform.system().lower() == "darwin":
                    rss = ru_maxrss / (1024.0 * 1024.0)
                else:
                    rss = ru_maxrss / 1024.0
            self.samples.append(ResourceSample(ts=ts, cpu_percent=cpu, rss_mb=rss))
            self._stop.wait(self.sample_sec)

    def stats(self) -> dict[str, float]:
        cpu = [s.cpu_percent for s in self.samples]
        rss = [s.rss_mb for s in self.samples]
        return {
            "cpu_avg_percent": statistics.mean(cpu) if cpu else 0.0,
            "cpu_p95_percent": percentile(cpu, 0.95),
            "peak_rss_mb": max(rss) if rss else 0.0,
        }


def environment_info() -> dict[str, Any]:
    logical_cores = os.cpu_count() or 0
    ram_total_gb = None
    if psutil is not None:
        try:
            ram_total_gb = float(psutil.virtual_memory().total) / (1024.0**3)
        except Exception:  # noqa: BLE001
            ram_total_gb = None

    return {
        "os": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_logical_cores": int(logical_cores),
        "ram_total_gb": ram_total_gb,
    }


def rolling_alerts_per_minute(alert_times_sec: deque[float], now_sec: float) -> float:
    # Why: alert storms can hide true incidents and overwhelm on-call. Keep visibility.
    cutoff = now_sec - 60.0
    while alert_times_sec and alert_times_sec[0] < cutoff:
        alert_times_sec.popleft()
    return float(len(alert_times_sec))


def read_errors_total_best_effort(internal_errors: int = 0) -> int:
    """Return total errors counter if available, else fallback to internal counter."""
    try:
        from src.monitoring import metrics as m

        total = 0.0
        for comp in ("stream", "feature", "model", "drift", "api"):
            total += float(m.ERRORS_TOTAL.labels(component=comp)._value.get())
        return int(total)
    except Exception:  # noqa: BLE001
        return int(internal_errors)
