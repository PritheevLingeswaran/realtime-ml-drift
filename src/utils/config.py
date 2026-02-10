from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml


def deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Merge b into a recursively (b wins)."""
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@dataclass(frozen=True)
class AppConfig:
    raw: dict[str, Any]

    @property
    def env(self) -> str:
        return str(self.raw.get("app", {}).get("env", "dev"))


def load_config(config_path: str | None) -> AppConfig:
    """
    Load config using a production pattern:
    - Always load configs/base.yaml first
    - Then merge an optional env override file (dev/prod) on top
    - Then merge optional overlays from CONFIG_OVERLAYS env var
    - Then apply small env var overrides (APP_ENV, SEED, API_* etc.)
    """
    default_base = os.environ.get("CONFIG_BASE", "configs/base.yaml")
    base_cfg = load_yaml(default_base)

    # If user passes --config, treat it as an override overlay, not a replacement.
    if config_path:
        override_cfg = load_yaml(config_path)
        cfg = deep_merge(base_cfg, override_cfg)
    else:
        # If no explicit file passed, use CONFIG_PATH as override if provided.
        override_path = os.environ.get("CONFIG_PATH")
        cfg = base_cfg
        if override_path and override_path != default_base:
            cfg = deep_merge(cfg, load_yaml(override_path))

    # Optional additive overlays (comma-separated)
    overlays = os.environ.get("CONFIG_OVERLAYS", "")
    for p in [x.strip() for x in overlays.split(",") if x.strip()]:
        cfg = deep_merge(cfg, load_yaml(p))

    # Minimal env overrides
    cfg.setdefault("app", {})
    cfg["app"]["env"] = os.environ.get("APP_ENV", cfg["app"].get("env", "dev"))
    seed = os.environ.get("SEED")
    if seed is not None:
        cfg["app"]["seed"] = int(seed)

    cfg.setdefault("monitoring", {})
    cfg["monitoring"]["log_level"] = os.environ.get(
        "LOG_LEVEL", cfg["monitoring"].get("log_level", "INFO")
    )

    cfg.setdefault("api", {})
    cfg["api"]["host"] = os.environ.get("API_HOST", cfg["api"].get("host", "0.0.0.0"))
    port = os.environ.get("API_PORT")
    cfg["api"]["port"] = int(port) if port else int(cfg["api"].get("port", 8000))

    return AppConfig(raw=cfg)
