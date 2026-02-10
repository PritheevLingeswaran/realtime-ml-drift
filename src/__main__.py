from __future__ import annotations

import argparse
import asyncio
import sys

from src.api.app import run_api
from src.streaming.runner import run_stream_processor


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="realtime-ml-drift entrypoint")
    p.add_argument("--config", default=None, help="Path to YAML config (defaults to env CONFIG_PATH)")
    p.add_argument("--mode", default="api", choices=["api", "stream"], help="Run mode")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    if args.mode == "api":
        run_api(config_path=args.config)
        return

    if args.mode == "stream":
        asyncio.run(run_stream_processor(config_path=args.config))
        return

    raise ValueError(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutdown requested.", file=sys.stderr)
