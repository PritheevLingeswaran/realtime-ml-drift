from __future__ import annotations

import argparse
import asyncio

from src.streaming.runner import run_stream_processor


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_stream_processor(config_path=args.config))
