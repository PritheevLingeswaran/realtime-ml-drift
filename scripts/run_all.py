from __future__ import annotations

import argparse
import multiprocessing as mp
import time

from src.api.app import run_api
from src.streaming.runner import run_stream_processor


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    return p.parse_args()


def _run_stream(cfg: str | None) -> None:
    import asyncio

    asyncio.run(run_stream_processor(config_path=cfg))


if __name__ == "__main__":
    args = parse_args()
    # Run stream + API in separate processes (mirrors prod separation).
    p_stream = mp.Process(target=_run_stream, args=(args.config,), daemon=True)
    p_stream.start()
    time.sleep(0.5)
    run_api(config_path=args.config)
