from __future__ import annotations

import argparse

from src.api.app import run_api


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_api(config_path=args.config)
