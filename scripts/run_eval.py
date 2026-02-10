from __future__ import annotations

import argparse

from evaluation.evaluate import run_evaluation


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_evaluation(config_path=args.config)
