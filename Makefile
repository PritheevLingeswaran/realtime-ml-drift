.PHONY: install test lint format run_api run_stream gen_stream eval run_all

install:
	python -m pip install -r requirements.txt

test:
	python -m pytest

lint:
	python -m ruff check src tests scripts evaluation

format:
	python -m ruff format .

gen_stream:
	python scripts/generate_stream.py --config configs/dev.yaml --out data/raw/streams/dev_stream.jsonl

run_api:
	python scripts/run_api.py --config configs/dev.yaml

run_stream:
	python scripts/run_stream.py --config configs/dev.yaml

eval:
	python scripts/run_eval.py --config configs/dev.yaml

run_all:
	python scripts/run_all.py --config configs/dev.yaml
