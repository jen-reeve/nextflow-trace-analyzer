.PHONY: test install-dev coverage

install-dev:
	pip install -r requirements-dev.txt

test:
	pytest -q

coverage:
	pytest --cov=nextflow_trace_analyzer --cov-report=term
