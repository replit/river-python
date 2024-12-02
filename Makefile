lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy .
	uv run pyright-python .
	uv run deptry .

format:
	uv run ruff format .
	uv run ruff check . --fix
