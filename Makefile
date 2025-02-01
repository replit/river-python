lint:
	uv run ruff format --check src tests
	uv run ruff check src tests
	uv run mypy src tests
	uv run pyright-python src tests
	uv run deptry src tests

format:
	uv run ruff format src tests
	uv run ruff check src tests --fix
