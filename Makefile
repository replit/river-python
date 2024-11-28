lint:
	uv run black --check .
	uv run ruff check .
	uv run mypy .
	uv run pyright-python .

format:
	uv run black .
	uv run ruff check . --fix
