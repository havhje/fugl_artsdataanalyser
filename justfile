# Checks to run before declaring work done. `just fix` applies the autofixes.

marimo := "uv run --frozen marimo"

# Fail on any lint, format, notebook-structure or test problem
check:
    {{marimo}} check --strict --ignore-scripts .
    ruff check .
    ruff format --check .
    uv run --frozen pytest -q

# Apply autofixes; marimo runs last so its notebook layout wins
fix:
    ruff check --fix .
    ruff format .
    {{marimo}} check --fix --ignore-scripts .
