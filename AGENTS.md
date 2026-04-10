Norwegian species data analysis tool built as interactive marimo notebooks, using polars and DuckDB for spatial/taxonomic data from Artsdatabanken.

**Language:** Inline comments and UI text in Norwegian. Code identifiers, docstrings, and function names in English.

This project uses `uv` as the package manager and uses marimo check as a notebook linter. Run this after every edit.

```bash
uv sync                        # install dependencies
uv sync --group dev            # install with dev dependencies
uv run marimo edit <file>.py   # run notebook in edit mode
uv run marimo check
uv run pytest                  # run tests
```

For marimo notebook conventions, see docs/MARIMO.md
For Python code style and naming, see docs/PYTHON_CONVENTIONS.md
For API usage, spatial data, and domain concepts, see docs/API_AND_DATA.md
For Git workflow, see docs/GIT.md
