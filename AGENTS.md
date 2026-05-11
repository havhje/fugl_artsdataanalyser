Fugl Artsdataanalyser: Norwegian Artsdatabanken species-data project using marimo.

Main files: `databehandling/databehandling.py` (ingest/validate/enrich/export), `dataanalyse/data_analyse.py` (analysis/presentation), tests in `tests_KI/`.

Use `uv`; after changes prefer `uv run marimo check` and `uv run pytest tests_KI`.

Rules: Norwegian UI/markdown/CLI/comments; English docstrings OK. Follow `databehandling.py`: explicit typed pipeline functions; Polars expressions over pandas; DuckDB when useful; early validation/helpful errors; nearby domain constants; Parquet outputs.

Tests: add pytest files in `tests_KI/`; no notebook test cells unless requested.
