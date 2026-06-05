Fugl Artsdataanalyser: Norwegian Artsdatabanken species-data project using marimo.

Main files: `databehandling/databehandling.py` (ingest/validate/enrich/export), `dataanalyse/data_analyse.py` (analysis/presentation), tests in `tests_KI/`.

Rules: Norwegian UI/markdown/CLI/comments; English docstrings OK. Follow `databehandling.py`: explicit typed pipeline functions; Polars expressions over pandas; DuckDB when useful; early validation/helpful errors; nearby domain constants; Parquet outputs.
