# Python Code Style and Naming

## Naming Conventions

- **Functions:** `snake_case` in English (`fetch_taxon_data`, `extract_hierarchy_and_ids`)
- **Variables:** `snake_case`, Norwegian is acceptable for domain-specific names
  (`arter_etter_1990`, `mangler_navn`, `artsdata_df`)
- **Constants:** `UPPER_SNAKE_CASE` (`NORTAXA_API_BASE_URL`, `DESIRED_RANKS`)
- **DataFrames:** suffix with `_df` (`artsdata_df`, `system_arter_df`)
- **Column names:** Norwegian with spaces for user-facing columns
  (`"Observert dato"`, `"Art av nasjonal forvaltningsinteresse"`)

## Functions

- Docstrings in English, Google-style or NumPy-style format
- Include parameter types and return types in both signatures and docstrings
- Keep functions pure where possible (take DataFrame in, return DataFrame out)
- Use the pipe pattern: `df.pipe(func1).pipe(func2).pipe(func3)`

## Type Hints

- Add type hints to function signatures for new code
- Use `pl.DataFrame` for polars DataFrames
- Use `str | None` style (Python 3.10+ union syntax)

## DataFrame Operations (Polars)

- Prefer polars over pandas for all new data processing
- Use `pl.col()` expressions, not bracket indexing
- Chain operations with `.with_columns()`, `.select()`, `.filter()`
- Use `.pipe()` for composing transformation functions
- Always specify `return_dtype` in `map_elements()` calls
- Use `pl.Utf8` for string columns, `pl.Float64`/`pl.Int64` for numerics

## Formatting

- ruff with default settings (no custom config in pyproject.toml)
