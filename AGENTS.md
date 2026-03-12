# AGENTS.md - Artsdatabanken Species Data Analyzer

## Project Overview

Norwegian species data analysis tool built as interactive **marimo notebooks**.
Fetches taxonomy data from Artsdatabanken's NorTaxa API, enriches species
observations with conservation status and national management criteria, and
performs spatial overlay analysis against ecosystem maps.

**Language:** Norwegian (comments, UI text, column names). Code identifiers and
docstrings use English.

## Tech Stack

- **Python 3.14** (managed via `uv`, see `.python-version`)
- **marimo** - reactive notebook framework (all `.py` files are marimo apps)
- **polars** - primary DataFrame library (not pandas for new code)
- **DuckDB** - SQL queries via `mo.sql()`, including spatial extensions
- **plotly** - interactive maps and visualizations
- **pyproj** - coordinate transformations (UTM 33N / EPSG:25833)
- **requests** - HTTP calls to Artsdatabanken APIs
- **pydantic-ai-slim** - AI agent framework (dependency, not yet heavily used)
- **ruff** - linter and formatter (dev dependency)
- **pytest** - test runner (dev dependency, no tests exist yet)

## Build / Run / Test Commands

```bash
# Install dependencies (uses uv with lockfile)
uv sync

# Install with dev dependencies
uv sync --group dev

# Run a marimo notebook in edit mode (interactive browser UI)
uv run marimo edit Databehandling.py
uv run marimo edit arter_i_polygoner.py

# Run a notebook as a read-only app
uv run marimo run Databehandling.py

# Lint (ruff - no custom config, uses defaults)
uv run ruff check .
uv run ruff check --fix .

# Format
uv run ruff format .
uv run ruff format --check .

# Run all tests (none exist yet)
uv run pytest

# Run a single test file
uv run pytest tests/test_example.py

# Run a single test function
uv run pytest tests/test_example.py::test_function_name

# Run tests with verbose output
uv run pytest -v

# Run tests matching a keyword
uv run pytest -k "keyword"
```

## Project Structure

```
.
├── Databehandling.py                      # Main marimo notebook: data enrichment pipeline
├── arter_i_polygoner.py                   # Marimo notebook: spatial overlay analysis
├── Arter av nasjonal forvaltningsinteresse/
│   └── arter_av_nasjonal_forvaltning.xlsx # Reference data: national management species
├── layouts/
│   └── Artsdata.grid.json                 # Marimo grid layout config
├── __marimo__/                            # Marimo session data (auto-generated)
├── pyproject.toml                         # Project config and dependencies
├── uv.lock                                # Locked dependency versions
└── .python-version                        # Python 3.14
```

## Code Style Guidelines

### Marimo Notebook Structure

- Files are marimo apps: `app = marimo.App(width="columns")` or `width="medium"`
- Each cell is a function decorated with `@app.cell()`
- Cells declare dependencies via function parameters (marimo's reactive model)
- Cells return values as tuples to expose them to other cells
- The imports cell typically lives at the end or in a specific column
- Use `hide_code=True` for markdown/UI cells
- Use `column=N` for multi-column layouts
- End files with `if __name__ == "__main__": app.run()`

### Imports

- Place all imports in a single dedicated cell (typically the last major cell)
- Return all imported names as a tuple from that cell
- Standard library first, then third-party, then local
- Use `import marimo as mo` and `import polars as pl` consistently
- Never use `from module import *`

### DataFrame Operations

- **Prefer polars over pandas** for all new data processing
- Use `pl.col()` expressions, not bracket indexing
- Chain operations with `.with_columns()`, `.select()`, `.filter()`
- Use `.pipe()` for composing transformation functions
- Always specify `return_dtype` in `map_elements()` calls
- Use `pl.Utf8` for string columns, `pl.Float64`/`pl.Int64` for numerics

### Naming Conventions

- **Functions:** `snake_case` in English (`fetch_taxon_data`, `extract_hierarchy_and_ids`)
- **Variables:** `snake_case`, Norwegian is acceptable for domain-specific names
  (`arter_etter_1990`, `mangler_navn`, `artsdata_df`)
- **Constants:** `UPPER_SNAKE_CASE` (`NORTAXA_API_BASE_URL`, `DESIRED_RANKS`)
- **DataFrames:** suffix with `_df` (`artsdata_df`, `system_arter_df`)
- **Column names:** Norwegian with spaces for user-facing columns
  (`"Observert dato"`, `"Art av nasjonal forvaltningsinteresse"`)

### Functions

- Write docstrings in English using Google-style or NumPy-style format
- Include parameter types and return types in docstrings
- Keep functions pure where possible (take DataFrame in, return DataFrame out)
- Use the pipe pattern: `df.pipe(func1).pipe(func2).pipe(func3)`

### SQL (DuckDB via mo.sql)

- Use f-strings to inject table names into SQL queries
- DuckDB spatial extension: `INSTALL spatial; LOAD spatial;`
- Coordinate system is UTM Zone 33N (EPSG:25833) throughout
- Use `ST_GeomFromText()`, `ST_Intersects()`, `ST_Read()` for spatial ops

### Error Handling

- Use try/except around HTTP requests with specific exception types
- Print errors with context: `print(f"Error fetching ID {id}: {e}")`
- Use `mo.stop(condition, message)` to halt notebook execution with a UI message
- Return `None` from functions that may fail (API calls)
- Check for null columns with `pl.col("x").is_null()`

### API Usage

- Base URL: `https://nortaxa.artsdatabanken.no/api/v1/TaxonName`
- Apply rate limiting (`time.sleep()`) between API calls
- Use `@lru_cache` for caching repeated API lookups
- Always set `timeout=10` on `requests.get()` calls
- Use `response.ok` to check HTTP success

### Type Hints

- Add type hints to function signatures for new code
- Use `pl.DataFrame` for polars DataFrames
- Use `str | None` style (Python 3.10+ union syntax)
- No strict type checking is enforced yet; ruff defaults apply

### Formatting

- **ruff** with default settings (no custom config in pyproject.toml)
- Line length: 88 (ruff default)
- Use double quotes for strings
- Trailing commas in multi-line structures
- No unused imports or variables (ruff will flag these)

### Git

- `.gitignore` excludes: `__pycache__/`, `*.py[oc]`, `build/`, `dist/`,
  `*.egg-info`, `test_data/`, `.venv`
- Do not commit `.venv/`, `__marimo__/`, or `test_data/`

## Key Domain Concepts

- **Artsdatabanken** - Norwegian Biodiversity Information Centre
- **NorTaxa** - Norwegian taxonomy database/API
- **IUCN categories** - CR, EN, VU, NT, LC (conservation status)
- **M1941** - Norwegian valuation method for species importance
- **Arter av nasjonal forvaltningsinteresse** - Species of national management interest
- **Hovedokosystem** - Main ecosystem map service (Miljodirektoratet)
- Coordinate system: **UTM Zone 33N (EPSG:25833)** for all spatial data
