# Marimo Notebook Conventions

## App Setup

- Files are marimo apps: `app = marimo.App(width="columns")` or `width="medium"`
- End files with `if __name__ == "__main__": app.run()`

## Cell Structure

- Each cell is a function decorated with `@app.cell()`
- Cells declare dependencies via function parameters (marimo's reactive model)
- Cells return values as tuples to expose them to other cells
- Use `hide_code=True` for markdown/UI cells
- Use `column=N` for multi-column layouts

## Imports

- Place all imports in a single dedicated cell (typically the last major cell)
- Return all imported names as a tuple from that cell
- Use `import marimo as mo` and `import polars as pl` consistently
