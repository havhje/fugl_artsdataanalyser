import marimo

__generated_with = "0.23.2"
app = marimo.App(width="columns", layout_file="layouts/data_analyse.grid.json")

with app.setup:
    import marimo as mo
    import polars as pl


@app.cell
def _():
    valgt_fil = mo.ui.file_browser()
    valgt_fil
    return (valgt_fil,)


@app.cell
def _(valgt_fil):
    file_info = valgt_fil.value[0]
    arter_df = pl.read_parquet (file_info.path)
    artsdata_df = mo.ui.table(arter_df, page_size=20)

    return (artsdata_df,)


@app.cell
def _(artsdata_df):
    artsdata_df
    return


@app.cell(column=1)
def _():
    return


if __name__ == "__main__":
    app.run()
