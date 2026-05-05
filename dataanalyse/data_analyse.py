import marimo

__generated_with = "0.23.2"
app = marimo.App(width="columns", layout_file="layouts/data_analyse.grid.json")

with app.setup:
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import leafmap.foliumap as leafmap


@app.cell
def _():
    valgt_fil = mo.ui.file_browser()
    valgt_fil
    return (valgt_fil,)


@app.cell
def _(valgt_fil):
    file_info = valgt_fil.value[0]
    arter_df = pl.read_parquet(file_info.path)
    artsdata_df = mo.ui.table(arter_df, page_size=20)
    return arter_df, artsdata_df


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Dataanalyse
    "Info om notatboken her"
    """)
    return


@app.cell
def _(artsdata_df):
    artsdata_df
    return


@app.cell(column=1, hide_code=True)
def _():
    mo.md(r"""
    #Kart
    """)
    return


@app.cell
def plotlymap(arter_df):
    plotly_map_fig = px.scatter_map(
        arter_df,
        lat="latitude",
        lon="longitude",
        hover_name="Navn",
        hover_data=["Art", "Antall", "Observert dato", "Kommune", "Fylke"],
        custom_data=["Artens ID", "Navn"],
        zoom=8,
        height=650,
        map_style="open-street-map",
    )
    plotly_map_fig.update_traces(
        marker={"size": 8, "opacity": 0.75},
        selected={"marker": {"size": 11, "opacity": 1.0}},
        unselected={"marker": {"opacity": 0.25}},
    )
    plotly_map_fig.update_layout(
        dragmode="lasso",
        clickmode="event+select",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    plotly_map = mo.ui.plotly(
        plotly_map_fig,
        config={"scrollZoom": True, "displaylogo": False},
    )
    plotly_map
    return (plotly_map,)


@app.cell
def _(arter_df, plotly_map):
    selected_arter_df = (
        arter_df.with_row_index("__row_nr").filter(pl.col("__row_nr").is_in(plotly_map.indices)).drop("__row_nr")
    )

    mo.vstack(
        [
            mo.md(f"**Valgte observasjoner:** {selected_arter_df.height}"),
            mo.ui.table(selected_arter_df, page_size=10),
        ]
    )
    return


@app.cell(column=2)
def _():
    return


@app.cell(column=3)
def _():
    return


if __name__ == "__main__":
    app.run()
