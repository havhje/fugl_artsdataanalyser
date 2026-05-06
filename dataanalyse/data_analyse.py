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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Heatmap
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Selekteringskart
    """)
    return


@app.cell
def _():
    farge_kart_arter = mo.ui.dropdown(
        options=["Navn", "Verdi M1941", "Atferd"], value="Verdi M1941", label="Farge på punkter"
    )
    return (farge_kart_arter,)


@app.cell
def _(farge_kart_arter):
    farge_kart_arter
    return


@app.cell(hide_code=True)
def plotlymap(arter_df, farge_kart_arter):
    verdi_m1941_color_map = {
        "Svært stor verdi": "#AF0F0F",
        "Stor verdi": "#FD7032",
        "Middels verdi": "#FEC02D",
        "Noe verdi": "#FFFF00",
        "Uten betydning for KU": "#D9D9D9",
        "Ikke definert": "#000000",
    }

    verdi_m1941_draw_order = [
        "Uten betydning for KU",
        "Ikke definert",
        "Noe verdi",
        "Middels verdi",
        "Stor verdi",
        "Svært stor verdi",
    ]

    atferd_priority_order = [
        "reproductive",
        "possiblereproductive",
        "feeding",
        "stationary",
        "moving",
        "dead",
    ]
    atferd_draw_order = list(reversed(atferd_priority_order))

    plotly_color_kwargs = {}
    if farge_kart_arter.value == "Verdi M1941":
        plotly_color_kwargs = {
            "color_discrete_map": verdi_m1941_color_map,
            "category_orders": {"Verdi M1941": verdi_m1941_draw_order},
        }
    elif farge_kart_arter.value == "Atferd":
        plotly_color_kwargs = {
            "category_orders": {"Atferd": atferd_draw_order},
        }

    plotly_map_fig = px.scatter_map(
        arter_df,
        lat="latitude",
        lon="longitude",
        hover_name="Navn",
        hover_data=[
            "Antall",
            "Kategori",
            "Art av nasjonal forvaltningsinteresse",
            "Atferd",
            "Observert dato",
            "Verdi M1941",
        ],
        custom_data=["Artens ID", "Navn"],
        zoom=8,
        height=650,
        map_style="open-street-map",
        color=farge_kart_arter.value,
        **plotly_color_kwargs,
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


if __name__ == "__main__":
    app.run()
