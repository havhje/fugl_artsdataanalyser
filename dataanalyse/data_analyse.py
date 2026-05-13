import marimo

__generated_with = "0.23.5"
app = marimo.App(width="columns", layout_file="layouts/data_analyse.grid.json")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import leafmap.foliumap as leafmap
    import colorcet as cc
    import holoviews.operation.datashader as h
    import hvplot.polars
    import holoviews as hv
    import datashader as ds
    import geopandas as gpd
    from holoviews.element.tiles import EsriImagery


@app.cell
def _():
    valgt_fil = mo.ui.file_browser()
    valgt_fil
    return (valgt_fil,)


@app.cell(hide_code=True)
def _(valgt_fil):
    file_info = valgt_fil.value[0]

    arter_df_lest_inn = pl.read_parquet(file_info.path)

    artsdata_df = mo.ui.table(arter_df_lest_inn, page_size=20)
    return (artsdata_df,)


@app.cell(hide_code=True)
def _(artsdata_df):
    arter_df = artsdata_df.value
    return (arter_df,)


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
    ### Selekteringskart
    """)
    return


@app.cell
def _():
    farge_kart_arter = mo.ui.dropdown(
        options=["Navn", "Verdi M1941", "Atferd"],
        value="Verdi M1941",
        label="Farge på punkter (punkter hvor atferd ikke er registrert vises ikke i kartet)",
    )

    mo.vstack(
        [
            farge_kart_arter,
            mo.md(
                "*Merk: Punkter uten registrert atferd vises ikke når kartet fargelegges etter atferd (eller punkter med null values)*"
            ),
        ]
    )
    return (farge_kart_arter,)


@app.cell
def _(farge_kart_arter):
    farge_kart_arter
    return


@app.cell
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

    plotly_arter_df = arter_df.with_row_index("__row_nr")
    plotly_kartflis_lag = [
        {
            "below": "traces",
            "source": ["https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"],
            "sourceattribution": "© OpenStreetMap-bidragsytere © CARTO",
            "sourcetype": "raster",
        }
    ]

    plotly_map_fig = px.scatter_map(
        plotly_arter_df,
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
            "Art",
        ],
        custom_data=["__row_nr", "Artens ID", "Navn"],
        zoom=8,
        height=650,
        map_style="white-bg",
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
        map_layers=plotly_kartflis_lag,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    plotly_map = mo.ui.plotly(
        plotly_map_fig,
        config={"scrollZoom": True, "displaylogo": False},
    )
    plotly_map
    return plotly_map, plotly_map_fig


@app.cell
def _():
    mo.md(r"""
    #Heatmap
    """)
    return


@app.cell(hide_code=True)
def test(selected_arter_df):
    arter_pdf = selected_arter_df.to_pandas()

    arter_gdf = gpd.GeoDataFrame(
        arter_pdf,
        geometry=gpd.points_from_xy(arter_pdf["longitude"], arter_pdf["latitude"]),
        crs="EPSG:4326",  # lat/lon
    ).to_crs("EPSG:3857")  # Web Mercator for kartfliser

    arter_map_df = pl.from_pandas(
        arter_gdf.assign(
            x_webmercator=arter_gdf.geometry.x,
            y_webmercator=arter_gdf.geometry.y,
        ).drop(columns="geometry")
    )
    return (arter_map_df,)


@app.cell
def _():
    max_px_value = mo.ui.slider(start=1, stop=10, step=2, value=10, show_value=True, label="Maks spredning")
    threshold_value = mo.ui.slider(start=0.1, stop=1, value=0.95, show_value=True, label="Terskel")
    return max_px_value, threshold_value


@app.cell
def _(max_px_value, threshold_value):
    max_px_value
    threshold_value

    stack = mo.vstack([max_px_value, threshold_value])

    stack
    return


@app.cell
def heatmap(arter_map_df, max_px_value, threshold_value):
    species_density = arter_map_df.hvplot.points(
        x="x_webmercator",
        y="y_webmercator",
        rasterize=True,
        dynspread=True,
        max_px=max_px_value.value,
        threshold=threshold_value.value,
        aggregator=ds.count(),
        cnorm="eq_hist",
        cmap=cc.fire[100:],
        width=900,
        height=700,
        xaxis=None,
        yaxis=None,
    )

    EsriImagery().opts(alpha=0.75) * species_density
    return


@app.cell
def _(arter_df, plotly_map, plotly_map_fig):
    def get_selected_row_nrs(points, figure):
        """For every selected map point, use its curveNumber to find the right Plotly trace, use its pointIndex to find the right point inside that trace, look in that point’s hidden customdata, take the first value, convert it to an integer, and return all those integers as a list."""
        return [int(figure.data[point["curveNumber"]].customdata[point["pointIndex"]][0]) for point in points]


    selected_row_nrs = get_selected_row_nrs(plotly_map.points, plotly_map_fig)

    selected_arter_df = (
        arter_df.with_row_index("__row_nr").filter(pl.col("__row_nr").is_in(selected_row_nrs)).drop("__row_nr")
    )

    mo.vstack(
        [
            mo.md(f"**Valgte observasjoner:** {selected_arter_df.height}"),
            mo.ui.table(selected_arter_df, page_size=10),
        ]
    )
    return (selected_arter_df,)


@app.cell(column=2)
def _():
    return


if __name__ == "__main__":
    app.run()
