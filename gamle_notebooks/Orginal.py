import marimo

__generated_with = "0.23.2"
app = marimo.App(width="columns")


@app.cell(column=0, hide_code=True)
def _(mo):
    mo.md(r"""
    ### Kart
    """)
    return


@app.cell(hide_code=True)
def _(artsdata_df):
    artsdata_kart = artsdata_df.value
    return (artsdata_kart,)


@app.cell(hide_code=True)
def _(mo):
    # Lager UI elementer for å velge kart
    map_style_dropdown = mo.ui.dropdown(
        options=[
            "carto-positron",
            "carto-darkmatter",
            "open-street-map",
        ],
        value="carto-positron",
        label="Select a base map style:",
    )

    satellite_toggle = mo.ui.checkbox(value=True, label="Show Satellite Imagery")
    return map_style_dropdown, satellite_toggle


@app.cell(hide_code=True)
def _(map_style_dropdown, mo, satellite_toggle):
    controls = mo.vstack([map_style_dropdown, satellite_toggle])
    controls
    return


@app.cell(hide_code=True)
def _(mo):
    # Create a dropdown to switch between count and sum
    aggregation_mode = mo.ui.dropdown(
        options=["Antall observasjoner", "Sum individer"],
        value="Antall observasjoner",
        label="Aggregation mode:",
    )
    aggregation_mode
    return (aggregation_mode,)


@app.cell(hide_code=True)
def _(
    aggregation_mode,
    artsdata_kart,
    ff,
    map_style_dropdown,
    mo,
    np,
    satellite_toggle,
):
    # Set parameters based on aggregation mode
    if aggregation_mode.value == "Antall observasjoner":
        color_param = None
        agg_func_param = None
        label_text = "Antall observasjoner"
    else:
        color_param = "Antall"
        agg_func_param = np.sum
        label_text = "Sum individer"

    # Create the hexbin map with conditional parameters
    fig_hex = ff.create_hexbin_mapbox(
        data_frame=artsdata_kart,
        lat="latitude",
        lon="longitude",
        color=color_param,  # None for count, "Antall" for sum
        nx_hexagon=15,
        opacity=0.5,
        labels={"color": label_text},
        min_count=1,
        color_continuous_scale="Viridis",
        show_original_data=True,
        original_data_marker=dict(size=4, opacity=0.6, color="deeppink"),
        agg_func=agg_func_param,  # None for count, np.sum for sum
    )

    # Apply map style settings
    fig_hex.update_layout(mapbox_style=map_style_dropdown.value, height=1000)

    # Conditionally add the satellite layer based on the checkbox's value
    if satellite_toggle.value:
        fig_hex.update_layout(
            mapbox_style="white-bg",
            mapbox_layers=[
                {
                    "below": "traces",
                    "sourcetype": "raster",
                    "source": [
                        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                    ],
                }
            ],
        )
    else:
        fig_hex.update_layout(mapbox_layers=[])

    hekskart = mo.ui.plotly(fig_hex)
    hekskart
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Punktkart
    """)
    return


@app.cell(hide_code=True)
def _(artsdata_kart, map_style_dropdown, mo, px, satellite_toggle):
    fig = px.scatter_map(
        artsdata_kart,
        lat="latitude",
        lon="longitude",
        color="Kategori",
        size="Antall",
        size_max=100,
        zoom=10,
        hover_name="Navn",
    )

    fig.update_layout(map_style=map_style_dropdown.value, height=1000)

    # Conditionally add the satellite layer based on the checkbox's value
    if satellite_toggle.value:
        fig.update_layout(
            map_layers=[
                {
                    "below": "traces",
                    "sourcetype": "raster",
                    "source": [
                        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                    ],
                }
            ]
        )
    else:
        # An empty list removes any existing raster layers
        fig.update_layout(map_layers=[])

    satelitt_kart = mo.ui.plotly(fig)

    # Display the marimo element
    satelitt_kart
    return


@app.cell(column=1, hide_code=True)
def _():
    import json
    import os
    import tempfile
    import time

    import altair as alt
    import marimo as mo
    import numpy as np
    import pandas as pd
    import plotly.express as px
    import plotly.figure_factory as ff
    import plotly.graph_objects as go
    import polars as pl
    import pyproj
    import requests

    return alt, ff, go, mo, np, pl, px


@app.cell(hide_code=True)
def _(mo):
    valgt_fil = mo.ui.file_browser()
    valgt_fil
    return (valgt_fil,)


@app.cell(hide_code=True)
def _(valgt_fil):
    file_info = valgt_fil.value[0]
    filepath = file_info.path
    str(filepath)
    return


@app.cell
def _(mo, pl):
    arter_df = pl.read_parquet ("/home/havhje/koding/fugl_artsdataanalyser/data_csv_gammel/output.parquet")
    artsdata_df = mo.ui.table(arter_df, page_size=20)
    artsdata_df
    return (artsdata_df,)


@app.cell(column=2, hide_code=True)
def _(artsdata_df, mo, pl):
    # Calculate summary statistics from the data
    _data = artsdata_df.value

    # Basic counts
    total_species = _data.select("Navn").unique().height
    total_observations = _data.height
    total_individuals = _data.select("Antall").sum().item()

    # Get unique species with all categories
    _species_unique = _data.select(
        [
            "Navn",
            "Kategori",
            "Ansvarsarter",
            "Andre spesielt hensynskrevende arter",
            "Prioriterte arter",
            "Spesielle økologiske former",
        ]
    ).unique()

    # Count by red list category - ALL categories
    cr_count = _species_unique.filter(pl.col("Kategori") == "CR").height
    en_count = _species_unique.filter(pl.col("Kategori") == "EN").height
    vu_count = _species_unique.filter(pl.col("Kategori") == "VU").height
    nt_count = _species_unique.filter(pl.col("Kategori") == "NT").height
    lc_count = _species_unique.filter(pl.col("Kategori") == "LC").height
    dd_count = _species_unique.filter(pl.col("Kategori") == "DD").height
    ne_count = _species_unique.filter(pl.col("Kategori") == "NE").height
    na_count = _species_unique.filter(pl.col("Kategori") == "NA").height

    # Calculate threatened and red list totals
    threatened_total = cr_count + en_count + vu_count
    redlist_total = threatened_total + nt_count

    # Count specific management categories
    ansvarsarter_count = _species_unique.filter(
        pl.col("Ansvarsarter") == True
    ).height
    hensynskrevende_count = _species_unique.filter(
        pl.col("Andre spesielt hensynskrevende arter") == True
    ).height
    prioriterte_count = _species_unique.filter(
        pl.col("Prioriterte arter") == True
    ).height
    okologiske_former_count = _species_unique.filter(
        pl.col("Spesielle økologiske former") == True
    ).height

    # Check for alien species (fremmed art)
    fremmed_count = _species_unique.filter(
        pl.col("Kategori").is_in(["SE", "HI", "PH", "LO", "NK"])
    ).height

    summary_text = mo.md(f"""
    ## Datasettsammendrag

    ### Generell statistikk
    - **Totalt antall arter:** {total_species}
    - **Totalt antall observasjoner:** {total_observations}
    - **Totalt antall individer:** {total_individuals}

    ### Rødlistekategorier
    - **Totalt rødlistevurderte arter (CR+EN+VU+NT):** {redlist_total}
    - Kritisk truet (CR): {cr_count}
    - Sterkt truet (EN): {en_count}
    - Sårbar (VU): {vu_count}
    -  Nær truet (NT): {nt_count}
    -  Livskraftig (LC): {lc_count}
    -  Datamangel (DD): {dd_count}
    -  Ikke vurdert (NE): {ne_count}
    -  Ikke egnet (NA): {na_count}


    ### Forvaltningskategorier
    {f"- **Ansvarsarter:** {ansvarsarter_count}" if ansvarsarter_count > 0 else ""}
    {f"- **Andre spesielt hensynskrevende arter:** {hensynskrevende_count}" if hensynskrevende_count > 0 else ""}
    {f"- **Prioriterte arter:** {prioriterte_count}" if prioriterte_count > 0 else ""}
    {f"- **Spesielle økologiske former:** {okologiske_former_count}" if okologiske_former_count > 0 else ""}

    ### Andre kategorier
    {f"- **Fremmede arter:** {fremmed_count}" if fremmed_count > 0 else ""}
    """)

    summary_text
    return


@app.cell(hide_code=True)
def _(artsdata_df, go, mo, pl):
    # Get the selected data
    _pie_data = artsdata_df.value

    # Group by species first to get unique species, then count by category
    _kategori_counts = (
        _pie_data.select(["Navn", "Kategori"])
        .unique()
        .group_by("Kategori")
        .agg([pl.len().alias("count")])
        # Filter to only include specified categories
        .filter(
            pl.col("Kategori").is_in(
                ["LC", "NT", "VU", "EN", "CR", "DD", "NE", "NA"]
            )
        )
    )

    # Norwegian category names
    _kategori_names = {
        "CR": "Kritisk truet",
        "EN": "Sterkt truet",
        "VU": "Sårbar",
        "NT": "Nær truet",
        "LC": "Livskraftig",
        "DD": "Datamangel",
        "NE": "Ikke vurdert",
        "NA": "Ikke egnet",
    }

    # Add Norwegian names with code and count to the dataframe
    _kategori_counts = _kategori_counts.with_columns(
        pl.concat_str(
            [
                pl.col("Kategori").map_elements(
                    lambda x: _kategori_names.get(x, x), return_dtype=pl.Utf8
                ),
                pl.lit(" ["),
                pl.col("Kategori"),
                pl.lit("]; "),
                pl.col("count").cast(pl.Utf8),
            ]
        ).alias("Kategori_norsk")
    )

    # Define the specific order for the pie chart (from least to most threatened)
    _category_order = ["LC", "NT", "VU", "EN", "CR", "DD", "NE", "NA"]

    # Add sort column and sort by the defined order
    _kategori_counts = (
        _kategori_counts.with_columns(
            pl.col("Kategori")
            .map_elements(
                lambda x: _category_order.index(x)
                if x in _category_order
                else 999,
                return_dtype=pl.Int32,
            )
            .alias("sort_order")
        )
        .sort("sort_order")
        .drop("sort_order")
    )

    # Convert to pandas and ensure order is maintained
    _kategori_df = _kategori_counts.to_pandas()

    # Create ordered lists for plotly (ensuring the exact order we want)
    _ordered_names = []
    _ordered_counts = []
    for cat in _category_order:
        _row = _kategori_df[_kategori_df["Kategori"] == cat]
        if not _row.empty:
            _ordered_names.append(_row["Kategori_norsk"].values[0])
            _ordered_counts.append(_row["count"].values[0])

    # Official IUCN Red List color scheme
    _iucn_colors_by_code = {
        "CR": "#D81E05",  # Critically Endangered
        "EN": "#FC7F3F",  # Endangered
        "VU": "#F9E814",  # Vulnerable
        "NT": "#CCE226",  # Near threatened
        "LC": "#60C659",  # Least Concern
        "DD": "#D1D1C6",  # Data Deficient
        "NE": "#FFFFFF",  # Not Evaluated
        "NA": "#C1B5A5",  # Not Applicable
    }

    # Create color list in the same order as the data
    _ordered_colors = []
    for cat in _category_order:
        _row = _kategori_df[_kategori_df["Kategori"] == cat]
        if not _row.empty:
            _ordered_colors.append(_iucn_colors_by_code.get(cat, "#888888"))

    # Create pie chart with explicit ordering
    fig_pie = go.Figure(
        data=[
            go.Pie(
                labels=_ordered_names,
                values=_ordered_counts,
                marker=dict(colors=_ordered_colors),
                hole=0.3,
                textposition="inside",
                textinfo="label",
                hovertemplate="<b>%{label}</b><br>Arter: %{value}<br>Andel: %{percent}<extra></extra>",
                sort=False,  # Important: don't let plotly sort
            )
        ]
    )

    # Update layout for better appearance
    fig_pie.update_layout(
        title="Antall arter fordelt på rødlistekategorier",
        height=600,
        width=800,
        showlegend=False,
    )

    mo.ui.plotly(fig_pie)
    return


@app.cell(hide_code=True)
def _(artsdata_df, pl):
    import great_tables as gt

    # Get selected data from the table
    _obs_data = artsdata_df.value

    # Calculate per-species statistics - INCLUDING reproductive activities in the initial aggregation
    _species_stats = (
        _obs_data.group_by("Navn")
        .agg(
            [
                pl.len().alias("Observasjoner"),
                pl.col("Antall").sum().alias("Individer"),
                pl.col("Familie").first().alias("Familie"),
                pl.col("Orden").first().alias("Orden"),
                pl.col("Kategori").first().alias("Kategori"),
                pl.col("Ansvarsarter").first().alias("Ansvarsarter"),
                pl.col("Andre spesielt hensynskrevende arter")
                .first()
                .alias("Andre spesielt hensynskrevende arter"),
                pl.col("Prioriterte arter").first().alias("Prioriterte arter"),
                pl.col("Observert dato").dt.year().min().alias("År fra"),
                pl.col("Observert dato").dt.year().max().alias("År til"),
                pl.col("Antall").mean().alias("Gj.snitt individer"),
                pl.col("Observert dato")
                .dt.month()
                .unique()
                .sort()
                .alias("Måneder_num"),
                # Add reproductive activity counts directly here
                (pl.col("Atferd") == "reproductive").sum().alias("Reproduksjon"),
                (pl.col("Atferd") == "possiblereproductive").sum().alias("Mulig reproduksjon"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("År fra") == pl.col("År til"))
                .then(pl.col("År fra").cast(pl.Utf8))
                .otherwise(
                    pl.concat_str(
                        [pl.col("År fra"), pl.lit("-"), pl.col("År til")]
                    )
                )
                .alias("År-periode"),
                pl.col("Måneder_num")
                .map_elements(
                    lambda months: ", ".join(
                        [
                            [
                                "Jan",
                                "Feb",
                                "Mar",
                                "Apr",
                                "Mai",
                                "Jun",
                                "Jul",
                                "Aug",
                                "Sep",
                                "Okt",
                                "Nov",
                                "Des",
                            ][m - 1]
                            for m in months
                            if m is not None
                        ]
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("Måneder"),
                # Create Øvrige kategorier column
                pl.concat_list(
                    [
                        pl.when(pl.col("Ansvarsarter"))
                        .then(pl.lit("Ansvarsart"))
                        .otherwise(pl.lit(None)),
                        pl.when(pl.col("Andre spesielt hensynskrevende arter"))
                        .then(pl.lit("Hensynskrevende"))
                        .otherwise(pl.lit(None)),
                        pl.when(pl.col("Prioriterte arter"))
                        .then(pl.lit("Prioritert"))
                        .otherwise(pl.lit(None)),
                    ]
                )
                .list.drop_nulls()
                .list.join(", ")
                .alias("Øvrige kategorier"),
            ]
        )
    )

    # Define custom sort order for Kategori
    _kategori_order = {"CR": 1, "EN": 2, "VU": 3, "NT": 4, "LC": 5}

    # Add sort key and sort by Kategori order, then by Observasjoner
    _species_stats = (
        _species_stats.with_columns(
            pl.col("Kategori")
            .map_elements(
                lambda x: _kategori_order.get(x, 999), return_dtype=pl.Int32
            )
            .alias("kategori_sort")
        )
        .sort(["kategori_sort", "Observasjoner"], descending=[False, True])
        .drop("kategori_sort")
    )

    # IMPORTANT: Remove underscore to make accessible from other cells
    species_stats_formatted = _species_stats.select(
        [
            "Kategori",
            "Øvrige kategorier",
            "Navn",
            "Observasjoner",
            "Individer",
            "Gj.snitt individer",
            "År-periode",
            "Måneder",
            "Familie",
            "Orden",
            "Reproduksjon",
            "Mulig reproduksjon",
        ]
    )

    # Create the species statistics table
    species_table = (
        gt.GT(species_stats_formatted.to_pandas())  # Show all species
        .tab_header(
            title="Statistikk per art med aktiviteter",
            subtitle=f"Alle {_species_stats.height} arter sortert etter rødlistekategori",
        )
        .fmt_number(
            columns=[
                "Observasjoner",
                "Individer",
                "Reproduksjon",
                "Mulig reproduksjon",
            ],
            decimals=0,
            use_seps=True,
        )
        .fmt_number(columns=["Gj.snitt individer"], decimals=1)
        .tab_spanner(
            label="Aktiviteter", columns=["Reproduksjon", "Mulig reproduksjon"]
        )
        .tab_options(
            table_font_size="12px",
            heading_title_font_size="16px",
            column_labels_font_size="13px",
        )
    )

    species_table
    return (species_stats_formatted,)


@app.cell(hide_code=True)
def _(mo):
    # Create a run button for clipboard export
    clipboard_button = mo.ui.run_button(label="Copy Table to Clipboard")
    clipboard_button
    return (clipboard_button,)


@app.cell(hide_code=True)
def _(clipboard_button, mo, species_stats_formatted):
    # Copy data to clipboard when button is clicked
    mo.stop(
        not clipboard_button.value,
        mo.md(" Click the button above to copy data to clipboard"),
    )

    try:
        # Convert the polars dataframe to pandas and copy to clipboard
        # You can choose which dataset to copy
        species_stats_formatted.to_pandas().to_clipboard(index=False)

        row_count = species_stats_formatted.height
        col_count = species_stats_formatted.width

        mo.md(f"""
         **Data copied to clipboard!**
        - **Rows:** {row_count}
        - **Columns:** {col_count}
        - You can now paste it into Excel, Google Sheets, or any other application
        """)
    except Exception as e:
        mo.md(f"❌ **Failed to copy:** {str(e)}")
    return


@app.cell(hide_code=True)
def _(artsdata_df, mo, pl, px):
    # Prepare hierarchical data for sunburst
    _selected_data = artsdata_df.value

    # Create hierarchical structure with proper parent relationships
    _sunburst_data = []

    # Add Orders
    _orden_stats = _selected_data.group_by("Orden").agg(
        [
            pl.len().alias("observations"),
            pl.col("Antall").sum().alias("individuals"),
        ]
    )

    for row in _orden_stats.iter_rows(named=True):
        _sunburst_data.append(
            {
                "labels": row["Orden"],
                "parents": "",
                "values": row["individuals"],
                "observations": row["observations"],
                "level": "Orden",
            }
        )

    # Add Families
    _familie_stats = _selected_data.group_by(["Orden", "Familie"]).agg(
        [
            pl.len().alias("observations"),
            pl.col("Antall").sum().alias("individuals"),
        ]
    )

    for row in _familie_stats.iter_rows(named=True):
        _sunburst_data.append(
            {
                "labels": row["Familie"],
                "parents": row["Orden"],
                "values": row["individuals"],
                "observations": row["observations"],
                "level": "Familie",
            }
        )

    # Add Species
    _species_stats = _selected_data.group_by(["Familie", "Navn"]).agg(
        [
            pl.len().alias("observations"),
            pl.col("Antall").sum().alias("individuals"),
        ]
    )

    for row in _species_stats.iter_rows(named=True):
        _sunburst_data.append(
            {
                "labels": row["Navn"],
                "parents": row["Familie"],
                "values": row["individuals"],
                "observations": row["observations"],
                "level": "Art",
            }
        )

    _sunburst_df = pl.DataFrame(_sunburst_data).to_pandas()

    # Create sunburst chart
    fig_sunburst = px.sunburst(
        _sunburst_df,
        names="labels",
        parents="parents",
        values="values",
        color="level",
        color_discrete_map={
            "Orden": "#1f77b4",
            "Familie": "#ff7f0e",
            "Art": "#2ca02c",
        },
        hover_data={"observations": True, "values": True},
        title="Taksonomisk hierarki - Sunburst",
    )

    fig_sunburst.update_traces(
        textinfo="label+value",
        hovertemplate="<b>%{label}</b><br>Individer: %{value}<br>Observasjoner: %{customdata[0]}<extra></extra>",
    )

    fig_sunburst.update_layout(height=1000, width=1200)

    mo.ui.plotly(fig_sunburst)
    return


@app.cell(column=3, hide_code=True)
def _(mo):
    mo.md(r"""
    ### Tid
    """)
    return


@app.cell(hide_code=True)
def _(artsdata_df):
    artsdata_tid = artsdata_df.value
    return (artsdata_tid,)


@app.cell
def _(mo):
    mo.md(r"""
    #### Arter pr. år (mangler std.error, som for fig under)
    """)
    return


@app.cell(hide_code=True)
def _(alt, artsdata_tid, mo, pl):
    # Group by year and sum the number of individuals
    individuals_by_year = (
        artsdata_tid.group_by(pl.col("Observert dato").dt.year().alias("year"))
        .agg(
            [
                pl.len().alias("observation_count"),  # Using pl.len() as requested
                pl.col("Antall")
                .sum()
                .alias("individual_count"),  # Sum of individuals
            ]
        )
        .sort("year")
    )

    # Create the Altair chart for individuals per year
    chart_tid = (
        alt.Chart(individuals_by_year)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:O", title="År"),
            y=alt.Y("individual_count:Q", title="Antall individer"),
            tooltip=[
                alt.Tooltip("year:O", title="År"),
                alt.Tooltip("individual_count:Q", title="Antall individer"),
                alt.Tooltip("observation_count:Q", title="Antall observasjoner"),
            ],
        )
        .properties(
            width=900, height=400, title="Antall individer observert per år"
        )
        .interactive()
    )

    mo.ui.altair_chart(chart_tid)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Gj.snitt obs/individer pr. måned for hele datasettet
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    toggle = mo.ui.switch(label="Individer", value=False)
    window_size = mo.ui.slider(
        start=1,
        stop=30,
        step=1,
        show_value=True,
        label="Antall dager i rullende gj.snitt",
    )
    window_size
    return toggle, window_size


@app.cell(hide_code=True)
def _(alt, artsdata_tid, mo, pl, toggle, window_size):
    daily_stats = (
        artsdata_tid.with_columns(
            [
                pl.col("Observert dato").dt.date().alias("date"),
                pl.col("Observert dato").dt.year().alias("year"),
                pl.col("Antall")
                .cast(pl.Int64, strict=False)
                .fill_null(1)
                .alias("ind_count"),
            ]
        )
        .group_by("date")
        .agg(
            [
                pl.len().alias("daily_obs_count"),
                pl.col("ind_count").sum().alias("daily_ind_count"),
                pl.col("year").first().alias("year"),
            ]
        )
        # Create a synthetic date using year 2024 for all data to show pattern
        .with_columns(
            [
                pl.date(
                    2024, pl.col("date").dt.month(), pl.col("date").dt.day()
                ).alias("common_date")
            ]
        )
        .group_by("common_date")
        .agg(
            [
                # For observations
                pl.col("daily_obs_count").mean().alias("avg_daily_obs"),
                pl.col("daily_obs_count").std().alias("std_daily_obs"),
                # For individuals
                pl.col("daily_ind_count").mean().alias("avg_daily_ind"),
                pl.col("daily_ind_count").std().alias("std_daily_ind"),
                # Count years
                pl.col("daily_obs_count").count().alias("n_years"),
            ]
        )
        .sort("common_date")
        .with_columns(
            [
                # Rolling averages
                pl.col("avg_daily_obs")
                .rolling_mean(window_size.value, center=True)
                .alias("rolling_avg_obs"),
                pl.col("avg_daily_ind")
                .rolling_mean(window_size.value, center=True)
                .alias("rolling_avg_ind"),
                # Standard errors
                (pl.col("std_daily_obs") / pl.col("n_years").sqrt()).alias(
                    "se_obs"
                ),
                (pl.col("std_daily_ind") / pl.col("n_years").sqrt()).alias(
                    "se_ind"
                ),
            ]
        )
        .with_columns(
            [
                # Confidence bands
                (pl.col("rolling_avg_obs") - pl.col("se_obs")).alias("lower_obs"),
                (pl.col("rolling_avg_obs") + pl.col("se_obs")).alias("upper_obs"),
                (pl.col("rolling_avg_ind") - pl.col("se_ind")).alias("lower_ind"),
                (pl.col("rolling_avg_ind") + pl.col("se_ind")).alias("upper_ind"),
            ]
        )
    )

    # Create observations chart
    obs_chart = (
        (
            alt.Chart(daily_stats)
            .mark_area(opacity=0.3, color="lightblue")
            .encode(
                x=alt.X(
                    "common_date:T", title="Dato", axis=alt.Axis(format="%d %b")
                ),
                y=alt.Y(
                    "lower_obs:Q", title="Rullerende gjennomsnitt (observasjoner)"
                ),
                y2="upper_obs:Q",
            )
            + alt.Chart(daily_stats)
            .mark_line(point=True, size=2, color="steelblue")
            .encode(
                x="common_date:T",
                y="rolling_avg_obs:Q",
                tooltip=[
                    alt.Tooltip("common_date:T", title="Dato", format="%d %B"),
                    alt.Tooltip(
                        "rolling_avg_obs:Q",
                        title="Rullerende gjennomsnitt",
                        format=".1f",
                    ),
                    alt.Tooltip("se_obs:Q", title="Standardfeil", format=".2f"),
                ],
            )
        )
        .properties(width=900, height=400, title="Observasjoner")
        .interactive()
    )

    # Create individuals chart
    ind_chart = (
        (
            alt.Chart(daily_stats)
            .mark_area(opacity=0.3, color="peachpuff")
            .encode(
                x=alt.X(
                    "common_date:T", title="Dato", axis=alt.Axis(format="%d %b")
                ),
                y=alt.Y(
                    "lower_ind:Q", title="Rullerende gjennomsnitt (individer)"
                ),
                y2="upper_ind:Q",
            )
            + alt.Chart(daily_stats)
            .mark_line(
                point={"filled": True, "fill": "darkorange", "size": 20},
                size=2,
                color="darkorange",
            )
            .encode(
                x="common_date:T",
                y="rolling_avg_ind:Q",
                tooltip=[
                    alt.Tooltip("common_date:T", title="Dato", format="%d %B"),
                    alt.Tooltip(
                        "rolling_avg_ind:Q",
                        title="Rullerende gjennomsnitt",
                        format=".1f",
                    ),
                    alt.Tooltip("se_ind:Q", title="Standardfeil", format=".2f"),
                ],
            )
        )
        .properties(width=900, height=800, title="Individer")
        .interactive()
    )

    # Display toggle and appropriate chart
    mo.vstack([toggle, ind_chart if toggle.value else obs_chart])
    return


@app.cell(column=4, hide_code=True)
def _(mo):
    mo.md(r"""
    ### Figurer
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    # Create a dropdown to select between the two dataframes
    dataframe_selector = mo.ui.dropdown(
        options=["Alle arter", "Kun arter i valgte økosystemtyper"],
        value="Alle arter",  # Default to all species
        label="Velg datasett:",
    )

    # Display the selector
    dataframe_selector
    return (dataframe_selector,)


@app.cell(hide_code=True)
def _(artsdata_df, dataframe_selector, mo, okosystem_arter_df):
    # Add this at the beginning of the cell that uses okosystem_arter_df
    mo.stop(
        "okosystem_arter_df" not in globals()
        and dataframe_selector.value != "Alle arter",
        mo.md(
            "⚠️ **Run the ecosystem overlay analysis first to use filtered data**"
        ),
    )

    # Your existing logic continues here
    if dataframe_selector.value == "Alle arter":
        artsdata_fg = artsdata_df.value
    else:
        artsdata_fg = okosystem_arter_df.value
    return (artsdata_fg,)


@app.cell(hide_code=True)
def _(mo):
    # Cell 1: Create dropdowns (unchanged)
    metric_dropdown = mo.ui.dropdown(
        options=[
            "Antall individer",
            "Antall observasjoner",
            "Gjennomsnittelig antall individer pr. observasjon",
        ],
        value="Antall individer",
        label="Velg metrikk",
    )

    grouping_dropdown = mo.ui.dropdown(
        options=["Art (kategori)", "Familie", "Orden"],
        value="Art (kategori)",
        label="Sorter etter",
    )

    mo.vstack([metric_dropdown, grouping_dropdown])
    return grouping_dropdown, metric_dropdown


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ####Arter
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    # Create a checkbox for toggling markers
    show_markers = mo.ui.checkbox(label="Vis forvaltningsinteresse", value=True)
    show_markers
    return (show_markers,)


@app.cell(hide_code=True)
def _(artsdata_fg, metric_dropdown, pl):
    if metric_dropdown.value == "Antall individer":
        aggregated_data = artsdata_fg.group_by("Navn").agg(
            pl.col("Antall").sum().alias("Total")
        )
        y_label = "Antall individer"
    elif metric_dropdown.value == "Antall observasjoner":
        aggregated_data = artsdata_fg.group_by("Navn").agg(pl.len().alias("Total"))
        y_label = "Antall observasjoner"
    else:
        aggregated_data = artsdata_fg.group_by("Navn").agg(
            pl.col("Antall").mean().alias("Total")
        )
        y_label = "Gjennomsnitt individer per observasjon"

    # Join with species information - INCLUDING THE SPECIAL CATEGORIES
    species_info = artsdata_fg.select(
        [
            "Navn",
            "Kategori",
            "Familie",
            "Orden",
            "Ansvarsarter",
            "Andre spesielt hensynskrevende arter",
            "Prioriterte arter",
        ]
    ).unique()

    data_with_info = aggregated_data.join(species_info, on="Navn")
    return data_with_info, y_label


@app.cell(hide_code=True)
def _(data_with_info, grouping_dropdown, pl):
    # Cell 3: Sort data and calculate group statistics
    # Define sorting field based on dropdown
    if grouping_dropdown.value == "Art (kategori)":
        sort_field = "Kategori"
        color_field = "Kategori"
        color_title = "Rødlistekategori"

        # Define explicit sort order for all possible categories
        # Norwegian Red List categories (IUCN)
        redlist_order = ["CR", "EN", "VU", "NT", "LC", "DD", "NR"]

        # Alien species risk categories (Fremmede arter)
        alien_order = ["SE", "HI", "PH", "LO", "NK"]

        # Other categories
        other_order = ["NA", "Unknown"]

        # Combined order: Red list first (most to least threatened), then alien species (highest to lowest risk), then others
        kategori_order = redlist_order + alien_order + other_order

        # Create a mapping for sort priority
        kategori_priority = {cat: i for i, cat in enumerate(kategori_order)}

        # Add sort priority column
        data_with_priority = data_with_info.with_columns(
            pl.col("Kategori")
            .map_elements(
                lambda x: kategori_priority.get(x, 999), return_dtype=pl.Int32
            )
            .alias("kategori_priority")
        )

        # Sort by category priority first, then by Total within each group
        sorted_data = data_with_priority.sort(
            ["kategori_priority", "Total"], descending=[False, True]
        )

        # Remove the temporary priority column
        sorted_data = sorted_data.drop("kategori_priority")

    elif grouping_dropdown.value == "Familie":
        sort_field = "Familie"
        color_field = "Familie"
        color_title = "Familie"
        # Sort alphabetically by Familie, then by Total within each group
        sorted_data = data_with_info.sort(
            [sort_field, "Total"], descending=[False, True]
        )

    else:
        sort_field = "Orden"
        color_field = "Orden"
        color_title = "Orden"
        # Sort alphabetically by Orden, then by Total within each group
        sorted_data = data_with_info.sort(
            [sort_field, "Total"], descending=[False, True]
        )

    # Calculate group totals for annotations
    group_totals = sorted_data.group_by(sort_field).agg(
        [
            pl.col("Total").sum().alias("GroupTotal"),
            pl.col("Navn").count().alias("SpeciesCount"),
            pl.col("Navn")
            .first()
            .alias("FirstSpecies"),  # To position the annotation
            pl.col("Navn").last().alias("LastSpecies"),
        ]
    )

    # Add x-position for each species (for separator lines)
    sorted_data_with_pos = sorted_data.with_columns(
        pl.arange(0, sorted_data.height).alias("x_position")
    )

    # Find group boundaries for separator lines
    group_boundaries = (
        sorted_data_with_pos.group_by(sort_field)
        .agg(pl.col("x_position").max().alias("last_position"))
        .filter(
            pl.col("last_position") < sorted_data_with_pos.height - 1
        )  # Exclude last group
        .with_columns((pl.col("last_position") + 0.5).alias("separator_position"))
    )

    # Create species order for x-axis
    species_order = sorted_data["Navn"].to_list()

    # Get unique values for consistent color ordering
    if grouping_dropdown.value == "Art (kategori)":
        # Use the explicit order for categories
        unique_groups = [
            cat
            for cat in kategori_order
            if cat in sorted_data[sort_field].unique()
        ]
    else:
        # Use alphabetical order for other groupings
        unique_groups = sorted_data[sort_field].unique().sort().to_list()
    return (
        color_field,
        color_title,
        kategori_order,
        sort_field,
        sorted_data,
        species_order,
        unique_groups,
    )


@app.cell(hide_code=True)
def _(
    alt,
    color_field,
    color_title,
    grouping_dropdown,
    kategori_order,
    sorted_data,
    unique_groups,
):
    # Cell 4: Define color schemes
    # Define color schemes based on grouping type
    if grouping_dropdown.value == "Art (kategori)":
        # Combined color scheme for both Red List and alien species
        kategori_colors = {
            # Red List categories (threat-based colors)
            "CR": "#d62728",  # Critically Endangered - Dark Red
            "EN": "#ff7f0e",  # Endangered - Orange
            "VU": "#ffbb78",  # Vulnerable - Light Orange
            "NT": "#aec7e8",  # Near Threatened - Light Blue
            "LC": "#2ca02c",  # Least Concern - Green
            "DD": "#c7c7c7",  # Data Deficient - Gray
            "NR": "#f7f7f7",  # Not Evaluated - Light Gray
            # Alien species categories (risk-based colors)
            "SE": "#8b0000",  # Severe impact - Dark Red
            "HI": "#ff1493",  # High impact - Deep Pink
            "PH": "#ff69b4",  # Potentially high impact - Hot Pink
            "LO": "#dda0dd",  # Low impact - Plum
            "NK": "#e6e6fa",  # No known impact - Lavender
            # Other categories
            "NA": "#b0b0b0",  # Not Applicable - Medium Gray
            "Unknown": "#888888",  # Unknown - Dark Gray
        }

        # Use kategori_order from Cell 3 to ensure consistent ordering
        # kategori_order is already defined in Cell 3

        # Get actual categories in the data, maintaining the defined order
        actual_categories = sorted_data["Kategori"].unique().to_list()
        color_domain = [cat for cat in kategori_order if cat in actual_categories]
        color_range = [kategori_colors[cat] for cat in color_domain]

        color_scale = alt.Scale(domain=color_domain, range=color_range)
        legend_sort = color_domain  # Explicit sort order for legend
    else:
        # Use default color scheme for Familie and Orden
        color_scale = alt.Scale(
            scheme="category20" if len(unique_groups) > 10 else "category10"
        )
        legend_sort = unique_groups  # Alphabetical order from Cell 3

    # Create color encoding with explicit sort order
    color_encoding = alt.Color(
        color_field,
        title=color_title,
        scale=color_scale,
        sort=legend_sort,  # Use explicit sort order for legend
        legend=alt.Legend(orient="right", titleLimit=200),
    )
    return (color_encoding,)


@app.cell(hide_code=True)
def _(
    alt,
    color_encoding,
    metric_dropdown,
    mo,
    pl,
    show_markers,
    sort_field,
    sorted_data,
    species_order,
    y_label,
):
    # --- 1. Initial Setup (similar to your original code) ---

    # Calculate dynamic bar width based on number of species
    num_species = sorted_data.height
    bar_width = max(0.5, min(0.9, 30 / num_species))

    # Calculate a base marker offset (e.g., 5% of the max value)
    max_value = sorted_data["Total"].max()
    marker_offset = max_value * 0.05 if max_value > 0 else 1

    # Base chart with bars
    bars = (
        alt.Chart(sorted_data)
        .mark_bar(width=alt.RelativeBandSize(bar_width))
        .encode(
            x=alt.X(
                "Navn",
                title="Art",
                sort=species_order,
                axis=alt.Axis(labelAngle=-45, labelLimit=200, labelOverlap=False),
            ),
            y=alt.Y(
                "Total",
                title=y_label,
                scale=alt.Scale(domain=[0, max_value * 1.2]),
            ),
            color=color_encoding,
            tooltip=[
                alt.Tooltip("Navn", title="Art"),
                alt.Tooltip(
                    "Total",
                    title=y_label,
                    format=".2f" if "Gjennomsnitt" in y_label else ".0f",
                ),
                alt.Tooltip("Kategori", title="Rødlistestatus"),
                alt.Tooltip("Familie", title="Familie"),
                alt.Tooltip("Orden", title="Orden"),
                alt.Tooltip("Ansvarsarter", title="Ansvarsart"),
                alt.Tooltip(
                    "Andre spesielt hensynskrevende arter", title="Hensynskrevende"
                ),
                alt.Tooltip("Prioriterte arter", title="Prioritert"),
            ],
        )
    )

    # --- 2. Conditionally create and add markers based on checkbox ---
    if show_markers.value:
        # Data Transformation for Markers
        marker_cols = [
            "Ansvarsarter",
            "Andre spesielt hensynskrevende arter",
            "Prioriterte arter",
        ]

        marker_data = (
            sorted_data.filter(pl.any_horizontal(pl.col(c) for c in marker_cols))
            .unpivot(
                index=["Navn", "Total"],
                on=marker_cols,
                variable_name="Status",
                value_name="Is_True",
            )
            .filter(pl.col("Is_True"))
        )

        # Create the Improved Marker Layer
        if marker_data.height > 0:
            markers = (
                alt.Chart(marker_data)
                .mark_point(
                    size=50,
                    filled=False,
                    stroke="black",
                    strokeWidth=0.5,
                )
                .encode(
                    x=alt.X("Navn:N", sort=species_order),
                    y=alt.Y("y_pos:Q"),
                    shape=alt.Shape(
                        "Status:N",
                        scale=alt.Scale(
                            domain=marker_cols,
                            range=["circle", "square", "triangle-up"],
                        ),
                        legend=alt.Legend(title="Forvaltningsinteresse"),
                    ),
                    tooltip=[
                        alt.Tooltip("Navn", title="Art"),
                        alt.Tooltip("Status", title="Status"),
                    ],
                )
                .transform_window(
                    marker_rank="rank()",
                    groupby=["Navn"],
                )
                .transform_calculate(
                    y_pos=f"datum.Total + {marker_offset} * datum.marker_rank"
                )
            )

            # Layer the charts with shared Y-scale
            chart = alt.layer(bars, markers).resolve_scale(y="shared")
        else:
            chart = bars
    else:
        # If checkbox is unchecked, only show bars
        chart = bars

    # --- 3. Final Chart Configuration ---
    final_chart = (
        chart.properties(
            width=1600,
            height=500,
            title=f"{metric_dropdown.value} sortert etter {sort_field.lower()}",
        )
        .configure_axis(labelFontSize=11, titleFontSize=12)
        .configure_title(fontSize=16, anchor="start")
        .configure_legend(
            titleFontSize=12,
            labelFontSize=11,
            orient="right",
            symbolFillColor="transparent",
            symbolStrokeColor="black",
            symbolStrokeWidth=0.1,
        )
    )

    interactive_chart = mo.ui.altair_chart(final_chart)
    interactive_chart
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Atferd
    """)
    return


@app.cell
def _(alt, artsdata_fg, mo):
    atferd_figur = mo.ui.altair_chart(
        alt.Chart(artsdata_fg)
        .mark_bar()
        .encode(
            x="Navn",
            y="Antall",
            color="Atferd",
            tooltip=["Navn", "Antall", "Atferd"],
        )
        .properties(width=1500, height=400)
    )

    atferd_figur
    return


if __name__ == "__main__":
    app.run()
