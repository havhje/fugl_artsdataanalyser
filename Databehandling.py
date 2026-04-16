import marimo

__generated_with = "0.23.0"
app = marimo.App(width="columns")

with app.setup:
    import time
    from datetime import date
    from functools import lru_cache
    from typing import Any
    import marimo as mo
    import polars as pl
    import requests
    import duckdb
    import pytest
    from unittest.mock import MagicMock, patch
    from datetime import datetime, date as dt_date


@app.cell
def _():
    DATABASE_URL = "fugl_atributt_data"
    bird_data = duckdb.connect(DATABASE_URL, read_only=False)
    return (bird_data,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Utility functions
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Rydder opp i navn og datatyper
    """)
    return


@app.function
def rydd_navn_og_datatyper(df_input: pl.DataFrame) -> pl.DataFrame: 

    df_alle_funksjoner_ferdig_kjørt = df_input.select(
    [
        pl.col("category").alias("Kategori"),
        pl.col("Art av nasjonal forvaltningsinteresse"),
        pl.col("Verdi M1941"),
        pl.col("preferredPopularName").alias("Navn"),
        pl.col("validScientificName").alias("Art"),
        pl.col("individualCount")
        .fill_null("1") #antar at alle obs = minimum 1 når observatøren ikke har lagt inn spesifitk antall
        .str.split("/")  # Noen har en 1/1 antall - aner ikke hva det betyr
        .list.first()  # Take the first number
        .cast(pl.Float64)  # Noen har komma, så må ta til float først
        .cast(pl.Int64)
        .alias("Antall"),
        pl.col("behavior").alias("Atferd"),
        pl.col("dateTimeCollected").dt.date().alias("Observert dato"),
        pl.col("coordinateUncertaintyInMeters").alias("Usikkerhet meter").cast(pl.Int64),
        pl.col("FamilieNavn").alias("Familie"),
        pl.col("OrdenNavn").alias("Orden"),
        pl.col("taxonGroupName").alias("Artsgruppe"),
        pl.col("collector").alias("Observatør"),
        pl.col("locality").alias("Lokalitet"),
        pl.col("municipality").alias("Kommune"),
        pl.col("county").alias("Fylke"),
        pl.col("scientificNameRank").alias("Taksonomisk nivå"),
        pl.col("Ansvarsarter"),
        pl.col("Andre spesielt hensynskrevende arter"),
        pl.col("Spesielle okologiske former").alias("Spesielle økologiske former"),
        pl.col("Prioriterte arter"),
        pl.col("Fredete arter"),
        pl.col("Fremmede arter"),
        pl.col("latitude").str.replace_all(",", ".").cast(pl.Float64),
        pl.col("longitude").str.replace_all(",", ".").cast(pl.Float64),
        pl.col("geometry"),
        pl.col("validScientificNameId").alias("Artens ID"),
    ]
    )


    return (df_alle_funksjoner_ferdig_kjørt)


@app.function
def test_rydd_navn_og_datatyper():

    # ── Build test input with all required columns ──────────────────
    test_df = pl.DataFrame(
        {
            "category": ["LC", "NT", "EN"],
            "Art av nasjonal forvaltningsinteresse": ["Yes", "No", "Yes"],
            "Verdi M1941": ["C", "D", "B"],
            "preferredPopularName": ["dompap", "kråke", "tjeld"],
            "validScientificName": [
                "Pyrrhula pyrrhula",
                "Corvus cornix",
                "Haematopus ostralegus",
            ],
            "individualCount": ["6", None, "3/1"],
            "behavior": ["singing", None, "flying"],
            "dateTimeCollected": [
                datetime(2022, 5, 15, 10, 30, 0),
                datetime(2023, 7, 4, 14, 0, 0),
                datetime(2019, 6, 1, 7, 4, 19),
            ],
            "coordinateUncertaintyInMeters": [300, None, 9],
            "FamilieNavn": ["Fringillidae", "Corvidae", "Haematopodidae"],
            "OrdenNavn": ["Passeriformes", "Passeriformes", "Charadriiformes"],
            "taxonGroupName": ["Fugler", "Fugler", "Fugler"],
            "collector": ["Ola Nordmann", "Kari Nordmann", None],
            "locality": ["Sommarøyveien 21", "Strengelvågfjorden", None],
            "municipality": ["Øksnes", "Øksnes", "Øksnes"],
            "county": ["Nordland", "Nordland", "Nordland"],
            "scientificNameRank": ["species", "species", "species"],
            "Ansvarsarter": ["No", "No", "Yes"],
            "Andre spesielt hensynskrevende arter": ["No", "Yes", "No"],
            "Spesielle okologiske former": ["No", "No", "Yes"],
            "Prioriterte arter": ["No", "No", "Yes"],
            "Fredete arter": ["No", "No", "No"],
            "Fremmede arter": ["No", "No", "No"],
            "latitude": ["68,904168", "68,962388", "68.974144"],
            "longitude": ["15,066918", "15,148183", "14.947151"],
            "geometry": [
                "POINT (502688 7643678)",
                "POINT (505937 7650175)",
                "POINT (497884 7651480)",
            ],
            "validScientificNameId": [4263, 4164, 3664],
        }
    )

    test_result = rydd_navn_og_datatyper(test_df)



    # ── Antall rader skal være uendret ──────────────────────────────
    assert test_result.height == 3, f"Forventet 3 rader, fikk {test_result.height}"

    # ── Alle forventede kolonner skal finnes ────────────────────────
    expected_cols = [
        "Kategori",
        "Art av nasjonal forvaltningsinteresse",
        "Verdi M1941",
        "Navn",
        "Art",
        "Antall",
        "Atferd",
        "Observert dato",
        "Usikkerhet meter",
        "Familie",
        "Orden",
        "Artsgruppe",
        "Observatør",
        "Lokalitet",
        "Kommune",
        "Fylke",
        "Taksonomisk nivå",
        "Ansvarsarter",
        "Andre spesielt hensynskrevende arter",
        "Spesielle økologiske former",
        "Prioriterte arter",
        "Fredete arter",
        "Fremmede arter",
        "latitude",
        "longitude",
        "geometry",
        "Artens ID",
    ]

    for col in expected_cols:
        assert col in test_result.columns, f"Kolonne '{col}' mangler i resultatet"

    assert len(test_result.columns) == len(expected_cols), (
        f"Forventet {len(expected_cols)} kolonner, fikk {len(test_result.columns)}: {test_result.columns}"
    )

    # ── Test kolonneomnavning (renaming) ────────────────────────────
    # category → Kategori
    assert test_result.get_column("Kategori").to_list() == ["LC", "NT", "EN"], (
        "category skal omdøpes til Kategori med riktige verdier"
    )

    # preferredPopularName → Navn
    assert test_result.get_column("Navn").to_list() == ["dompap", "kråke", "tjeld"], (
        "preferredPopularName skal omdøpes til Navn"
    )

    # validScientificName → Art
    assert test_result.get_column("Art").to_list() == [
        "Pyrrhula pyrrhula",
        "Corvus cornix",
        "Haematopus ostralegus",
    ], "validScientificName skal omdøpes til Art"

    # validScientificNameId → Artens ID
    assert test_result.get_column("Artens ID").to_list() == [4263, 4164, 3664], (
        "validScientificNameId skal omdøpes til Artens ID"
    )

    # FamilieNavn → Familie
    assert test_result.get_column("Familie").to_list() == [
        "Fringillidae",
        "Corvidae",
        "Haematopodidae",
    ], "FamilieNavn skal omdøpes til Familie"

    # OrdenNavn → Orden
    assert test_result.get_column("Orden").to_list() == [
        "Passeriformes",
        "Passeriformes",
        "Charadriiformes",
    ], "OrdenNavn skal omdøpes til Orden"

    # Spesielle okologiske former → Spesielle økologiske former (ø)
    assert "Spesielle økologiske former" in test_result.columns, (
        "'Spesielle okologiske former' skal omdøpes til 'Spesielle økologiske former' med ø"
    )

    # ── Test individualCount-transformasjon → Antall ────────────────
    antall = test_result.get_column("Antall")
    assert antall.dtype == pl.Int64, f"Antall skal være Int64, fikk {antall.dtype}"

    # "6" → 6
    assert antall[0] == 6, f"individualCount '6' skal bli 6, fikk {antall[0]}"

    # None → 1 (fill_null med "1")
    assert antall[1] == 1, f"individualCount null skal bli 1, fikk {antall[1]}"

    # "3/1" → 3 (split på '/' og ta første)
    assert antall[2] == 3, f"individualCount '3/1' skal bli 3, fikk {antall[2]}"

    # ── Test dateTimeCollected → Observert dato (date) ──────────────
    obs_dato = test_result.get_column("Observert dato")
    assert obs_dato.dtype == pl.Date, f"Observert dato skal være Date, fikk {obs_dato.dtype}"
    assert obs_dato[0] == dt_date(2022, 5, 15), f"Dato for rad 0 skal være 2022-05-15, fikk {obs_dato[0]}"
    assert obs_dato[1] == dt_date(2023, 7, 4), f"Dato for rad 1 skal være 2023-07-04, fikk {obs_dato[1]}"
    assert obs_dato[2] == dt_date(2019, 6, 1), f"Dato for rad 2 skal være 2019-06-01, fikk {obs_dato[2]}"

    # ── Test coordinateUncertaintyInMeters → Usikkerhet meter ───────
    usikkerhet = test_result.get_column("Usikkerhet meter")
    assert usikkerhet.dtype == pl.Int64, f"Usikkerhet meter skal være Int64, fikk {usikkerhet.dtype}"
    assert usikkerhet[0] == 300, f"Usikkerhet for rad 0 skal være 300, fikk {usikkerhet[0]}"
    assert usikkerhet[2] == 9, f"Usikkerhet for rad 2 skal være 9, fikk {usikkerhet[2]}"

    # ── Test latitude komma → punktum → Float64 ────────────────────
    lat = test_result.get_column("latitude")
    assert lat.dtype == pl.Float64, f"latitude skal være Float64, fikk {lat.dtype}"
    assert abs(lat[0] - 68.904168) < 1e-5, f"latitude '68,904168' skal bli 68.904168, fikk {lat[0]}"
    # Latitude med punktum skal også fungere
    assert abs(lat[2] - 68.974144) < 1e-5, f"latitude '68.974144' (allerede punktum) skal bli 68.974144, fikk {lat[2]}"

    # ── Test longitude komma → punktum → Float64 ───────────────────
    lon = test_result.get_column("longitude")
    assert lon.dtype == pl.Float64, f"longitude skal være Float64, fikk {lon.dtype}"
    assert abs(lon[0] - 15.066918) < 1e-5, f"longitude '15,066918' skal bli 15.066918, fikk {lon[0]}"
    assert abs(lon[1] - 15.148183) < 1e-5, f"longitude '15,148183' skal bli 15.148183, fikk {lon[1]}"

    # ── Test at kolonner som ikke omdøpes beholder verdier ──────────
    assert test_result.get_column("Atferd").to_list() == ["singing", None, "flying"], (
        "behavior skal omdøpes til Atferd med riktige verdier"
    )
    assert test_result.get_column("Artsgruppe").to_list() == [
        "Fugler",
        "Fugler",
        "Fugler",
    ], "taxonGroupName skal omdøpes til Artsgruppe"

    assert test_result.get_column("Kommune").to_list() == [
        "Øksnes",
        "Øksnes",
        "Øksnes",
    ], "municipality skal omdøpes til Kommune"

    assert test_result.get_column("Fylke").to_list() == [
        "Nordland",
        "Nordland",
        "Nordland",
    ], "county skal omdøpes til Fylke"

    # ── Test at passthrough-kolonner ikke endres ────────────────────
    assert test_result.get_column("geometry").to_list() == [
        "POINT (502688 7643678)",
        "POINT (505937 7650175)",
        "POINT (497884 7651480)",
    ], "geometry skal beholde sine verdier uendret"

    assert test_result.get_column("Art av nasjonal forvaltningsinteresse").to_list() == [
        "Yes",
        "No",
        "Yes",
    ], "Art av nasjonal forvaltningsinteresse skal beholde sine verdier"

    assert test_result.get_column("Verdi M1941").to_list() == ["C", "D", "B"], "Verdi M1941 skal beholde sine verdier"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### API fra artsdatabanken
    """)
    return


@app.cell
def _():
    # Constants
    NORTAXA_API_BASE_URL = "https://nortaxa.artsdatabanken.no/api/v1/TaxonName"
    DESIRED_RANKS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus"]
    RATE_LIMIT_DELAY = 0.1  # seconds between API calls (adjust as needed)
    return DESIRED_RANKS, NORTAXA_API_BASE_URL, RATE_LIMIT_DELAY


@app.cell
def _(DESIRED_RANKS, NORTAXA_API_BASE_URL):
    @lru_cache(maxsize=10000)
    def fetch_taxon_data(scientific_name_id: int) -> dict[str, Any] | None:
        """Fetch taxon data with caching to avoid duplicate API calls.

        Queries the NorTaxa API for a given scientific name ID and returns
        the full taxon record.

        Args:
            scientific_name_id: Unique identifier from Artsdatabanken.

        Returns:
            JSON response as a dict, or None if the request fails.
        """
        try:
            response = requests.get(
                f"{NORTAXA_API_BASE_URL}/ByScientificNameId/{scientific_name_id}",
                timeout=10,
            )
            if response.ok:
                return response.json()
        except Exception as e:
            print(f"Error fetching ID {scientific_name_id}: {e}")
        return None


    def extract_hierarchy_and_ids(
        api_data: dict[str, Any] | None,
    ) -> tuple[dict[str, str], int | None, int | None]:
        """Extract taxonomic hierarchy and rank IDs from API data.

        Parses the higherClassification field to build a rank-to-name mapping
        and extracts the scientificNameId for Family and Order ranks.

        Args:
            api_data: Parsed JSON response from the NorTaxa API.

        Returns:
            A tuple of (hierarchy dict, family_id, order_id).
        """
        hierarchy = {}
        family_id = order_id = None

        if api_data and "higherClassification" in api_data:
            for level in api_data["higherClassification"]:
                rank = level.get("taxonRank")
                if rank in DESIRED_RANKS:
                    hierarchy[rank] = level.get("scientificName")
                if rank == "Family":
                    family_id = level.get("scientificNameId")
                elif rank == "Order":
                    order_id = level.get("scientificNameId")

        return hierarchy, family_id, order_id


    def get_norwegian_name(api_data: dict[str, Any] | None) -> str | None:
        """Extract Norwegian vernacular name (prioritize Bokmål over Nynorsk).

        Looks through vernacularNames in the API response, preferring Bokmål
        (nb) and falling back to Nynorsk (nn).

        Args:
            api_data: Parsed JSON response from the NorTaxa API.

        Returns:
            The Norwegian common name, or None if unavailable.
        """
        if not api_data or "vernacularNames" not in api_data:
            return None

        names = api_data["vernacularNames"]
        # First try Bokmål
        for name in names:
            if name.get("languageIsoCode") == "nb":
                return name.get("vernacularName")
        # Fallback to Nynorsk
        for name in names:
            if name.get("languageIsoCode") == "nn":
                return name.get("vernacularName")
        return None

    return extract_hierarchy_and_ids, fetch_taxon_data, get_norwegian_name


@app.cell
def _(
    DESIRED_RANKS,
    RATE_LIMIT_DELAY,
    extract_hierarchy_and_ids,
    fetch_taxon_data,
    get_norwegian_name,
):
    def process_and_enrich_data(source_df: pl.DataFrame) -> pl.DataFrame | None:
        """Process the dataframe and enrich with taxonomy data.

        Fetches taxonomic hierarchy and Norwegian vernacular names from the
        NorTaxa API for each unique species ID, then joins the results back
        onto the source dataframe.

        Args:
            source_df: Input dataframe containing a ``validScientificNameId`` column.

        Returns:
            Enriched dataframe with taxonomy columns, or None on error.
        """
        # Convert to Polars for better performance
        if isinstance(source_df, pl.DataFrame):
            df_work = source_df.clone()
        else:
            df_work = pl.from_pandas(source_df)

        # Check if required column exists
        if "validScientificNameId" not in df_work.columns:
            mo.md("Error: 'validScientificNameId' column not found in input data.")
            return None

        # Get unique IDs
        unique_ids = df_work.select("validScientificNameId").unique().to_series().to_list()
        total_ids = len(unique_ids)

        # Storage for results
        taxonomy_data = {}
        family_names = {}
        order_names = {}

        # Process with progress bar
        with mo.status.progress_bar(total=total_ids) as bar:
            bar.update(0, title="Fetching taxonomy data from NorTaxa API...")

            for i, species_id in enumerate(unique_ids):
                try:
                    species_id = int(species_id)
                except ValueError, TypeError:
                    bar.update(i + 1)
                    continue

                # Fetch species data
                species_data = fetch_taxon_data(species_id)
                if species_data:
                    hierarchy, family_id, order_id = extract_hierarchy_and_ids(species_data)
                    taxonomy_data[species_id] = hierarchy

                    # Fetch family name if available
                    if family_id:
                        family_data = fetch_taxon_data(family_id)
                        if family_data:
                            family_names[species_id] = get_norwegian_name(family_data)

                    # Fetch order name if available
                    if order_id:
                        order_data = fetch_taxon_data(order_id)
                        if order_data:
                            order_names[species_id] = get_norwegian_name(order_data)

                # Rate limiting
                if RATE_LIMIT_DELAY > 0:
                    time.sleep(RATE_LIMIT_DELAY)

                # Update progress
                bar.update(i + 1, title=f"Processing ID {species_id} ({i + 1}/{total_ids})")

        # Add taxonomy columns with proper return_dtype
        for rank in DESIRED_RANKS:
            df_work = df_work.with_columns(
                pl.col("validScientificNameId")
                .map_elements(
                    lambda x: taxonomy_data.get(int(x), {}).get(rank) if x and x is not None else None,
                    return_dtype=pl.Utf8,  # Fixed: Added return_dtype
                )
                .alias(rank)
            )

        # Add Norwegian names with proper return_dtype
        df_work = df_work.with_columns(
            [
                pl.col("validScientificNameId")
                .map_elements(
                    lambda x: family_names.get(int(x)) if x and x is not None else None,
                    return_dtype=pl.Utf8,  # Fixed: Added return_dtype
                )
                .alias("FamilieNavn"),
                pl.col("validScientificNameId")
                .map_elements(
                    lambda x: order_names.get(int(x)) if x and x is not None else None,
                    return_dtype=pl.Utf8,  # Fixed: Added return_dtype
                )
                .alias("OrdenNavn"),
            ]
        )

        return df_work

    return (process_and_enrich_data,)


@app.cell
def _(process_and_enrich_data):
    def test_process_and_enrich_data():

        # Du må endre df, slik at det abre er species ID som er inputten, er slik funksjonen fungerer
        test_df = pl.DataFrame(
            {
                "validScientificNameId": [
                    4382,  # granmeis
                    204586,  # skjeand
                    3677,  # gråmåke
                    295741,
                ],  # hønsehauk
            }
        )

        test_result = process_and_enrich_data(test_df)

        # Test granmeis (4382)
        granmeis = test_result.filter(pl.col("validScientificNameId") == 4382)
        assert granmeis.get_column("Order").eq("Passeriformes").all(), "Granmeis should be in Passeriformes order"
        assert granmeis.get_column("Family").eq("Paridae").all(), "Granmeis should be in Paridae family"
        assert granmeis.get_column("Genus").eq("Poecile").all(), "Granmeis should be in Poecile genus"
        assert granmeis.get_column("FamilieNavn").eq("meisefamilien").all(), (
            "Granmeis should have FamilieNavn 'meisefamilien'"
        )
        assert granmeis.get_column("OrdenNavn").eq("spurvefugler").all(), "Granmeis should have OrdenNavn 'spurvefugler'"

        # Test skjeand (204586)
        skjeand = test_result.filter(pl.col("validScientificNameId") == 204586)
        assert skjeand.get_column("Order").eq("Anseriformes").all(), "Skjeand should be in Anseriformes order"
        assert skjeand.get_column("Family").eq("Anatidae").all(), "Skjeand should be in Anatidae family"
        assert skjeand.get_column("Genus").eq("Spatula").all(), "Skjeand should be in Spatula genus"
        assert skjeand.get_column("FamilieNavn").eq("andefamilien").all(), "Skjeand should have FamilieNavn 'andefamilien'"
        assert skjeand.get_column("OrdenNavn").eq("andefugler").all(), "Skjeand should have OrdenNavn 'andefugler'"

        # Test gråmåke (3677)
        graamake = test_result.filter(pl.col("validScientificNameId") == 3677)
        assert graamake.get_column("Order").eq("Charadriiformes").all(), "Gråmåke should be in Charadriiformes order"
        assert graamake.get_column("Family").eq("Laridae").all(), "Gråmåke should be in Laridae family"
        assert graamake.get_column("Genus").eq("Larus").all(), "Gråmåke should be in Larus genus"
        assert graamake.get_column("FamilieNavn").eq("måkefamilien").all(), "Gråmåke should have FamilieNavn 'måkefamilien'"
        assert graamake.get_column("OrdenNavn").eq("vade-, måke- og alkefugler").all(), (
            "Gråmåke should have OrdenNavn 'vade-, måke- og alkefugler'"
        )

        # Test hønsehauk (295741)
        honsehauk = test_result.filter(pl.col("validScientificNameId") == 295741)
        assert honsehauk.get_column("Order").eq("Accipitriformes").all(), "Hønsehauk should be in Accipitriformes order"
        assert honsehauk.get_column("Family").eq("Accipitridae").all(), "Hønsehauk should be in Accipitridae family"
        assert honsehauk.get_column("Genus").eq("Astur ").all(), "Hønsehauk should be in Astur genus"
        assert honsehauk.get_column("FamilieNavn").eq("haukefamilien").all(), (
            "Hønsehauk should have FamilieNavn 'haukefamilien'"
        )
        assert honsehauk.get_column("OrdenNavn").eq("haukefugler").all(), "Hønsehauk should have OrdenNavn 'rovfugler'"

    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Arter av nasjonal forvaltningsinteresse
    """)
    return


@app.cell
def _add_national_interest_criteria(bird_data):
    def add_national_interest_criteria(df_enriched: pl.DataFrame) -> pl.DataFrame:
        """Add national interest criteria from Excel file to enriched dataframe.

        Reads criteria columns from the national-interest Excel sheet, converts
        X-marks to Yes/No, and left-joins the result onto the enriched dataframe.

        Args:
            df_enriched: The enriched dataframe with species data.
            excel_path: Path to the Excel file with criteria. If None, uses
                the default path.

        Returns:
            DataFrame with added criteria columns.
        """

        # Load Excel with criteria
        df_arter_nf = bird_data.execute("SELECT * FROM arter_av_nasjonal_forvaltningsinteresse").pl()

        # Get criteria columns
        criteria_cols = [
            "Kriterium_Ansvarsarter",
            "Kriterium_Trua_arter",
            "Kriterium_Andre spesielt_hensynskrevende_arter",
            "Kriterium_Spesielle_okologiske_former",
            "Kriterium_Prioriterte_arter",
            "Kriterium_Fredete_arter",
            "Kriterium_NT",
            "Kriterium_Fremmede_arter",
        ]

        # Process criteria data - convert X marks to Yes/No and remve the Kriterium prefix
        criteria_data = df_arter_nf.select(
            pl.col("ValidScientificNameId").alias(
                "arts_id_mdir"
            ),  # , legger til ett nytt argument slik at ny df består av scientificid OG ...
            pl.col("Vitenskapelig_Navn").alias(
                "vitenskapelig_navn_mdir"
            ),  # _mdir er verdiene fra arter av nasjonal forvaltningsinteresse tabell
            *[
                pl.when(pl.col(c).str.to_uppercase().str.strip_chars() == "X")
                .then(pl.lit("Yes"))
                .otherwise(pl.lit("No"))
                .alias(c.replace("Kriterium_", "").replace("_", " "))
                for c in criteria_cols
            ],
        )
        # *[] er en list comprehension, hvor * sier pakk ut alle "oppskriftene objektene (polars expression = objekter i lista og gjennomfør dette for alle kolonner som matcher de i criteria_cols". Strukturen er hva du skal gjøre først og deretter hvor du skal gjøre det som er hvorfor loopen kommer til slutt.

        criteria_cols_clean = [
            "Ansvarsarter",
            "Trua arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle okologiske former",
            "Prioriterte arter",
            "Fredete arter",
            "NT",
            "Fremmede arter",
        ]

        # Merge with enriched data
        df_with_criteria = (
            df_enriched.join(
                criteria_data,
                left_on="validScientificNameId",
                right_on="arts_id_mdir",
                how="left",
            )
            .join(
                criteria_data,
                left_on=("validScientificName"),
                right_on=("vitenskapelig_navn_mdir"),
                how="left",
                suffix="_fallback",
            )
            # Tar en second join på artsnavn, legger til alle criteria_data en gang til med eget suffix = fallback
            .with_columns(*[pl.coalesce(c, f"{c}_fallback").fill_null("Treff ikke funnet") for c in criteria_cols_clean])
            # bruker coalece som sier velg først join 1 (c), hvis det finnes null verdier bruk verdiene i join 2 (c_fallback)
            .drop(([f"{c}_fallback" for c in criteria_cols_clean]))
            .drop(pl.col("vitenskapelig_navn_mdir"), pl.col("arts_id_mdir"))
            # dropper kollonene som er lagt til 2 ganger
        )

        return df_with_criteria

    return (add_national_interest_criteria,)


@app.cell
def _(add_national_interest_criteria):
    def test_add_national_interest_criteria():

        test_df_anf = pl.DataFrame(
            {
                "validScientificNameId": [
                    3506,  # havelle
                    3768,  # svarthalespove (iclandica)
                    295741,  # hønsehauk
                    3478,  # dverggås
                    3495,  # kanadagås
                    999999,  # finnes ikke i kriterietabellen
                ],
                "validScientificName": [
                    "Clangula hyemalis",  # havelle
                    "Limosa limosa islandica",  # svarthalespove
                    "Accipiter gentilis",  # hønsehauk
                    "Anser erythropus",  # dverggås
                    "Branta canadensis",  # kanadagås
                    "Nonexistent species",  # finnes ikke
                ],
            }
        )

        test_result = add_national_interest_criteria(test_df_anf)

        expected_criteria_cols = [
            "Ansvarsarter",
            "Trua arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle okologiske former",
            "Prioriterte arter",
            "Fredete arter",
            "NT",
            "Fremmede arter",
        ]

        # Alle kriteriekolonner skal finnes i resultatet
        for col in expected_criteria_cols:
            assert col in test_result.columns, f"Kolonne '{col}' mangler i resultatet"

        # Antall rader skal være uendret
        assert test_result.height == 6, f"Forventet 6 rader, fikk {test_result.height}"

        # Test havelle (3506) – nært trua art og andre spesielt hensynskrevende
        havelle = test_result.filter(pl.col("validScientificNameId") == 3506)
        assert havelle.height > 0, "Havelle ikke funnet i resultatet"
        assert havelle.get_column("Ansvarsarter").eq("No").all(), "Havelle er ikke en ansvarsart"
        assert havelle.get_column("Trua arter").eq("No").all(), "Havelle er ikke en trua art"
        assert havelle.get_column("Andre spesielt hensynskrevende arter").eq("Yes").all(), (
            "Havelle er en andre spesielt hensynskrevende art"
        )
        assert havelle.get_column("Spesielle okologiske former").eq("No").all(), (
            "Havelle er ikke en spesiell økologisk form"
        )
        assert havelle.get_column("Prioriterte arter").eq("No").all(), "Havelle er ikke en prioritert art"
        assert havelle.get_column("Fredete arter").eq("No").all(), "Havelle er ikke en fredet art"
        assert havelle.get_column("NT").eq("Yes").all(), "Havelle er en nært trua art"
        assert havelle.get_column("Fremmede arter").eq("No").all(), "Havelle er ikke en fremmed art"

        # Test svarthalespove (3768) – trua art og prioritert art
        svarthalespove = test_result.filter(pl.col("validScientificNameId") == 3768)
        assert svarthalespove.height > 0, "Svarthalespove ikke funnet i resultatet"
        assert svarthalespove.get_column("Ansvarsarter").eq("No").all(), "Svarthalespove er ikke en ansvarsart"
        assert svarthalespove.get_column("Trua arter").eq("Yes").all(), "Svarthalespove er en trua art"
        assert svarthalespove.get_column("Andre spesielt hensynskrevende arter").eq("No").all(), (
            "Svarthalespove er ikke en andre spesielt hensynskrevende art"
        )
        assert svarthalespove.get_column("Spesielle okologiske former").eq("No").all(), (
            "Svarthalespove er ikke en spesiell økologisk form"
        )
        assert svarthalespove.get_column("Prioriterte arter").eq("Yes").all(), "Svarthalespove er en prioritert art"
        assert svarthalespove.get_column("Fredete arter").eq("No").all(), "Svarthalespove er ikke en fredet art"
        assert svarthalespove.get_column("NT").eq("No").all(), "Svarthalespove er ikke en nært trua art"
        assert svarthalespove.get_column("Fremmede arter").eq("No").all(), "Svarthalespove er ikke en fremmed art"

        # Test hønsehauk (295741) – trua art (matchet via vitenskapelig navn-fallback)
        hønsehauk = test_result.filter(pl.col("validScientificNameId") == 295741)
        assert hønsehauk.height > 0, "Hønsehauk ikke funnet i resultatet"
        assert hønsehauk.get_column("Ansvarsarter").eq("No").all(), "Hønsehauk er ikke en ansvarsart"
        assert hønsehauk.get_column("Trua arter").eq("Yes").all(), "Hønsehauk er en trua art"
        assert hønsehauk.get_column("Andre spesielt hensynskrevende arter").eq("No").all(), (
            "Hønsehauk er ikke en andre spesielt hensynskrevende art"
        )
        assert hønsehauk.get_column("Spesielle okologiske former").eq("No").all(), (
            "Hønsehauk er ikke en spesiell økologisk form"
        )
        assert hønsehauk.get_column("Prioriterte arter").eq("No").all(), "Hønsehauk er ikke en prioritert art"
        assert hønsehauk.get_column("Fredete arter").eq("No").all(), "Hønsehauk er ikke en fredet art"
        assert hønsehauk.get_column("NT").eq("No").all(), "Hønsehauk er ikke en nært trua art"
        assert hønsehauk.get_column("Fremmede arter").eq("No").all(), "Hønsehauk er ikke en fremmed art"

        # Test dverggås (3478) – ansvarsart, trua art og prioritert art
        dverggås = test_result.filter(pl.col("validScientificNameId") == 3478)
        assert dverggås.height > 0, "Dverggås ikke funnet i resultatet"
        assert dverggås.get_column("Ansvarsarter").eq("Yes").all(), "Dverggås er en ansvarsart"
        assert dverggås.get_column("Trua arter").eq("Yes").all(), "Dverggås er en trua art"
        assert dverggås.get_column("Andre spesielt hensynskrevende arter").eq("No").all(), (
            "Dverggås er ikke en andre spesielt hensynskrevende art"
        )
        assert dverggås.get_column("Spesielle okologiske former").eq("No").all(), (
            "Dverggås er ikke en spesiell økologisk form"
        )
        assert dverggås.get_column("Prioriterte arter").eq("Yes").all(), "Dverggås er en prioritert art"
        assert dverggås.get_column("Fredete arter").eq("No").all(), "Dverggås er ikke en fredet art"
        assert dverggås.get_column("NT").eq("No").all(), "Dverggås er ikke en nært trua art"
        assert dverggås.get_column("Fremmede arter").eq("No").all(), "Dverggås er ikke en fremmed art"

        # Test kanadagås (3495) – fremmed art
        kanadagås = test_result.filter(pl.col("validScientificNameId") == 3495)
        assert kanadagås.height > 0, "Kanadagås ikke funnet i resultatet"
        assert kanadagås.get_column("Ansvarsarter").eq("No").all(), "Kanadagås er ikke en ansvarsart"
        assert kanadagås.get_column("Trua arter").eq("No").all(), "Kanadagås er ikke en trua art"
        assert kanadagås.get_column("Andre spesielt hensynskrevende arter").eq("No").all(), (
            "Kanadagås er ikke en andre spesielt hensynskrevende art"
        )
        assert kanadagås.get_column("Spesielle okologiske former").eq("No").all(), (
            "Kanadagås er ikke en spesiell økologisk form"
        )
        assert kanadagås.get_column("Prioriterte arter").eq("No").all(), "Kanadagås er ikke en prioritert art"
        assert kanadagås.get_column("Fredete arter").eq("No").all(), "Kanadagås er ikke en fredet art"
        assert kanadagås.get_column("NT").eq("No").all(), "Kanadagås er ikke en nært trua art"
        assert kanadagås.get_column("Fremmede arter").eq("Yes").all(), "Kanadagås er en fremmed art"

        # Test ID som ikke finnes (999999) -> "Treff ikke funnet"
        missing = test_result.filter(pl.col("validScientificNameId") == 999999)
        assert missing.height > 0, "Manglende art (999999) ikke funnet i resultatet"
        for col in expected_criteria_cols:
            assert missing.get_column(col).eq("Treff ikke funnet").all(), (
                f"Kolonne '{col}' skal være 'Treff ikke funnet' for ukjent art"
            )

        # Verdier skal kun være "Yes", "No" eller "Treff ikke funnet"
        valid_values = {"Yes", "No", "Treff ikke funnet"}
        for col in expected_criteria_cols:
            unique_vals = set(test_result.get_column(col).unique().to_list())
            assert unique_vals.issubset(valid_values), (
                f"Kolonne '{col}' inneholder ugyldige verdier: {unique_vals - valid_values}"
            )

    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Lager en ny kolonne som inneholder mulige verdier av arter av nasjonal forvaltning
    """)
    return


@app.function
def legg_til_kolonne_arteravnasjonal(input_df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a new column 'Art av nasjonal forvaltningsinteresse' to the DataFrame.

    The new column is populated based on the values in a predefined list of
    category columns. If any of these columns have the value "Yes", the new
    column will contain a comma-separated list of the category names.
    Otherwise, it will contain the value "Nei".

    Args:
        input_df: The input Polars DataFrame.

    Returns:
        A new Polars DataFrame with the added column.
    """
    category_columns = [
        "Ansvarsarter",
        "Trua arter",
        "Andre spesielt hensynskrevende arter",
        "Spesielle okologiske former",
        "Prioriterte arter",
        "Fredete arter",
        "NT",
        "Fremmede arter",
    ]

    category_list = (
        pl.concat_list(  # slår sammen alle anf til en kolonne, men merk List Concatenation  = packing items into a list within a single row ( noe annet enn a stacke tabbeller)
            *[
                pl.when(pl.col(col) == "Yes").then(pl.lit(col))  # erstatter YES/NO med kolonne navnet
                for col in category_columns
            ]
        ).list.drop_nulls()  # fjerner null verdier slik at du ikke får NT, null, null, Fremmed art
    )

    output_df = input_df.with_columns(
        pl.when(category_list.list.len() > 0)
        .then(
            category_list.list.join(", ")
        )  # You need .list.join(", ") because pl.concat_list() gives you the computer-code format (a list object), and you want the human-readable format (a single text string). Hvor du da joiner tingene i listen med ,
        .otherwise(pl.lit("Nei"))
        .alias("Art av nasjonal forvaltningsinteresse")
    )

    return output_df


@app.function
def test_legg_til_kolonne_arteravnasjonal():
    sample_df = pl.DataFrame(
        {
            "species": ["Hubro", "Gråspurv", "Fjellrev", "Villmink", "Dverggås"],
            "Ansvarsarter": ["Yes", "No", "No", "No", "Yes"],
            "Trua arter": ["No", "No", "No", "No", "No"],
            "Andre spesielt hensynskrevende arter": ["No", "No", "Yes", "No", "No"],
            "Spesielle okologiske former": ["No", "No", "No", "No", "Yes"],
            "Prioriterte arter": ["Yes", "No", "No", "No", "No"],
            "Fredete arter": ["Yes", "No", "No", "No", "No"],
            "NT": ["No", "No", "No", "No", "No"],
            "Fremmede arter": ["No", "No", "No", "Yes", "No"],
        }
    )

    result = legg_til_kolonne_arteravnasjonal(sample_df)

    # Column should exist. Henter alle kolonne navnene med .columns til en liste du tester mot (i.e. in "the list")
    assert "Art av nasjonal forvaltningsinteresse" in result.columns

    # Tar verdiene i kolonnen "art av nasjonal forvaltningsinteresse" og gjør verdiene for hver rad i kolonnen om til en liste med verdiene og en index som korrensponderer til raden/arten
    values = result.get_column("Art av nasjonal forvaltningsinteresse").to_list()

    # Unpacker (en egen python greie (må ha samme "lengde") lista inn i artene (som er de fra df), slik at disse får sine korrensponderende verdier fra ANF kolonnen i orginal df
    hubro, graspurv, fjellrev, villmink, dverggas = values

    # Kan da skrive de riktige "assertene" i testen
    # Hubro: Ansvarsarter, Prioriterte arter, Fredete arter
    assert "Ansvarsarter" in hubro
    assert "Prioriterte arter" in hubro
    assert "Fredete arter" in hubro

    # Gråspurv: none → "Nei"
    assert graspurv == "Nei"

    # Fjellrev: Andre spesielt hensynskrevende arter
    assert fjellrev == "Andre spesielt hensynskrevende arter"

    # Villmink: Fremmede arter
    assert villmink == "Fremmede arter"

    # Dverggås: Ansvarsarter + Spesielle okologiske former
    assert "Ansvarsarter" in dverggas
    assert "Spesielle okologiske former" in dverggas
    assert dverggas.count(",") == 1


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Legger til en kolonne med verdi fra M1941
    """)
    return


@app.function
def legg_til_verdi_m1941(df: pl.DataFrame) -> pl.DataFrame:
    """Add 'Verdi M1941' column based on conservation status and criteria.

    Mapping:
    - LC -> noe verdi
    - NT -> middels verdi
    - VU or Andre spesielt hensynskrevende arter -> stor verdi
    - EN, CR, or Prioriterte arter -> svært stor verdi

    Args:
        df: DataFrame with ``category`` and criteria columns.

    Returns:
        DataFrame with the added ``Verdi M1941`` column.
    """
    return df.with_columns(
        pl.when((pl.col("category").is_in(["EN", "CR"])) | (pl.col("Prioriterte arter") == "Yes"))
        .then(pl.lit("Svært stor verdi"))
        .when((pl.col("category") == "VU") | (pl.col("Andre spesielt hensynskrevende arter") == "Yes"))
        .then(pl.lit("Stor verdi"))
        .when(pl.col("category") == "NT")
        .then(pl.lit("Middels verdi"))
        .when(pl.col("category") == "LC")
        .then(pl.lit("Noe verdi"))
        .otherwise(pl.lit("Ikke definert"))
        .alias("Verdi M1941")
    )


@app.function
def test_legg_til_verdi_m1941():
    sample_df = pl.DataFrame(
        {
            "species": [
                "Hubro",  # Tester "EN" og "Prioriterte arter" == "Yes" -> Svært stor verdi
                "Gråspurv",  # Tester "LC" -> Noe verdi
                "Fjellrev",  # Tester "VU" og "Andre spesielt..." == "Yes" -> Stor verdi
                "Villmink",  # Tester udefinert kategori ("NA") -> Ikke deffinert
                "Dverggås",  # Tester "NT" -> Middels verdi
                "Gaupe",  # Tester "LC" men "Andre spesielt..." == "Yes" -> Stor verdi
                "Ulv",  # Tester "CR" -> Svært stor verdi
            ],
            "category": ["EN", "LC", "VU", "haraball", "NT", "LC", "CR"],
            "Ansvarsarter": ["Yes", "No", "No", "No", "Yes", "No", "No"],
            "Trua arter": ["No", "No", "No", "No", "No", "No", "No"],
            "Andre spesielt hensynskrevende arter": ["No", "No", "Yes", "No", "No", "Yes", "No"],
            "Spesielle okologiske former": ["No", "No", "No", "No", "Yes", "No", "No"],
            "Prioriterte arter": ["Yes", "No", "No", "No", "No", "No", "No"],
            "Fredete arter": ["Yes", "No", "No", "No", "No", "No", "No"],
            "NT": ["No", "No", "No", "No", "No", "No", "No"],
            "Fremmede arter": ["No", "No", "No", "Yes", "No", "No", "No"],
        }
    )

    result = legg_til_verdi_m1941(sample_df)

    hubro_df = result.filter(pl.col("species") == "Hubro")
    assert hubro_df.get_column("Verdi M1941").eq("Svært stor verdi").all(), "Hubro skal ha svært stor verdi"

    gråspurv_df = result.filter(pl.col("species") == "Gråspurv")
    assert gråspurv_df.get_column("Verdi M1941").eq("Noe verdi").all(), "Gråspurv skal ha noe verdi"

    fjellrev_df = result.filter(pl.col("species") == "Fjellrev")
    assert fjellrev_df.get_column("Verdi M1941").eq("Stor verdi").all(), "Fjellrev skal ha stor verdi"

    villmink_df = result.filter(pl.col("species") == "Villmink")
    assert villmink_df.get_column("Verdi M1941").eq("Ikke definert").all(), "Villmink skal ha ikke deffinert"

    dverggås_df = result.filter(pl.col("species") == "Dverggås")
    assert dverggås_df.get_column("Verdi M1941").eq("Middels verdi").all(), "Dverggås skal ha middels verdi"

    gaupe_df = result.filter(pl.col("species") == "Gaupe")
    assert gaupe_df.get_column("Verdi M1941").eq("Stor verdi").all(), "Gaupe skal ha stor verdi"

    ulv_df = result.filter(pl.col("species") == "Ulv")
    assert ulv_df.get_column("Verdi M1941").eq("Svært stor verdi").all(), "Ulv skal ha svært stor verdi"


@app.cell(column=1)
def _():
    valgt_fil = mo.ui.file_browser()
    valgt_fil
    return (valgt_fil,)


@app.cell
def _(valgt_fil):
    file_info = valgt_fil.value[0]
    filepath = file_info.path
    str(filepath)
    return (filepath,)


@app.cell
def _(filepath):
    orginal_df = mo.sql(
        f"""
        SELECT * FROM read_csv('{str(filepath)}');
        """
    )
    return (orginal_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Filtrerer ut alt før X tid (default 1990)
    """)
    return


@app.cell
def _(add_national_interest_criteria, orginal_df, process_and_enrich_data):
    df_artsdatabanken = process_and_enrich_data(orginal_df)

    df_alle_funksjoner = (
        df_artsdatabanken.pipe(add_national_interest_criteria)
        .pipe(legg_til_kolonne_arteravnasjonal)
        .pipe(legg_til_verdi_m1941)
    )
    return


@app.cell
def _(artsdata_df):
    arter_etter_1990 = artsdata_df.filter(pl.col("Observert dato") >= date(1990, 1, 1))
    return (arter_etter_1990,)


@app.cell
def _(artsdata_df):
    artsdata_df.null_count()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Legger til manglende artsnavn (dette steget må du gjøre)
    """)
    return


@app.cell
def _(arter_etter_1990):
    mangler_navn = (
        arter_etter_1990.filter(pl.col("Navn").is_null() | pl.col("Familie").is_null() | pl.col("Orden").is_null())
        .select(["Art", "Navn", "Familie", "Orden"])
        .unique()
        .sort("Art")
    )

    mangler_navn
    return (mangler_navn,)


@app.cell
def _(mangler_navn):
    navn_inputs = mo.ui.dictionary(
        {row["Art"]: mo.ui.text(placeholder="Skriv inn norsk navn...") for row in mangler_navn.iter_rows(named=True)}
    )

    navn_inputs
    return (navn_inputs,)


@app.cell
def _(arter_etter_1990, navn_inputs):
    # Get the mapping of species to Norwegian names from the UI inputs
    navn_mapping = {
        art: text_value
        for art, text_value in navn_inputs.value.items()
        if text_value  # Only include non-empty values
    }

    # Create a temporary dataframe with the mappings
    if navn_mapping:
        mapping_df = pl.DataFrame({"Art": list(navn_mapping.keys()), "Navn_ny": list(navn_mapping.values())})

        # Join with the original dataframe and update the Navn column
        endelig_datasett_for_nedlastning = (
            arter_etter_1990.join(mapping_df, on="Art", how="left")
            .with_columns(
                pl.when(pl.col("Navn").is_null() & pl.col("Navn_ny").is_not_null())
                .then(pl.col("Navn_ny"))
                .otherwise(pl.col("Navn"))
                .alias("Navn")
            )
            .drop("Navn_ny")
        )
    else:
        # If no names were entered, keep the original dataframe
        endelig_datasett_for_nedlastning = arter_etter_1990
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
