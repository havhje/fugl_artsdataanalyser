import marimo

__generated_with = "0.21.1"
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


@app.cell
def _():
    DATABASE_URL = "fugl_atributt_data"
    bird_data = duckdb.connect(DATABASE_URL, read_only=False)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Utility functions
    """)
    return


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


app._unparsable_cell(
    """
    def add_national_interest_criteria(df_enriched: pl.DataFrame) -> pl.DataFrame:
        \"\"\"Add national interest criteria from Excel file to enriched dataframe.

        Reads criteria columns from the national-interest Excel sheet, converts
        X-marks to Yes/No, and left-joins the result onto the enriched dataframe.

        Args:
            df_enriched: The enriched dataframe with species data.
            excel_path: Path to the Excel file with criteria. If None, uses
                the default path.

        Returns:
            DataFrame with added criteria columns.
        \"\"\"

        # Load Excel with criteria
        df_arter_nf = bird_data.execute(\"SELECT * FROM arter_av_nasjonal_forvaltningsinteresse\").pl()

        # Get criteria columns
        criteria_cols = [
            \"Kriterium_Ansvarsarter\",
            \"Kriterium_Trua_arter\",
            \"Kriterium_Andre spesielt_hensynskrevende_arter\",
            \"Kriterium_Spesielle_okologiske_former\",
            \"Kriterium_Prioriterte_arter\",
            \"Kriterium_Fredete_arter\",
            \"Kriterium_NT\",
            \"Kriterium_Fremmede_arter\"
        ]
    

        # Process criteria data - convert X marks to Yes/No and remve the Kriterium prefix
        criteria_data= 
        df_arter_nf.select(
            pl.col(\"ValidScientificNameId\"), #, legger til ett nytt argument slik at ny df består av scientificid OG ...
            *[ 
                pl.when(pl.col(c).str.to_uppercase().str.strip_chars() == \"X\")
                .then(pl.lit(\"YES\"))
                .otherwise(pl.lit(\"NO\"))
                .alias(c.replace(\"Kriterium_\", \"\").replace(\"_\", \" \"))
                for c in criteria_cols
            ] 
    # *[] er en list comprehension, hvor * sier pakk ut alle \"oppskriftene objektene (polars expression = objekter i lista og gjennomfør dette for alle kolonner som matcher de i criteria_cols\". Strukturen er hva du skal gjøre først og deretter hvor du skal gjøre det som er hvorfor loopen kommer til slutt. 

    
    ################## Har kommet hit###

        # Merge with enriched data
        df_with_criteria = df_enriched.join(
            criteria_data,
            left_on=\"validScientificNameId\",
            right_on=\"ValidScientificNameId\",
            how=\"left\",
        )

        # Fill nulls with \"No\" for non-matched rows # men non matching rows burde flahhes?? Skal ikke finnes??






    
        df_with_criteria = df_with_criteria.with_columns([pl.col(col).fill_null(\"No\") for col in criteria_renamed])

        return df_with_criteria
    """,
    name="*add_national_interest_criteria"
)


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
        "Andre spesielt hensynskrevende arter",
        "Spesielle okologiske former",
        "Prioriterte arter",
        "Fredete arter",
        "Fremmede arter",
    ]

    # Build the category list once
    expressions = []
    for col in category_columns:
        expr = pl.when(pl.col(col) == "Yes").then(pl.lit(col))
        expressions.append(expr)
    category_list = pl.concat_list(expressions).list.drop_nulls()

    return input_df.with_columns(
        pl.when(category_list.list.len() > 0)
        .then(category_list.list.join(", "))
        .otherwise(pl.lit("Nei"))
        .alias("Art av nasjonal forvaltningsinteresse")
    )


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
        .otherwise(None)
        .alias("Verdi M1941")
    )


@app.cell(column=1, hide_code=True)
def _():
    valgt_fil = mo.ui.file_browser()
    valgt_fil
    return (valgt_fil,)


@app.cell(hide_code=True)
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
    ### Oppdatere og rydder i datasettet
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
    return (df_alle_funksjoner,)


@app.cell
def _(df_alle_funksjoner):
    # Endrer navn, datatype og rekkefølge i dataframen
    artsdata_df = df_alle_funksjoner.select(
        [
            pl.col("category").alias("Kategori"),
            pl.col("Art av nasjonal forvaltningsinteresse"),
            pl.col("Verdi M1941"),
            pl.col("preferredPopularName").alias("Navn"),
            pl.col("validScientificName").alias("Art"),
            pl.col("individualCount")
            .fill_null("1")
            .str.split("/")  # Noen har en 1/1 antall - anter ikke hva det betyr
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
    artsdata_df
    return (artsdata_df,)


@app.cell(column=2)
def _(artsdata_df):
    artsdata_df.null_count()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Filtrerer ut alt før X tid (default 1990)
    """)
    return


@app.cell
def _(artsdata_df):
    arter_etter_1990 = artsdata_df.filter(pl.col("Observert dato") >= date(1990, 1, 1))
    return (arter_etter_1990,)


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


if __name__ == "__main__":
    app.run()
