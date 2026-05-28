import marimo

__generated_with = "0.23.6"
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
    from unittest.mock import patch
    from datetime import datetime, date as dt_date
    import typer
    from rich.console import Console
    from rich.table import Table
    from rich.prompt import Prompt
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
        MofNCompleteColumn,
    )
    from rich.panel import Panel
    from rich.rule import Rule


@app.cell
def todo():
    todo_liste = mo.callout(
        mo.md(
            r"""
    ### Todo

    - [ ] Du må sjekke hvordan verdi settes. Slik det er nå er ikke alle arter I mdir tabellen og disse får ikke treff, men kan ha høy rødlistevurdering. Altså må M1941 verdien settes utifra både mdir tabellen, men også rødliste vurderingen ifra artskart. du må tenke over hvilken logikk som skal gi verdi først og så deretter hvordan den andre skal tas inn.

    Mulig det er best om rødliste er den som gir verdi først, og deretter mdir tabellen. ta inn funksjonen som du slettet tidligere

    Les inn innput og outputtabellene slik at du kan se hvilke dette gjelder for

    - [ ] Forbedre logging/oppsummering for ANF-treff.
        - Dagens telling av `Treff ikke funnet` viser alle rader som ikke matcher Mdir/ANF-tabellen.
        - Dette blander reelle ikke-treff med taxa som bør få verdi fra rødlistevurdering fordi de mangler i Mdir/ANF-tabellen.
        - Lag en egen diagnostikk som skiller mellom:
            - reelle ikke-treff,
            - underarter/taxa som mangler i Mdir/ANF, men kan få verdi fra rødlistekategori,
            - andre mulige navn-/ID-avvik som bør kontrolleres manuelt.
    """
        ),
        kind="info",
    )

    todo_liste
    return


@app.cell
def _():
    DATABASE_URL = "databehandling/fugl_atributt_data"
    bird_data = duckdb.connect(DATABASE_URL, read_only=True)
    return (bird_data,)


@app.cell
def _():
    console = Console()
    return (console,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Rydder opp i navn og datatyper
    """)
    return


@app.function(hide_code=True)
def rydd_navn_og_datatyper(df_input: pl.DataFrame) -> pl.DataFrame:

    VERDI_M1941_ORDER = {
        "Svært stor verdi": 0,
        "Stor verdi": 1,
        "Middels verdi": 2,
        "Noe verdi": 3,
        "Ikke definert": 4,
    }

    KATEGORI_ORDER = {
        # Rødlistekategorier, mest alvorlig først
        "RE": 0,  # Regionalt utdødd
        "CR": 1,  # Kritisk truet
        "EN": 2,  # Sterkt truet
        "VU": 3,  # Sårbar
        "NT": 4,  # Nær truet
        "LC": 5,  # Livskraftig
        "DD": 6,  # Datamangel
        # Fremmedartslista, høJat økologisk risiko først
        "SE": 7,  # Svært høy risiko
        "HI": 8,  # Høy risiko
        "PH": 9,  # Potensielt høy risiko
        "LO": 10,  # Lav risiko
        "NK": 11,  # Ingen kjent risiko
        # Ikke vurderbare / ikke vurdert / ukjent
        "NA": 12,  # Ikke egnet
        "NE": 13,  # Ikke vurdert
        "Unknown": 14,
    }

    df_alle_funksjoner_ferdig_kjørt = (
        df_input.select(
            [
                pl.col("Verdi M1941"),
                pl.col("category").alias("Kategori"),
                pl.col("Art av nasjonal forvaltningsinteresse"),
                pl.col("preferredPopularName").alias("Navn"),
                pl.col("validScientificName").alias("Art"),
                pl.col("individualCount")
                .fill_null("1")  # antar at alle obs = minimum 1 når observatøren ikke har lagt inn spesifikt antall
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
                pl.col("Spesielle økologiske former"),
                pl.col("Prioriterte arter"),
                pl.col("Fredete arter"),
                pl.col("Fremmede arter"),
                pl.col("latitude").str.replace_all(",", ".").cast(pl.Float64),
                pl.col("longitude").str.replace_all(",", ".").cast(pl.Float64),
                pl.col("geometry"),
                pl.col("validScientificNameId").alias("Artens ID"),
            ]
        )
        # Sorterer i riktig rekkefølge
        .sort(
            by=[
                pl.col("Verdi M1941").replace_strict(
                    VERDI_M1941_ORDER,
                    default=999,
                ),
                pl.col("Kategori").replace_strict(
                    KATEGORI_ORDER,
                    default=999,
                ),
            ],
            maintain_order=True,
        )
    )

    return df_alle_funksjoner_ferdig_kjørt


@app.function(hide_code=True)
def test_rydd_navn_og_datatyper():

    # ── Build test input with all required columns ──────────────────
    test_df = pl.DataFrame(
        {
            "category": ["LC", "NT", "EN"],
            "Art av nasjonal forvaltningsinteresse": ["Ja", "Nei", "Ja"],
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
            "Ansvarsarter": ["Nei", "Nei", "Ja"],
            "Andre spesielt hensynskrevende arter": ["Nei", "Ja", "Nei"],
            "Spesielle økologiske former": ["Nei", "Nei", "Ja"],
            "Prioriterte arter": ["Nei", "Nei", "Ja"],
            "Fredete arter": ["Nei", "Nei", "Nei"],
            "Fremmede arter": ["Nei", "Nei", "Nei"],
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

    # ── Alle forventede kolonner skal finnes
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

    # ── Test kolonneomnavning og sortert rekkefølge ─────────────────
    # Funksjonen sorterer radene etter Verdi M1941 og Kategori.
    # Testdataene har ukjente M1941-verdier (B/C/D), så sortering skjer etter Kategori.
    # category → Kategori
    assert test_result.get_column("Kategori").to_list() == ["EN", "NT", "LC"], (
        "category skal omdøpes til Kategori og sorteres etter kategori"
    )

    # preferredPopularName → Navn
    assert test_result.get_column("Navn").to_list() == ["tjeld", "kråke", "dompap"], (
        "preferredPopularName skal omdøpes til Navn"
    )

    # validScientificName → Art
    assert test_result.get_column("Art").to_list() == [
        "Haematopus ostralegus",
        "Corvus cornix",
        "Pyrrhula pyrrhula",
    ], "validScientificName skal omdøpes til Art"

    # validScientificNameId → Artens ID
    assert test_result.get_column("Artens ID").to_list() == [3664, 4164, 4263], (
        "validScientificNameId skal omdøpes til Artens ID"
    )

    # FamilieNavn → Familie
    assert test_result.get_column("Familie").to_list() == [
        "Haematopodidae",
        "Corvidae",
        "Fringillidae",
    ], "FamilieNavn skal omdøpes til Familie"

    # OrdenNavn → Orden
    assert test_result.get_column("Orden").to_list() == [
        "Charadriiformes",
        "Passeriformes",
        "Passeriformes",
    ], "OrdenNavn skal omdøpes til Orden"

    # Spesielle okologiske former → Spesielle økologiske former (ø)
    assert "Spesielle økologiske former" in test_result.columns, (
        "'Spesielle okologiske former' skal omdøpes til 'Spesielle økologiske former' med ø"
    )

    # ── Test individualCount-transformasjon → Antall ────────────────
    antall = test_result.get_column("Antall")
    assert antall.dtype == pl.Int64, f"Antall skal være Int64, fikk {antall.dtype}"

    # "3/1" → 3 (split på '/' og ta første) - sortert til rad 0
    assert antall[0] == 3, f"individualCount '3/1' skal bli 3, fikk {antall[0]}"

    # None → 1 (fill_null med "1") - sortert til rad 1
    assert antall[1] == 1, f"individualCount null skal bli 1, fikk {antall[1]}"

    # "6" → 6 - sortert til rad 2
    assert antall[2] == 6, f"individualCount '6' skal bli 6, fikk {antall[2]}"

    # ── Test dateTimeCollected → Observert dato (date) ──────────────
    obs_dato = test_result.get_column("Observert dato")
    assert obs_dato.dtype == pl.Date, f"Observert dato skal være Date, fikk {obs_dato.dtype}"
    assert obs_dato[0] == dt_date(2019, 6, 1), f"Dato for sortert rad 0 skal være 2019-06-01, fikk {obs_dato[0]}"
    assert obs_dato[1] == dt_date(2023, 7, 4), f"Dato for sortert rad 1 skal være 2023-07-04, fikk {obs_dato[1]}"
    assert obs_dato[2] == dt_date(2022, 5, 15), f"Dato for sortert rad 2 skal være 2022-05-15, fikk {obs_dato[2]}"

    # ── Test coordinateUncertaintyInMeters → Usikkerhet meter ───────
    usikkerhet = test_result.get_column("Usikkerhet meter")
    assert usikkerhet.dtype == pl.Int64, f"Usikkerhet meter skal være Int64, fikk {usikkerhet.dtype}"
    assert usikkerhet[0] == 9, f"Usikkerhet for sortert rad 0 skal være 9, fikk {usikkerhet[0]}"
    assert usikkerhet[1] is None, f"Usikkerhet for sortert rad 1 skal være None, fikk {usikkerhet[1]}"
    assert usikkerhet[2] == 300, f"Usikkerhet for sortert rad 2 skal være 300, fikk {usikkerhet[2]}"

    # ── Test latitude komma → punktum → Float64 ────────────────────
    lat = test_result.get_column("latitude")
    assert lat.dtype == pl.Float64, f"latitude skal være Float64, fikk {lat.dtype}"
    # Latitude med punktum skal også fungere - sortert til rad 0
    assert abs(lat[0] - 68.974144) < 1e-5, f"latitude '68.974144' skal bli 68.974144, fikk {lat[0]}"
    assert abs(lat[1] - 68.962388) < 1e-5, f"latitude '68,962388' skal bli 68.962388, fikk {lat[1]}"
    assert abs(lat[2] - 68.904168) < 1e-5, f"latitude '68,904168' skal bli 68.904168, fikk {lat[2]}"

    # ── Test longitude komma → punktum → Float64 ───────────────────
    lon = test_result.get_column("longitude")
    assert lon.dtype == pl.Float64, f"longitude skal være Float64, fikk {lon.dtype}"
    assert abs(lon[0] - 14.947151) < 1e-5, f"longitude '14.947151' skal bli 14.947151, fikk {lon[0]}"
    assert abs(lon[1] - 15.148183) < 1e-5, f"longitude '15,148183' skal bli 15.148183, fikk {lon[1]}"
    assert abs(lon[2] - 15.066918) < 1e-5, f"longitude '15,066918' skal bli 15.066918, fikk {lon[2]}"

    # ── Test at kolonner som ikke omdøpes beholder verdier ──────────
    assert test_result.get_column("Atferd").to_list() == ["flying", None, "singing"], (
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
        "POINT (497884 7651480)",
        "POINT (505937 7650175)",
        "POINT (502688 7643678)",
    ], "geometry skal beholde sine verdier i sortert radrekkefølge"

    assert test_result.get_column("Art av nasjonal forvaltningsinteresse").to_list() == [
        "Ja",
        "Nei",
        "Ja",
    ], "Art av nasjonal forvaltningsinteresse skal beholde sine verdier"

    assert test_result.get_column("Verdi M1941").to_list() == ["B", "D", "C"], (
        "Verdi M1941 skal beholde sine verdier i sortert radrekkefølge"
    )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Henter tillatte kolonner fra artskart
    """)
    return


@app.function(hide_code=True)
def get_required_artskart_columns() -> set[str]:
    """Return the Artskart columns required by the processing pipeline."""
    return {
        "category",
        "validScientificNameId",
        "validScientificName",
        "preferredPopularName",
        "taxonGroupName",
        "collector",
        "dateTimeCollected",
        "locality",
        "coordinateUncertaintyInMeters",
        "municipality",
        "county",
        "individualCount",
        "latitude",
        "longitude",
        "geometry",
        "scientificNameRank",
        "behavior",
    }


@app.function(hide_code=True)
def get_allowed_categories() -> set[str]:
    """Return accepted red-list and alien-species category codes."""
    return {
        "RE",
        "CR",
        "EN",
        "VU",
        "NT",
        "LC",
        "DD",
        "SE",
        "HI",
        "PH",
        "LO",
        "NK",
        "NA",
        "NE",
        "Unknown",
    }


@app.function(hide_code=True)
def validate_artskart_input_contract(df: pl.DataFrame) -> None:
    """Validate the Artskart input schema and category domain values."""
    required_columns = get_required_artskart_columns()
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise ValueError(f"Mangler obligatoriske Artskart-kolonner: {', '.join(missing_columns)}")

    allowed_categories = get_allowed_categories()
    unknown_categories = (
        df.select(pl.col("category").cast(pl.Utf8).alias("category"))
        .filter(pl.col("category").is_null() | ~pl.col("category").is_in(allowed_categories))
        .get_column("category")
        .unique()
        .to_list()
    )
    if unknown_categories:
        formatted_unknown_categories = sorted("<null>" if value is None else str(value) for value in unknown_categories)
        raise ValueError(f"Ukjente category-verdier i Artskart-data: {', '.join(formatted_unknown_categories)}")


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


@app.cell(hide_code=True)
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
                    name = level.get("scientificName")
                    hierarchy[rank] = name.strip() if name else None
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


@app.cell(hide_code=True)
def _(
    DESIRED_RANKS,
    RATE_LIMIT_DELAY,
    console,
    extract_hierarchy_and_ids,
    fetch_taxon_data,
    get_norwegian_name,
):
    def process_and_enrich_data(source_df: pl.DataFrame) -> pl.DataFrame:
        """Process the dataframe and enrich with taxonomy data.

        Fetches taxonomic hierarchy and Norwegian vernacular names from the
        NorTaxa API for each unique species ID, then joins the results back
        onto the source dataframe.

        Args:
            source_df: Input dataframe containing a ``validScientificNameId`` column.

        Returns:
            Enriched dataframe with taxonomy columns.

        Raises:
            ValueError: If the required ID column is missing or Nei valid IDs exist.
            RuntimeError: If NorTaxa API calls for valid IDs fail or return empty data.
        """
        # Konverterer til Polars for bedre ytelse
        if isinstance(source_df, pl.DataFrame):
            df_work = source_df.clone()
        else:
            df_work = pl.from_pandas(source_df)

        if "validScientificNameId" not in df_work.columns:
            message = "'validScientificNameId'-kolonnen finnes ikke i datasettet."
            console.print(f"[bold red]Feil:[/bold red] {message}")
            raise ValueError(message)

        unique_ids = df_work.select("validScientificNameId").unique().to_series().to_list()
        total_ids = len(unique_ids)
        lookup_id_dtype = df_work["validScientificNameId"].dtype

        taxonomy_data: dict[int, dict[str, str | None]] = {}
        family_names: dict[int, str | None] = {}
        order_names: dict[int, str | None] = {}
        valid_species_ids: list[int] = []
        invalid_ids: list[Any] = []
        failed_api_ids: list[str] = []

        # Hent taksonomidata med Rich-fremdriftsindikator
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Henter taksonomidata fra NorTaxa API...", total=total_ids)

            for i, raw_species_id in enumerate(unique_ids):
                if raw_species_id is None or raw_species_id != raw_species_id:  # fanger None og NaN
                    invalid_ids.append(raw_species_id)
                    progress.update(
                        task,
                        advance=1,
                        description=f"Hopper over ugyldig ID ({i + 1}/{total_ids})",
                    )
                    continue

                try:
                    species_id = int(raw_species_id)
                except ValueError:
                    invalid_ids.append(raw_species_id)
                    progress.update(
                        task,
                        advance=1,
                        description=f"Hopper over ugyldig ID ({i + 1}/{total_ids})",
                    )
                    continue
                except TypeError:
                    invalid_ids.append(raw_species_id)
                    progress.update(
                        task,
                        advance=1,
                        description=f"Hopper over ugyldig ID ({i + 1}/{total_ids})",
                    )
                    continue

                valid_species_ids.append(species_id)

                species_data = fetch_taxon_data(species_id)
                if not species_data:
                    failed_api_ids.append(str(species_id))
                else:
                    hierarchy, family_id, order_id = extract_hierarchy_and_ids(species_data)
                    taxonomy_data[species_id] = hierarchy

                    # Hent norsk familienavn hvis API-et oppgir familie-ID
                    if family_id:
                        family_data = fetch_taxon_data(family_id)
                        if family_data:
                            family_names[species_id] = get_norwegian_name(family_data)
                        else:
                            failed_api_ids.append(f"{species_id} (Family {family_id})")

                    # Hent norsk ordennavn hvis API-et oppgir orden-ID
                    if order_id:
                        order_data = fetch_taxon_data(order_id)
                        if order_data:
                            order_names[species_id] = get_norwegian_name(order_data)
                        else:
                            failed_api_ids.append(f"{species_id} (Order {order_id})")

                if RATE_LIMIT_DELAY > 0:
                    time.sleep(RATE_LIMIT_DELAY)

                progress.update(
                    task,
                    advance=1,
                    description=f"Henter ID {species_id} ({i + 1}/{total_ids})",
                )

        if invalid_ids:
            formatted_invalid_ids = ["<null>" if value is None else str(value) for value in invalid_ids[:10]]
            console.print(
                f"[yellow]Advarsel:[/yellow] Hopper over {len(invalid_ids)} ugyldige art-IDer: "
                f"{formatted_invalid_ids}{'...' if len(invalid_ids) > 10 else ''}"
            )

        if not valid_species_ids:
            raise ValueError("Ingen gyldige validScientificNameId-verdier å hente fra NorTaxa API.")

        if failed_api_ids:
            examples = ", ".join(failed_api_ids[:10])
            raise RuntimeError(
                f"NorTaxa API-kall feilet eller ga tomt resultat for {len(failed_api_ids)} ID-er. Eksempel-IDer: {examples}"
            )

        taxonomy_rows = [
            {"validScientificNameId": sid, **{rank: hierarchy.get(rank) for rank in DESIRED_RANKS}}
            for sid, hierarchy in taxonomy_data.items()
        ]
        taxonomy_schema = {"validScientificNameId": lookup_id_dtype, **{rank: pl.Utf8 for rank in DESIRED_RANKS}}
        taxonomy_lookup = pl.DataFrame(taxonomy_rows) if taxonomy_rows else pl.DataFrame(schema=taxonomy_schema)
        taxonomy_lookup = taxonomy_lookup.with_columns(
            pl.col("validScientificNameId").cast(lookup_id_dtype),
            *[pl.col(rank).cast(pl.Utf8) for rank in DESIRED_RANKS],
        )

        name_rows = [
            {
                "validScientificNameId": sid,
                "FamilieNavn": family_names.get(sid),
                "OrdenNavn": order_names.get(sid),
            }
            for sid in sorted(set(family_names) | set(order_names))
        ]
        name_schema = {"validScientificNameId": lookup_id_dtype, "FamilieNavn": pl.Utf8, "OrdenNavn": pl.Utf8}
        name_lookup = pl.DataFrame(name_rows) if name_rows else pl.DataFrame(schema=name_schema)
        name_lookup = name_lookup.with_columns(
            pl.col("validScientificNameId").cast(lookup_id_dtype),
            pl.col("FamilieNavn").cast(pl.Utf8),
            pl.col("OrdenNavn").cast(pl.Utf8),
        )

        df_work = df_work.join(taxonomy_lookup, on="validScientificNameId", how="left").join(
            name_lookup, on="validScientificNameId", how="left"
        )

        return df_work

    return (process_and_enrich_data,)


@app.cell(hide_code=True)
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
        assert honsehauk.get_column("Genus").eq("Astur").all(), "Hønsehauk should be in Astur genus"
        assert honsehauk.get_column("FamilieNavn").eq("haukefamilien").all(), (
            "Hønsehauk should have FamilieNavn 'haukefamilien'"
        )
        assert honsehauk.get_column("OrdenNavn").eq("haukefugler").all(), "Hønsehauk should have OrdenNavn 'rovfugler'"

    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Legg til verdi M1941
    """)
    return


@app.function
def legg_til_verdi_m1941(df: pl.DataFrame) -> pl.DataFrame:
    """Add 'Verdi M1941' column based on conservation status and criteria.

    Mapping:
    - LC -> noe verdi
    - NT -> middels verdi
    - VU  -> stor verdi
    - EN, CR -> svært stor verdi

    Args:
        df: DataFrame with ``category`` and criteria columns.

    Returns:
        DataFrame with the added ``Verdi M1941`` column.
    """
    return df.with_columns(
        pl.when(pl.col("category").is_in(["EN", "CR"]))
            .then(pl.lit("Svært stor verdi"))
            .when(pl.col("category") == "VU")
            .then(pl.lit("Stor verdi"))
            .when(pl.col("category") == "NT")
            .then(pl.lit("Middels verdi"))
            .when(pl.col("category") == "LC")
            .then(pl.lit("Noe verdi"))
            .otherwise(None)
            .alias("verdi_rodliste_artskart")
            )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Arter av nasjonal forvaltningsinteresse
    """)
    return


@app.cell
def _(bird_data):
    df_nf = bird_data.execute("SELECT * FROM arter_av_nasjonal_forvaltningsinteresse").pl()
    df_nf
    return


@app.cell
def _add_national_interest_criteria(bird_data):
    def add_national_interest_criteria(df_enriched: pl.DataFrame) -> pl.DataFrame:
        """Add national interest criteria from Excel file to enriched dataframe."""

        # Load Excel with criteria
        df_arter_nf = bird_data.execute("SELECT * FROM arter_av_nasjonal_forvaltningsinteresse").pl()

        df_arter_nf_ryddet = df_arter_nf.select(
            [
                pl.col("vitenskapelig_navn_id").alias("arts_id_mdir"),
                pl.col("vitenskapelig_navn").alias("vitenskapelig_navn_mdir"),
                pl.col("forvaltningsverdi").alias("Verdi M1941"),
                pl.col("kriterium_prioriterte_arter").alias("Prioriterte arter"),
                pl.col("kriterium_fredete_arter").alias("Fredete arter"),
                pl.col("kriterium_andre_spesielt_hensynskrevende_arter").alias("Andre spesielt hensynskrevende arter"),
                pl.col("kriterium_spesielle_okologiske_former").alias("Spesielle økologiske former"),
                pl.col("kriterium_dd").alias("Datamangel"),
                pl.col("kriterium_hensynskrevende_arter").alias("Hensynskrevende arter"),
                pl.col("kriterium_ansvarsart").alias("Ansvarsarter"),
                pl.col("kriterium_fremmede_arter").alias("Fremmede arter"),
            ]
        )

        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
            "Verdi M1941",
        ]

        criteria_data = df_arter_nf_ryddet.with_columns(
            *[pl.when(pl.col(c) == 1).then(pl.lit("Ja")).otherwise(pl.lit("Nei")).alias(c) for c in criteria_cols]
        )

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
            .with_columns(*[pl.coalesce(c, f"{c}_fallback") for c in criteria_cols])
            # bruker coalece som sier velg først join 1 (c), hvis det finnes null verdier bruk verdiene i join 2 (c_fallback)
            .drop(([f"{c}_fallback" for c in criteria_cols]))
            .drop(pl.col("vitenskapelig_navn_mdir"), pl.col("arts_id_mdir"))
            # dropper kollonene som er lagt til 2 ganger
        )

        return df_with_criteria

    return (add_national_interest_criteria,)


@app.cell(hide_code=True)
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
                    4910,  # blodigle
                    3685,  # nordlig sildemåke
                    1807,  # pelekreps
                    3454,  # krikkand
                    999999,  # finnes ikke i kriterietabellen
                ],
                "validScientificName": [
                    "Clangula hyemalis",  # havelle
                    "Limosa limosa islandica",  # svarthalespove
                    "Accipiter gentilis",  # hønsehauk
                    "Anser erythropus",  # dverggås
                    "Branta canadensis",  # kanadagås
                    "Hirudo medicinalis",  # blodigle
                    "Larus fuscus fuscus",  # nordlig sildemåke
                    "Chelura terebrans",  # pelekreps
                    "Anas crecca",  # krikkand
                    "Nonexistent species",  # finnes ikke
                ],
            }
        )

        test_result = add_national_interest_criteria(test_df_anf)

        expected_criteria_cols = [
            "Ansvarsarter",
            "Andre spesielt hensynskrevende arter",
            "Hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Prioriterte arter",
            "Fredete arter",
            "Fremmede arter",
        ]

        # Alle kriteriekolonner skal finnes i resultatet
        for col in expected_criteria_cols:
            assert col in test_result.columns, f"Kolonne '{col}' mangler i resultatet"

        # Antall rader skal være uendret
        assert test_result.height == 10, f"Forventet 10 rader, fikk {test_result.height}"

        # Test havelle (3506) – nært trua art og andre spesielt hensynskrevende
        havelle = test_result.filter(pl.col("validScientificNameId") == 3506)
        assert havelle.height > 0, "Havelle ikke funnet i resultatet"
        assert havelle.get_column("Ansvarsarter").eq("Nei").all(), "Havelle er ikke en ansvarsart"
        assert havelle.get_column("Andre spesielt hensynskrevende arter").eq("Ja").all(), (
            "Havelle er en andre spesielt hensynskrevende art"
        )
        assert havelle.get_column("Hensynskrevende arter").eq("Nei").all(), "Havelle er ikke en hensynskrevende art"
        assert havelle.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Havelle er ikke en spesiell økologisk form"
        )
        assert havelle.get_column("Datamangel").eq("Nei").all(), "Havelle har ikke datamangel"
        assert havelle.get_column("Prioriterte arter").eq("Nei").all(), "Havelle er ikke en prioritert art"
        assert havelle.get_column("Fredete arter").eq("Nei").all(), "Havelle er ikke en fredet art"
        assert havelle.get_column("Fremmede arter").eq("Nei").all(), "Havelle er ikke en fremmed art"

        # Test svarthalespove (3768) – prioritert underart
        svarthalespove = test_result.filter(pl.col("validScientificNameId") == 3768)
        assert svarthalespove.height > 0, "Svarthalespove ikke funnet i resultatet"
        assert svarthalespove.get_column("Ansvarsarter").eq("Nei").all(), "Svarthalespove er ikke en ansvarsart"
        assert svarthalespove.get_column("Andre spesielt hensynskrevende arter").eq("Nei").all(), (
            "Svarthalespove er ikke en andre spesielt hensynskrevende art"
        )
        assert svarthalespove.get_column("Hensynskrevende arter").eq("Nei").all(), (
            "Svarthalespove er ikke en hensynskrevende art"
        )
        assert svarthalespove.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Svarthalespove er ikke en spesiell økologisk form"
        )
        assert svarthalespove.get_column("Datamangel").eq("Nei").all(), "Svarthalespove har ikke datamangel"
        assert svarthalespove.get_column("Prioriterte arter").eq("Ja").all(), "Svarthalespove er en prioritert art"
        assert svarthalespove.get_column("Fredete arter").eq("Nei").all(), "Svarthalespove er ikke en fredet art"
        assert svarthalespove.get_column("Fremmede arter").eq("Nei").all(), "Svarthalespove er ikke en fremmed art"

        # Test hønsehauk (295741) – sårbar art (matchet via vitenskapelig navn-fallback)
        hønsehauk = test_result.filter(pl.col("validScientificNameId") == 295741)
        assert hønsehauk.height > 0, "Hønsehauk ikke funnet i resultatet"
        assert hønsehauk.get_column("Ansvarsarter").eq("Nei").all(), "Hønsehauk er ikke en ansvarsart"
        assert hønsehauk.get_column("Andre spesielt hensynskrevende arter").eq("Ja").all(), (
            "Hønsehauk er ikke en andre spesielt hensynskrevende art"
        )
        assert hønsehauk.get_column("Hensynskrevende arter").eq("Nei").all(), "Hønsehauk er ikke en hensynskrevende art"
        assert hønsehauk.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Hønsehauk er ikke en spesiell økologisk form"
        )
        assert hønsehauk.get_column("Datamangel").eq("Nei").all(), "Hønsehauk har ikke datamangel"
        assert hønsehauk.get_column("Prioriterte arter").eq("Nei").all(), "Hønsehauk er ikke en prioritert art"
        assert hønsehauk.get_column("Fredete arter").eq("Nei").all(), "Hønsehauk er ikke en fredet art"
        assert hønsehauk.get_column("Fremmede arter").eq("Nei").all(), "Hønsehauk er ikke en fremmed art"

        # Test dverggås (3478) – ansvarsart, trua art og prioritert art
        dverggås = test_result.filter(pl.col("validScientificNameId") == 3478)
        assert dverggås.height > 0, "Dverggås ikke funnet i resultatet"
        assert dverggås.get_column("Ansvarsarter").eq("Ja").all(), "Dverggås er en ansvarsart"
        assert dverggås.get_column("Andre spesielt hensynskrevende arter").eq("Ja").all(), (
            "Dverggås er ikke en andre spesielt hensynskrevende art"
        )
        assert dverggås.get_column("Hensynskrevende arter").eq("Nei").all(), "Dverggås er ikke en hensynskrevende art"
        assert dverggås.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Dverggås er ikke en spesiell økologisk form"
        )
        assert dverggås.get_column("Datamangel").eq("Nei").all(), "Dverggås har ikke datamangel"
        assert dverggås.get_column("Prioriterte arter").eq("Ja").all(), "Dverggås er en prioritert art"
        assert dverggås.get_column("Fredete arter").eq("Nei").all(), "Dverggås er ikke en fredet art"
        assert dverggås.get_column("Fremmede arter").eq("Nei").all(), "Dverggås er ikke en fremmed art"

        # Test kanadagås (3495) – fremmed art
        kanadagås = test_result.filter(pl.col("validScientificNameId") == 3495)
        assert kanadagås.height > 0, "Kanadagås ikke funnet i resultatet"
        assert kanadagås.get_column("Ansvarsarter").eq("Nei").all(), "Kanadagås er ikke en ansvarsart"
        assert kanadagås.get_column("Andre spesielt hensynskrevende arter").eq("Nei").all(), (
            "Kanadagås er ikke en andre spesielt hensynskrevende art"
        )
        assert kanadagås.get_column("Hensynskrevende arter").eq("Nei").all(), "Kanadagås er ikke en hensynskrevende art"
        assert kanadagås.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Kanadagås er ikke en spesiell økologisk form"
        )
        assert kanadagås.get_column("Datamangel").eq("Nei").all(), "Kanadagås har ikke datamangel"
        assert kanadagås.get_column("Prioriterte arter").eq("Nei").all(), "Kanadagås er ikke en prioritert art"
        assert kanadagås.get_column("Fredete arter").eq("Nei").all(), "Kanadagås er ikke en fredet art"
        assert kanadagås.get_column("Fremmede arter").eq("Ja").all(), "Kanadagås er en fremmed art"

        # Test blodigle (4910) – fredet art
        blodigle = test_result.filter(pl.col("validScientificNameId") == 4910)
        assert blodigle.height > 0, "Blodigle ikke funnet i resultatet"
        assert blodigle.get_column("Ansvarsarter").eq("Nei").all(), "Blodigle er ikke en ansvarsart"
        assert blodigle.get_column("Andre spesielt hensynskrevende arter").eq("Nei").all(), (
            "Blodigle er ikke en andre spesielt hensynskrevende art"
        )
        assert blodigle.get_column("Hensynskrevende arter").eq("Nei").all(), "Blodigle er ikke en hensynskrevende art"
        assert blodigle.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Blodigle er ikke en spesiell økologisk form"
        )
        assert blodigle.get_column("Datamangel").eq("Nei").all(), "Blodigle har ikke datamangel"
        assert blodigle.get_column("Prioriterte arter").eq("Nei").all(), "Blodigle er ikke en prioritert art"
        assert blodigle.get_column("Fredete arter").eq("Ja").all(), "Blodigle er en fredet art"
        assert blodigle.get_column("Fremmede arter").eq("Nei").all(), "Blodigle er ikke en fremmed art"

        # Test nordlig sildemåke (3685) – spesiell økologisk form
        nordlig_sildemåke = test_result.filter(pl.col("validScientificNameId") == 3685)
        assert nordlig_sildemåke.height > 0, "Nordlig sildemåke ikke funnet i resultatet"
        assert nordlig_sildemåke.get_column("Ansvarsarter").eq("Nei").all(), "Nordlig sildemåke er ikke en ansvarsart"
        assert nordlig_sildemåke.get_column("Andre spesielt hensynskrevende arter").eq("Nei").all(), (
            "Nordlig sildemåke er ikke en andre spesielt hensynskrevende art"
        )
        assert nordlig_sildemåke.get_column("Hensynskrevende arter").eq("Nei").all(), (
            "Nordlig sildemåke er ikke en hensynskrevende art"
        )
        assert nordlig_sildemåke.get_column("Spesielle økologiske former").eq("Ja").all(), (
            "Nordlig sildemåke er en spesiell økologisk form"
        )
        assert nordlig_sildemåke.get_column("Datamangel").eq("Nei").all(), "Nordlig sildemåke har ikke datamangel"
        assert nordlig_sildemåke.get_column("Prioriterte arter").eq("Nei").all(), (
            "Nordlig sildemåke er ikke en prioritert art"
        )
        assert nordlig_sildemåke.get_column("Fredete arter").eq("Nei").all(), "Nordlig sildemåke er ikke en fredet art"
        assert nordlig_sildemåke.get_column("Fremmede arter").eq("Nei").all(), "Nordlig sildemåke er ikke en fremmed art"

        # Test pelekreps (1807) – datamangel
        pelekreps = test_result.filter(pl.col("validScientificNameId") == 1807)
        assert pelekreps.height > 0, "Pelekreps ikke funnet i resultatet"
        assert pelekreps.get_column("Ansvarsarter").eq("Nei").all(), "Pelekreps er ikke en ansvarsart"
        assert pelekreps.get_column("Andre spesielt hensynskrevende arter").eq("Nei").all(), (
            "Pelekreps er ikke en andre spesielt hensynskrevende art"
        )
        assert pelekreps.get_column("Hensynskrevende arter").eq("Nei").all(), "Pelekreps er ikke en hensynskrevende art"
        assert pelekreps.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Pelekreps er ikke en spesiell økologisk form"
        )
        assert pelekreps.get_column("Datamangel").eq("Ja").all(), "Pelekreps har datamangel"
        assert pelekreps.get_column("Prioriterte arter").eq("Nei").all(), "Pelekreps er ikke en prioritert art"
        assert pelekreps.get_column("Fredete arter").eq("Nei").all(), "Pelekreps er ikke en fredet art"
        assert pelekreps.get_column("Fremmede arter").eq("Nei").all(), "Pelekreps er ikke en fremmed art"

        # Test krikkand (3454) – hensynskrevende art
        krikkand = test_result.filter(pl.col("validScientificNameId") == 3454)
        assert krikkand.height > 0, "Krikkand ikke funnet i resultatet"
        assert krikkand.get_column("Ansvarsarter").eq("Nei").all(), "Krikkand er ikke en ansvarsart"
        assert krikkand.get_column("Andre spesielt hensynskrevende arter").eq("Nei").all(), (
            "Krikkand er ikke en andre spesielt hensynskrevende art"
        )
        assert krikkand.get_column("Hensynskrevende arter").eq("Ja").all(), "Krikkand er en hensynskrevende art"
        assert krikkand.get_column("Spesielle økologiske former").eq("Nei").all(), (
            "Krikkand er ikke en spesiell økologisk form"
        )
        assert krikkand.get_column("Datamangel").eq("Nei").all(), "Krikkand har ikke datamangel"
        assert krikkand.get_column("Prioriterte arter").eq("Nei").all(), "Krikkand er ikke en prioritert art"
        assert krikkand.get_column("Fredete arter").eq("Nei").all(), "Krikkand er ikke en fredet art"
        assert krikkand.get_column("Fremmede arter").eq("Nei").all(), "Krikkand er ikke en fremmed art"

        # Test ID som ikke finnes (999999) -> "Treff ikke funnet"
        missing = test_result.filter(pl.col("validScientificNameId") == 999999)
        assert missing.height > 0, "Manglende art (999999) ikke funnet i resultatet"
        for col in expected_criteria_cols:
            assert missing.get_column(col).eq("Treff ikke funnet").all(), (
                f"Kolonne '{col}' skal være 'Treff ikke funnet' for ukjent art"
            )

        # Verdier skal kun være "Ja", "Nei" eller "Treff ikke funnet"
        valid_values = {"Ja", "Nei", "Treff ikke funnet"}
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


@app.function(hide_code=True)
def legg_til_kolonne_arteravnasjonal(input_df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a new column 'Art av nasjonal forvaltningsinteresse' to the DataFrame.

    The new column is populated based on the values in a predefined list of
    category columns. If any of these columns have the value "Ja", the new
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
        "Hensynskrevende arter",
        "Spesielle økologiske former",
        "Datamangel",
        "Prioriterte arter",
        "Fredete arter",
        "Fremmede arter",
    ]

    missing_lookup_marker = "Treff ikke funnet"

    category_list = (
        pl.concat_list(  # slår sammen alle anf til en kolonne, men merk List Concatenation  = packing items into a list within a single row ( noe annet enn a stacke tabbeller)
            *[
                pl.when(pl.col(col) == "Ja").then(pl.lit(col))  # erstatter Ja/Nei med kolonne navnet
                for col in category_columns
            ]
        ).list.drop_nulls()  # fjerner null verdier slik at du ikke får NT, null, null, Fremmed art
    )
    has_missing_lookup = pl.any_horizontal(*[pl.col(col) == missing_lookup_marker for col in category_columns])

    output_df = input_df.with_columns(
        pl.when(category_list.list.len() > 0)
        .then(
            category_list.list.join(", ")
        )  # You need .list.join(", ") because pl.concat_list() gives you the computer-code format (a list object), and you want the human-readable format (a single text string). Hvor du da joiner tingene i listen med ,
        .when(has_missing_lookup)
        .then(pl.lit(missing_lookup_marker))
        .otherwise(pl.lit("Nei"))
        .alias("Art av nasjonal forvaltningsinteresse")
    )

    return output_df


@app.function(hide_code=True)
def test_legg_til_kolonne_arteravnasjonal():
    sample_df = pl.DataFrame(
        {
            "species": ["Hubro", "Gråspurv", "Fjellrev", "Villmink", "Dverggås", "Pelekreps", "Krikkand", "Ukjent art"],
            "Ansvarsarter": ["Ja", "Nei", "Nei", "Nei", "Ja", "Nei", "Nei", "Treff ikke funnet"],
            "Andre spesielt hensynskrevende arter": [
                "Nei",
                "Nei",
                "Ja",
                "Nei",
                "Nei",
                "Nei",
                "Nei",
                "Treff ikke funnet",
            ],
            "Hensynskrevende arter": ["Nei", "Nei", "Nei", "Nei", "Nei", "Nei", "Ja", "Treff ikke funnet"],
            "Spesielle økologiske former": ["Nei", "Nei", "Nei", "Nei", "Ja", "Nei", "Nei", "Treff ikke funnet"],
            "Datamangel": ["Nei", "Nei", "Nei", "Nei", "Nei", "Ja", "Nei", "Treff ikke funnet"],
            "Prioriterte arter": ["Ja", "Nei", "Nei", "Nei", "Nei", "Nei", "Nei", "Treff ikke funnet"],
            "Fredete arter": ["Ja", "Nei", "Nei", "Nei", "Nei", "Nei", "Nei", "Treff ikke funnet"],
            "Fremmede arter": ["Nei", "Nei", "Nei", "Ja", "Nei", "Nei", "Nei", "Treff ikke funnet"],
        }
    )

    result = legg_til_kolonne_arteravnasjonal(sample_df)

    # Column should exist. Henter alle kolonne navnene med .columns til en liste du tester mot (i.e. in "the list")
    assert "Art av nasjonal forvaltningsinteresse" in result.columns

    # Tar verdiene i kolonnen "art av nasjonal forvaltningsinteresse" og gjør verdiene for hver rad i kolonnen om til en liste med verdiene og en index som korrensponderer til raden/arten
    values = result.get_column("Art av nasjonal forvaltningsinteresse").to_list()

    # Unpacker (en egen python greie (må ha samme "lengde") lista inn i artene (som er de fra df), slik at disse får sine korrensponderende verdier fra ANF kolonnen i orginal df
    hubro, graspurv, fjellrev, villmink, dverggas, pelekreps, krikkand, ukjent_art = values

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

    # Dverggås: Ansvarsarter + Spesielle økologiske former
    assert "Ansvarsarter" in dverggas
    assert "Spesielle økologiske former" in dverggas
    assert dverggas.count(",") == 1

    # Pelekreps: Datamangel
    assert pelekreps == "Datamangel"

    # Krikkand: Hensynskrevende arter
    assert krikkand == "Hensynskrevende arter"

    # Ukjent art: Treff ikke funnet
    assert ukjent_art == "Treff ikke funnet"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Legger til manglende artsnavn
    """)
    return


@app.function(hide_code=True)
def finn_mangler_navn(df: pl.DataFrame) -> pl.DataFrame:
    """Finn arter som mangler norsk navn (Navn-kolonnen er null)."""

    mangler_df = df.filter(pl.col("Navn").is_null()).select(["Art", "Navn", "Familie", "Orden"]).unique().sort("Art")

    return mangler_df


@app.function(hide_code=True)
def test_finn_mangler_navn():
    sample_df = pl.DataFrame(
        {
            "Art": [
                "Bubo bubo",
                "Passer domesticus",
                "Vulpes lagopus",
                "Neogale vison",
                "Anser erythropus",
                "Lynx lynx",
                "Canis lupus",
            ],
            "Navn": [
                "Hubro",
                "Gråspurv",
                None,  # Fjellrev mangler norsk navn
                "Villmink",
                None,  # Dverggås mangler norsk navn
                "Gaupe",
                None,  # Ulv mangler norsk navn
            ],
            "Familie": [
                "Strigidae",
                "Passeridae",
                "Canidae",
                "Mustelidae",
                "Anatidae",
                "Felidae",
                "Canidae",
            ],
            "Orden": [
                "Strigiformes",
                "Passeriformes",
                "Carnivora",
                "Carnivora",
                "Anseriformes",
                "Carnivora",
                "Carnivora",
            ],
            "category": ["EN", "LC", "VU", "haraball", "NT", "LC", "CR"],
        }
    )

    result = finn_mangler_navn(sample_df)

    # Should find exactly the 3 rows with Navn == null
    assert result.height == 3, f"Forventet 3 arter uten navn, fikk {result.height}"

    # Should only contain the correct columns
    assert result.columns == ["Art", "Navn", "Familie", "Orden"]

    # All returned rows should have null Navn
    assert result.get_column("Navn").is_null().all(), "Alle returnerte rader skal ha null Navn"

    # The specific species missing names
    arter_uten_navn = set(
        result.get_column("Art").to_list()
    )  # bruker set istenden for list, for da betyr ikke rekkefølgen av variablene noe. Sets ignore order, so {"A", "B"} == {"B", "A"} is True. If you used lists instead, ["A", "B"] == ["B", "A"] would be False. This makes the assertion robust — you don't care which order the rows came in, just which species are present.
    assert arter_uten_navn == {"Vulpes lagopus", "Anser erythropus", "Canis lupus"}, (
        f"Feil arter returnert: {arter_uten_navn}"
    )

    # Should be sorted by Art
    art_list = result.get_column("Art").to_list()
    assert art_list == sorted(art_list), "Resultatet skal være sortert etter Art"

    # Test with Nei missing names — should return empty DataFrame
    complete_df = sample_df.with_columns(pl.col("Navn").fill_null("Ukjent"))
    empty_result = finn_mangler_navn(complete_df)
    assert empty_result.height == 0, "Skal returnere tom DataFrame når alle har navn"


@app.cell(hide_code=True)
def _(console):
    def prompt_mangler_navn(mangler_df: pl.DataFrame) -> dict[str, str]:
        """Vis arter uten norsk navn og be bruker skrive inn navn."""

        if mangler_df.is_empty():
            return {}

        arter = mangler_df.get_column("Art").fill_null("—").to_list()
        familier = mangler_df.get_column("Familie").fill_null("—").to_list()
        ordener = mangler_df.get_column("Orden").fill_null("—").to_list()

        # Build table - use zip() to iterate corresponding elements
        table = Table(title="Arter som mangler norsk navn")
        table.add_column("Art (latinsk)", style="cyan")
        table.add_column("Familie", style="green")
        table.add_column("Orden", style="magenta")
        for art, familie, orden in zip(
            arter, familier, ordener
        ):  # zipper sammen listene til en tuple pr art, familie, orden. Slik at alle med posisjon 1 i hver list blir en tuple, osv. Loopen kan da hente art, familie , orden (dvs. posisjon 1, 2 og 3 i hver tuple og det blir argumentet om a legge til en rad med disse verdiene)
            table.add_row(art, familie, orden)
        console.print(table)
        console.print(f"\n[bold]Fant {mangler_df.height} arter uten norsk navn.[/bold]")
        console.print("Skriv inn norsk navn for hver art:\n")

        # Collect names - iterate using zip on columns
        navn_mapping = {}  # lager en dictionary som fylles av for loopen under
        for art in arter:
            navn = Prompt.ask(f"  [cyan]{art}[/cyan]")
            if (
                not navn.strip()
            ):  # strip er å ta bort alle whitespacses, etc. Sånn at du kun evaluerer om det faktisk er tomt
                console.print(f"\n[bold red]Feil:[/bold red] Du må skrive inn navn for {art}. Avbryter.")
                raise typer.Exit(code=1)
            navn_mapping[art] = (
                navn.strip()
            )  # navn_mapping is a dictionary. The square bracket syntax [art] is how you access a specific slot in that dictionary by key. The variable art holds a string like "Sylvia borin", so this means "the slot in navn_mapping whose key is "Sylvia borin"." Slik at du ber programmet skrive over "value" i key:value nøkkelen til dictionary for den gitte arten (i.e key). Slik python funker så legges også art (i.e. the key) til når python ser at denne mangler i dictionarien når den skal assigne en value (navn) til den gitte keyen.
        return navn_mapping

    return (prompt_mangler_navn,)


@app.cell(hide_code=True)
def _(prompt_mangler_navn):
    def test_prompt_mangler_navn():
        """Test prompt_mangler_navn with mocked user input."""

        mangler_df = pl.DataFrame(
            {
                "Art": ["Vulpes lagopus", "Canis lupus"],
                "Navn": [None, None],
                "Familie": ["Canidae", "Canidae"],
                "Orden": ["Carnivora", "Carnivora"],
            }
        )

        # Empty DataFrame → returns empty dict, Nei prompts
        empty_df = pl.DataFrame(
            {"Art": [], "Navn": [], "Familie": [], "Orden": []},
            schema={
                "Art": pl.Utf8,
                "Navn": pl.Utf8,
                "Familie": pl.Utf8,
                "Orden": pl.Utf8,
            },
        )
        assert prompt_mangler_navn(empty_df) == {}, "Tom DataFrame skal gi tom dict"

        # Happy path: user provides valid names
        with patch.object(
            Prompt, "ask", side_effect=["Fjellrev", "Ulv"]
        ):  # patch.object er fra unittests og lar deg mocke den menneskelige inputen som rich ber om i terminalen.
            result = prompt_mangler_navn(mangler_df)

        assert result == {
            "Vulpes lagopus": "Fjellrev",
            "Canis lupus": "Ulv",
        }, f"Forventet riktig mapping, fikk {result}"

        # Blank input → should raise typer.Exit
        with patch.object(Prompt, "ask", side_effect=["Fjellrev", "   "]):
            with pytest.raises(typer.Exit):
                prompt_mangler_navn(mangler_df)

    return


@app.function(hide_code=True)
def join_navn_til_orginal_df(
    df: pl.DataFrame, navn_mapping: dict[str, str]
) -> pl.DataFrame:  # navn mapping er en dict med key:value som begge er str. hvor f.eks. det latinske navnet er "key" og det norske er "navn"
    """Oppdater Navn-kolonnen med manuelt oppgitte navn."""

    if not navn_mapping:
        return df

    mapping_df = pl.DataFrame(
        {"Art": list(navn_mapping.keys()), "Navn_ny": list(navn_mapping.values())}
    )  # dette lager en ny poalrs df med to kolonner art og navn_ny hvor du henter navn mapping fra "prompt_mangler navn" og henter ut deres keys og values. Keys og values argumentene er "They're iterator methods that traverse the entire dictionary collection." så trenger ikke iter rows.
    #  e.g. Polars knows how to build a column from a list of strings. It doesn't know how to build one from a dict_keys view.

    df_med_navn = (
        df.join(mapping_df, on="Art", how="left")
        .with_columns(
            pl.when(pl.col("Navn").is_null() & pl.col("Navn_ny").is_not_null())
            .then(pl.col("Navn_ny"))
            .otherwise(pl.col("Navn"))
            .alias("Navn")
        )
        .drop("Navn_ny")
    )

    return df_med_navn


@app.function(hide_code=True)
def test_join_navn_til_orginal_df():
    """Test that join_navn_til_orginal_df fills in missing names correctly."""

    df = pl.DataFrame(
        {
            "Art": [
                "Bubo bubo",
                "Passer domesticus",
                "Vulpes lagopus",
                "Canis lupus",
                "Anser erythropus",
            ],
            "Navn": [
                "Hubro",  # already has a name
                "Gråspurv",  # already has a name
                None,  # missing — should be filled
                None,  # missing — should be filled
                None,  # missing — but NOT in mapping
            ],
            "Familie": ["Strigidae", "Passeridae", "Canidae", "Canidae", "Anatidae"],
        }
    )

    mapping = {
        "Vulpes lagopus": "Fjellrev",
        "Canis lupus": "Ulv",
    }

    # Null names are filled from the mapping ─────────────────────
    result = join_navn_til_orginal_df(df, mapping)

    fjellrev = result.filter(pl.col("Art") == "Vulpes lagopus")
    assert fjellrev.get_column("Navn").to_list() == ["Fjellrev"], "Vulpes lagopus skal få navnet Fjellrev"
    # Må bruke to list, get_column returnerer kun er polars series som ikke er det samme som en list

    ulv = result.filter(pl.col("Art") == "Canis lupus")
    assert ulv.get_column("Navn").to_list() == ["Ulv"], "Canis lupus skal få navnet Ulv"

    # Existing names are NOT overwritten ─────────────────────────
    hubro = result.filter(pl.col("Art") == "Bubo bubo")
    assert hubro.get_column("Navn").to_list() == ["Hubro"], "Bubo bubo har allerede navn — skal ikke overskrives"

    graaspurv = result.filter(pl.col("Art") == "Passer domesticus")
    assert graaspurv.get_column("Navn").to_list() == ["Gråspurv"], (
        "Passer domesticus har allerede navn — skal ikke overskrives"
    )

    # Null names NOT in mapping stay null ────────────────────────
    dverggaas = result.filter(pl.col("Art") == "Anser erythropus")
    assert dverggaas.get_column("Navn").to_list() == [None], "Anser erythropus er ikke i mapping — skal forbli null"

    # Row count unchanged (left join, Nei extra rows) ────────────
    assert result.height == df.height, f"Radantall skal være uendret: forventet {df.height}, fikk {result.height}"

    # Temporary column Navn_ny is dropped ────────────────────────
    assert "Navn_ny" not in result.columns, "Hjelpkolonnen Navn_ny skal være fjernet"

    # Empty mapping → nothing changes ────────────────────────────
    result_empty = join_navn_til_orginal_df(df, {})
    assert result_empty.get_column("Navn").to_list() == df.get_column("Navn").to_list(), (
        "Tom mapping skal ikke endre noe"
    )


@app.cell(column=1, hide_code=True)
def _():
    mo.md(r"""
    ### Setter sammen alt og definerer rich
    """)
    return


@app.cell
def _(console, les_data_og_kjør_alle_funksjoner, prompt_mangler_navn):
    cli_app = typer.Typer()


    @cli_app.command()
    def les_data_cli(
        input_fil_sti: str = typer.Argument(..., help="Sti til CSV-fil med fugledata"),
        filter_year: int = typer.Option(1990, help="Filtrer observasjoner fra og med dette året"),
        output: str = typer.Option("output.parquet", help="Sti til utfil (Parquet)"),
    ):
        """Les fugledata fra CSV, berik med artsdatabanken, og skriv resultatet til fil."""
        console.print(Rule("[bold]Fugl Artsdataanalyser[/bold]"))
        console.print(f"  Inputfil:    [cyan]{input_fil_sti}[/cyan]")
        console.print(f"  Filter år:   [cyan]{filter_year}[/cyan]")
        console.print(f"  Utfil:       [cyan]{output}[/cyan]\n")

        console.print(Rule("[bold]Henter og beriker data[/bold]"))
        df = les_data_og_kjør_alle_funksjoner(input_fil_sti, filter_year)

        # Sjekk for manglende navn
        console.print(Rule("[bold]Sjekker artsnavn[/bold]"))
        mangler_df = finn_mangler_navn(df)
        if not mangler_df.is_empty():
            navn_mapping = prompt_mangler_navn(mangler_df)
            df = join_navn_til_orginal_df(df, navn_mapping)
        else:
            console.print("  [green]✓[/green] Alle arter har norsk navn")

        console.print(Rule("[bold]Skriver resultat[/bold]"))
        df.write_parquet(output)

        # Oppsummering
        unike_arter = df.select("Art").n_unique() if "Art" in df.columns else "—"
        antall_ikke_treff_anf = (
            df.filter(pl.col("Art av nasjonal forvaltningsinteresse") == "Treff ikke funnet").height
            if "Art av nasjonal forvaltningsinteresse" in df.columns
            else "—"
        )
        summary = (
            f"[bold green]Ferdig![/bold green]\n\n"
            f"  Rader:                                      [cyan]{df.height}[/cyan]\n"
            f"  Kolonner:                                   [cyan]{df.width}[/cyan]\n"
            f"  Unike arter:                                [cyan]{unike_arter}[/cyan]\n"
            f'  Antall "Ikke treff" på ANF-tabbell fra Mdir: [cyan]{antall_ikke_treff_anf}[/cyan]\n'
            f"  Skrevet til:                                [cyan]{output}[/cyan]"
        )
        console.print(Panel(summary, title="Oppsummering", border_style="green"))


    # Kjør Typer CLI kun i script-modus (uv run Databehandling.py -- ...)
    if mo.app_meta().mode == "script":
        cli_app()
    return


@app.cell
def _(add_national_interest_criteria, console, process_and_enrich_data):
    def les_data_og_kjør_alle_funksjoner(input_fil_sti: str, filter_year: int = 1990) -> pl.DataFrame:
        """Les CSV med DuckDB, filtrer på år, og kjør alle berikingsfunksjoner."""

        # Bruker DuckDB direkte for å lese CSV — unngår polars sin ragged-lines feil
        with console.status("[bold blue]Leser CSV-fil med DuckDB..."):
            input_df = duckdb.sql(f"SELECT * FROM read_csv('{input_fil_sti}')").pl()
        validate_artskart_input_contract(input_df)

        with console.status("[bold blue]Filtrerer observasjoner på år..."):
            # OBS: Observasjoner uten dato (null) filtreres også bort her.
            # null >= date returnerer null, som .filter() behandler som False.
            null_count = input_df.select(pl.col("dateTimeCollected").is_null().sum()).item()
            input_filtrert_df = input_df.with_columns(pl.col("dateTimeCollected").dt.date()).filter(
                pl.col("dateTimeCollected") >= date(filter_year, 1, 1)
            )
        if null_count > 0:
            console.print(f"  [yellow]Advarsel:[/yellow] {null_count} observasjoner uten dato fjernet")
        console.print(f"  [dim]Filtrert til {input_filtrert_df.height} rader (fra og med {filter_year})[/dim]")

        # Kjører alle berikingsfunksjonene — progress_bar håndteres inne i process_and_enrich_data
        df_artsdatabanken = process_and_enrich_data(input_filtrert_df)

        with console.status("[bold blue]Legger til kriterier for nasjonal interesse..."):
            df_steg1 = df_artsdatabanken.pipe(add_national_interest_criteria)
        console.print("  [green]✓[/green] Lagt til Arter av nasjonal forvaltningsinteresse")

        with console.status("[bold blue]Legger til kolonne for arter av nasjonal forvaltningsinteresse..."):
            df_steg2 = df_steg1.pipe(legg_til_kolonne_arteravnasjonal)
        console.print("  [green]✓[/green] Arter av nasjonal forvaltningsinteresse summert til en kolonne")

        with console.status("[bold blue]Rydder opp i navn og datatyper..."):
            df_alle_funksjoner = df_steg2.pipe(rydd_navn_og_datatyper)
        console.print("  [green]✓[/green] Ryddet navn, kolonner og datatyper")

        return df_alle_funksjoner

    return (les_data_og_kjør_alle_funksjoner,)


if __name__ == "__main__":
    app.run()
