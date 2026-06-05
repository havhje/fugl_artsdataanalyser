import marimo

__generated_with = "0.23.8"
app = marimo.App(width="columns")

with app.setup(hide_code=True):
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
def vis_todo_liste():
    todo_liste = mo.callout(
        mo.md(
            r"""
    ### Todo

    - Lage en egen md celle med alle testscenarioer for hver enkelt funksjon? 

    - Er testene for gira mot slik koden er nå? Altså er de genrelle nok og fanger de opp intensjonen i funksjonene?
    - Husk at du kan skrive pl.col.kolonnenavn, du trenger ikke å skrive pl.col("kolonnenavn"). Bruk denne fremover er bedre å lese

    - Sjekk med ny Ki modell når det kommer at alt er korrekt

    """
        ),
        kind="info",
    )

    todo_liste
    return


@app.cell
def koble_til_fugldatabase():
    DATABASE_URL = "databehandling/fugl_atributt_data"
    bird_data = duckdb.connect(DATABASE_URL, read_only=True)
    return (bird_data,)


@app.cell
def opprett_console():
    console = Console()
    return (console,)


@app.cell(hide_code=True)
def md_artskart_inputkontrakt():
    mo.md(r"""
    ### Henter tillatte kolonner fra artskart
    """)
    return


@app.function(hide_code=True)
def get_required_artskart_columns() -> set[str]:
    """Returner Artskart-kolonnene som inngår i input-kontrakten."""
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
    """Returner tillatte rødliste- og fremmedartskategorier."""
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
    """Valider at Artskart-data oppfyller input-kontrakten.

    Args:
        df: Artskart-data før beriking.

    Raises:
        ValueError: Når obligatoriske kolonner mangler eller `category` har
            verdier utenfor tillatt domene.
    """
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
def md_nortaxa_api():
    mo.md(r"""
    ### API fra artsdatabanken
    """)
    return


@app.cell(hide_code=True)
def definer_nortaxa_konstanter():
    # Constants
    NORTAXA_API_BASE_URL = "https://nortaxa.artsdatabanken.no/api/v1/TaxonName"
    DESIRED_RANKS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus"]
    RATE_LIMIT_DELAY = 0.1  # seconds between API calls (adjust as needed)
    return DESIRED_RANKS, NORTAXA_API_BASE_URL, RATE_LIMIT_DELAY


@app.cell(hide_code=True)
def definer_nortaxa_hjelpefunksjoner(DESIRED_RANKS, NORTAXA_API_BASE_URL):
    @lru_cache(maxsize=10000)
    def fetch_taxon_data(scientific_name_id: int) -> dict[str, Any] | None:
        """Hent taksondata fra NorTaxa for én vitenskapelig navne-ID.

        Args:
            scientific_name_id: `validScientificNameId` fra Artskart.

        Returns:
            API-svar som dict når oppslaget lykkes, ellers None.

        Notes:
            Resultatene mellomlagres med `lru_cache` for å unngå dupliserte
            API-kall. Nettverksfeil fanges og gir None.
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
        """Trekk ut taksonomisk hierarki og familie-/orden-ID-er fra NorTaxa.

        Args:
            api_data: Dekodet JSON-respons fra NorTaxa, eller None.

        Returns:
            Tuple med hierarki per rang, familie-ID og orden-ID. Manglende verdier
            settes til None.
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
        """Finn norsk navn fra NorTaxa-responsen.

        Args:
            api_data: Dekodet JSON-respons fra NorTaxa, eller None.

        Returns:
            Bokmålsnavn når tilgjengelig, ellers nynorsknavn eller None.
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
def definer_process_and_enrich_data(
    DESIRED_RANKS,
    RATE_LIMIT_DELAY,
    console,
    extract_hierarchy_and_ids,
    fetch_taxon_data,
    get_norwegian_name,
):
    def process_and_enrich_data(source_df: pl.DataFrame) -> pl.DataFrame:
        """Berik Artskart-data med taksonomi og norske familie-/ordennavn.

        Args:
            source_df: DataFrame med kolonnen `validScientificNameId`.

        Returns:
            DataFrame med originalradene, taksonomikolonner for ønskede ranger,
            `FamilieNavn` og `OrdenNavn`.

        Raises:
            ValueError: Når ID-kolonnen mangler eller ingen gyldige ID-er finnes.
            RuntimeError: Når ett eller flere NorTaxa-oppslag feiler eller gir
                tomt svar.

        Notes:
            Funksjonen gjør nettverkskall og skriver fremdrift/advarsler til
            konsollen.
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
def md_testmatrise_process_and_enrich_data():
    mo.md(r"""
    ### Testmatrise: `process_and_enrich_data`

    **Tiltenkt oppførsel:** Funksjonen skal berike Artskart-rader med taksonomisk hierarki fra NorTaxa og norske familie-/ordennavn, uten å miste eller duplisere originalrader.

    **Kilde til sannhet:** Brukergodkjent matrise 2026-06-05, funksjonskontrakt/docstring og ekte NorTaxa API for kjente arts-ID-er.

    **Kjøringstype:** Integrasjonstest mot ekte NorTaxa API. Testen er mer realistisk, men kan feile ved nettverksproblemer eller endringer i ekstern taksonomidata. Live-testcellene som krever eksakte NorTaxa-verdier hopper over kjøring dersom `NORTAXA_API_BASE_URL` peker på `mock://...`.

    **Inputkontrakt:** `source_df` er en Polars- eller pandas-DataFrame med kolonnen `validScientificNameId`. Gyldige ID-er må kunne konverteres til `int`; `None`/ugyldige ID-er skal hoppes over, men radene beholdes hvis minst én gyldig ID finnes.

    **Outputkontrakt:** Returnerer `pl.DataFrame` med alle originalrader og originalkolonner bevart, pluss `Kingdom`, `Phylum`, `Class`, `Order`, `Family`, `Genus`, `FamilieNavn` og `OrdenNavn`. Radantallet skal være likt input. Berikingskolonner for rader med null/ugyldig ID skal være null.

    **Godkjenningsstatus:** Godkjent av bruker 2026-06-05.

    **Revisjonspolicy:** Hvis forventet oppførsel endres, oppdater og godkjenn denne matrisen på nytt før testene endres.

    | ID | Scenario | Input | Forventet output/invariant | Toleranse | Hvorfor det betyr noe | Feilmodus testen beskytter mot | Testcelle |
    |---|---|---|---|---|---|---|---|
    | MTM-001 | Happy path med fire kjente fuglearter | `validScientificNameId`: 4382, 204586, 3677, 295741 | Rader beholdes. Forventede verdier: granmeis = `Passeriformes`/`Paridae`/`Poecile`/`meisefamilien`/`spurvefugler`; skjeand = `Anseriformes`/`Anatidae`/`Spatula`/`andefamilien`/`andefugler`; gråmåke = `Charadriiformes`/`Laridae`/`Larus`/`måkefamilien`/`vade-, måke- og alkefugler`; hønsehauk = `Accipitriformes`/`Accipitridae`/`Astur`/`haukefamilien`/`haukefugler`. | Eksakt tekstlikhet | Bekrefter kjerneberikingen mot reelle NorTaxa-data | Feil mapping, manglende navneoppslag eller feil join | `test_process_and_enrich_data_mtm_001` |
    | MTM-002 | Duplikate ID-er | Samme gyldige ID finnes i flere originalrader | Alle originalrader beholdes, og duplikatradene får like berikingsverdier | Eksakt tekstlikhet og radantall | Artskart kan inneholde mange observasjoner av samme art | Join som mister/dupliserer observasjoner eller gir inkonsistent beriking | `test_process_and_enrich_data_mtm_002` |
    | MTM-003 | Null-ID blandet med gyldig ID | Én gyldig ID og én `None` | Gyldig rad berikes; nullrad beholdes med null i alle berikingskolonner | Eksakt null-/radantallssjekk | Reelle data kan ha manglende ID-er | Nullrader droppes eller gir uønsket API-kall/join-feil | `test_process_and_enrich_data_mtm_003` |
    | MTM-004 | Manglende ID-kolonne | DataFrame uten `validScientificNameId` | `ValueError` med melding om manglende kolonne | Exception-type og tekstutdrag | Inputkontrakten skal feile tidlig og tydelig | Utydelige `ColumnNotFoundError` senere i løpet | `test_process_and_enrich_data_mtm_004` |
    | MTM-005 | NorTaxa-oppslag feiler/tomt svar | En gyldig tallverdi som ikke finnes i NorTaxa, brukt som integrasjonssjekk | `RuntimeError` som inkluderer ID-en som feilet | Exception-type og tekstutdrag | Pipeline skal ikke silently levere ufullstendig taksonomi | Tomme API-svar gir manglende kolonner eller skjulte hull i data | `test_process_and_enrich_data_mtm_005` |
    """)
    return


@app.cell(hide_code=True)
def test_process_and_enrich_data_mtm_001(
    NORTAXA_API_BASE_URL,
    process_and_enrich_data,
):
    def test_process_and_enrich_data_mtm_001():
        """MTM-001: kjente fuglearter får forventet NorTaxa-beriking."""
        if NORTAXA_API_BASE_URL.startswith("mock://"):
            return

        enrichment_columns = [
            "Kingdom",
            "Phylum",
            "Class",
            "Order",
            "Family",
            "Genus",
            "FamilieNavn",
            "OrdenNavn",
        ]
        expected_enrichment = {
            4382: {
                "Kingdom": "Animalia",
                "Phylum": "Chordata",
                "Class": "Aves",
                "Order": "Passeriformes",
                "Family": "Paridae",
                "Genus": "Poecile",
                "FamilieNavn": "meisefamilien",
                "OrdenNavn": "spurvefugler",
            },
            204586: {
                "Kingdom": "Animalia",
                "Phylum": "Chordata",
                "Class": "Aves",
                "Order": "Anseriformes",
                "Family": "Anatidae",
                "Genus": "Spatula",
                "FamilieNavn": "andefamilien",
                "OrdenNavn": "andefugler",
            },
            3677: {
                "Kingdom": "Animalia",
                "Phylum": "Chordata",
                "Class": "Aves",
                "Order": "Charadriiformes",
                "Family": "Laridae",
                "Genus": "Larus",
                "FamilieNavn": "måkefamilien",
                "OrdenNavn": "vade-, måke- og alkefugler",
            },
            295741: {
                "Kingdom": "Animalia",
                "Phylum": "Chordata",
                "Class": "Aves",
                "Order": "Accipitriformes",
                "Family": "Accipitridae",
                "Genus": "Astur",
                "FamilieNavn": "haukefamilien",
                "OrdenNavn": "haukefugler",
            },
        }

        test_df = pl.DataFrame(
            {
                "validScientificNameId": list(expected_enrichment),
                "observasjon_id": ["granmeis", "skjeand", "graamake", "honsehauk"],
            }
        )
        result = process_and_enrich_data(test_df)

        assert isinstance(result, pl.DataFrame), "MTM-001 skal returnere Polars DataFrame"
        assert result.height == test_df.height, "MTM-001 radantall skal beholdes"
        assert set(enrichment_columns) <= set(result.columns), "MTM-001 mangler én eller flere berikingskolonner"
        assert result.get_column("observasjon_id").to_list() == test_df.get_column("observasjon_id").to_list(), (
            "MTM-001 originalkolonner/-rekkefølge skal beholdes"
        )

        for species_id, expected_values in expected_enrichment.items():
            row = result.filter(pl.col.validScientificNameId == species_id)
            assert row.height == 1, f"MTM-001 forventet én rad for ID {species_id}, fikk {row.height}"
            for column, expected_value in expected_values.items():
                actual_value = row.get_column(column).item()
                assert actual_value == expected_value, (
                    f"MTM-001 ID {species_id}: {column} skal være {expected_value!r}, fikk {actual_value!r}"
                )


    test_process_and_enrich_data_mtm_001()
    return


@app.cell(hide_code=True)
def test_process_and_enrich_data_mtm_002(
    NORTAXA_API_BASE_URL,
    process_and_enrich_data,
):
    def test_process_and_enrich_data_mtm_002():
        """MTM-002: duplikate arts-ID-er beholder alle originalrader."""
        if NORTAXA_API_BASE_URL.startswith("mock://"):
            return

        enrichment_columns = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "FamilieNavn", "OrdenNavn"]
        expected_granmeis = {
            "Kingdom": "Animalia",
            "Phylum": "Chordata",
            "Class": "Aves",
            "Order": "Passeriformes",
            "Family": "Paridae",
            "Genus": "Poecile",
            "FamilieNavn": "meisefamilien",
            "OrdenNavn": "spurvefugler",
        }

        duplicate_df = pl.DataFrame(
            {
                "validScientificNameId": [4382, 4382, 204586],
                "observasjon_id": ["granmeis-1", "granmeis-2", "skjeand-1"],
            }
        )
        result = process_and_enrich_data(duplicate_df)
        duplicate_rows = result.filter(pl.col.validScientificNameId == 4382).sort("observasjon_id")

        assert result.height == duplicate_df.height, "MTM-002 duplikater skal ikke endre radantall"
        assert result.get_column("observasjon_id").to_list() == duplicate_df.get_column("observasjon_id").to_list(), (
            "MTM-002 original radrekkefølge skal beholdes"
        )
        assert duplicate_rows.height == 2, "MTM-002 forventet to rader for duplikat-ID 4382"
        assert duplicate_rows.get_column("observasjon_id").to_list() == ["granmeis-1", "granmeis-2"], (
            "MTM-002 originale duplikatrader skal beholdes"
        )
        for column in enrichment_columns:
            assert duplicate_rows.get_column(column).to_list() == [expected_granmeis[column], expected_granmeis[column]], (
                f"MTM-002 duplikat-ID 4382 skal ha lik verdi i {column}"
            )


    test_process_and_enrich_data_mtm_002()
    return


@app.cell(hide_code=True)
def test_process_and_enrich_data_mtm_003(process_and_enrich_data):
    def test_process_and_enrich_data_mtm_003():
        """MTM-003: null-ID blandet med gyldig ID beholdes uten beriking."""
        enrichment_columns = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "FamilieNavn", "OrdenNavn"]

        nullable_df = pl.DataFrame(
            {
                "validScientificNameId": [4382, None],
                "observasjon_id": ["gyldig", "mangler_id"],
            }
        )
        result = process_and_enrich_data(nullable_df)
        valid_row = result.filter(pl.col.validScientificNameId == 4382)
        null_row = result.filter(pl.col.validScientificNameId.is_null())

        assert result.height == nullable_df.height, "MTM-003 null-ID skal ikke droppes"
        assert valid_row.height == 1, "MTM-003 gyldig rad skal beholdes"
        assert null_row.height == 1, "MTM-003 forventet én rad med null-ID"
        assert null_row.get_column("observasjon_id").item() == "mangler_id", "MTM-003 original nullrad skal beholdes"
        for column in enrichment_columns:
            assert valid_row.get_column(column).null_count() == 0, f"MTM-003 gyldig rad mangler {column}"
            assert null_row.get_column(column).to_list() == [None], (
                f"MTM-003 null-ID skal ha null i berikingskolonnen {column}"
            )


    test_process_and_enrich_data_mtm_003()
    return


@app.cell(hide_code=True)
def test_process_and_enrich_data_mtm_004(process_and_enrich_data):
    def test_process_and_enrich_data_mtm_004():
        """MTM-004: manglende validScientificNameId feiler tydelig."""
        with pytest.raises(ValueError, match="validScientificNameId"):
            process_and_enrich_data(pl.DataFrame({"annenKolonne": [4382]}))


    test_process_and_enrich_data_mtm_004()
    return


@app.cell(hide_code=True)
def test_process_and_enrich_data_mtm_005(
    NORTAXA_API_BASE_URL,
    process_and_enrich_data,
):
    def test_process_and_enrich_data_mtm_005():
        """MTM-005: tomt NorTaxa-svar gir RuntimeError med ID i meldingen."""
        if NORTAXA_API_BASE_URL.startswith("mock://"):
            return

        ikke_eksisterende_id = 999_999_999
        with pytest.raises(RuntimeError, match=str(ikke_eksisterende_id)):
            process_and_enrich_data(pl.DataFrame({"validScientificNameId": [ikke_eksisterende_id]}))


    test_process_and_enrich_data_mtm_005()
    return


@app.cell(hide_code=True)
def md_legg_til_verdi_m1941():
    mo.md(r"""
    ### Legg til verdi M1941
    """)
    return


@app.function(hide_code=True)
def legg_til_verdi_m1941(df: pl.DataFrame) -> pl.DataFrame:
    """Legg til foreløpig M1941-verdi basert på Artskart-kategori.

    Args:
        df: DataFrame med `category`.

    Returns:
        DataFrame med ny kolonne `verdi_rodliste_artskart`.

    Notes:
        Rødlisteverdien brukes som fallback når ANF/Mdir-tabellen ikke gir
        `verdi_m1941_nasjonal`. EN/CR gir "Svært stor verdi", VU gir
        "Stor verdi", NT gir "Middels verdi", LC gir "Noe verdi" og øvrige
        tillatte kategorier gir "Ingen".
    """
    ubetydelig_verdi = ["RE", "DD", "SE", "HI", "PH", "LO", "NK", "NA", "NE", "Unknown"]

    return df.with_columns(
        pl.when(pl.col("category").is_in(["EN", "CR"]))
        .then(pl.lit("Svært stor verdi"))
        .when(pl.col("category") == "VU")
        .then(pl.lit("Stor verdi"))
        .when(pl.col("category") == "NT")
        .then(pl.lit("Middels verdi"))
        .when(pl.col("category") == "LC")
        .then(pl.lit("Noe verdi"))
        .when(pl.col("category").is_in(ubetydelig_verdi))
        .then(pl.lit("Ingen"))
        .otherwise(None)
        .alias("verdi_rodliste_artskart")
    )


@app.cell(hide_code=True)
def md_testmatrise_legg_til_verdi_m1941():
    mo.md(r"""
    ### Testmatrise: `legg_til_verdi_m1941`

    **Tiltenkt oppførsel:** Funksjonen skal legge til foreløpig M1941-verdi fra Artskart-kolonnen `category`, uten å endre eksisterende rader eller kolonner.

    **Kilde til sannhet:** Brukergodkjent matrise 2026-06-05, funksjonsdocstring, `get_allowed_categories()` og Artskart inputkontrakt.

    **Inputkontrakt:** `df` er en `pl.DataFrame` med kolonnen `category`. Verdier som kommer gjennom ordinær pipeline skal være i det tillatte domenet fra `get_allowed_categories()`. Ved direkte kall kan `None` forekomme og skal gi null i ny verdikolonne.

    **Outputkontrakt:** Returnerer `pl.DataFrame` med alle eksisterende rader og kolonner bevart, pluss `verdi_rodliste_artskart`. Radantall og radrekkefølge skal beholdes.

    **Godkjenningsstatus:** Godkjent av bruker 2026-06-05.

    **Revisjonspolicy:** Hvis forventet oppførsel endres, oppdater og godkjenn denne matrisen på nytt før testene endres.

    | ID | Scenario | Input | Forventet output/invariant | Toleranse | Hvorfor det betyr noe | Feilmodus testen beskytter mot | Testcelle |
    |---|---|---|---|---|---|---|---|
    | MTM-001 | Alle tillatte kategorier mappes riktig | `category`: `CR`, `EN`, `VU`, `NT`, `LC`, `RE`, `DD`, `SE`, `HI`, `PH`, `LO`, `NK`, `NA`, `NE`, `Unknown` | `CR`/`EN` → `Svært stor verdi`; `VU` → `Stor verdi`; `NT` → `Middels verdi`; `LC` → `Noe verdi`; øvrige tillatte → `Ingen` | Eksakt tekstlikhet | Bekrefter kjerne-mappingen som brukes som fallback for M1941 | Feil kategori-mapping eller uteglemt tillatt kategori | `MTM_001` |
    | MTM-002 | Originale rader og kolonner bevares | DataFrame med `category`, `art`, `rad_nr` og duplikate kategorier | Samme radantall, samme radrekkefølge, eksisterende kolonner uendret, ny kolonne lagt til | Eksakt DataFrame-sammenligning | Funksjonen skal kun berike, ikke filtrere eller sortere | Rader droppes/dupliseres, rekkefølge endres eller eksisterende data overskrives | `MTM_002` |
    | MTM-003 | Tom input med riktig schema | Tom `pl.DataFrame` med `category: Utf8` | Returnerer tom `pl.DataFrame` med ny kolonne `verdi_rodliste_artskart` | Eksakt radantall og kolonnesjekk | Pipeline-steg bør tåle tomme datasett etter filtrering | Tom input gir crash eller manglende outputkolonne | `MTM_003` |
    | MTM-004 | Null-kategori ved direkte kall | `category`: `[None]` med dtype `Utf8` | `verdi_rodliste_artskart` blir `None`; ordinær pipeline-validering skal normalt stoppe null-kategorier tidligere | Eksakt null-sjekk | Dokumenterer direkte funksjonsoppførsel uten å svekke inputkontrakten | Null mapper feilaktig til `Ingen` eller tekstverdi | `MTM_004` |
    | MTM-005 | Manglende `category`-kolonne | DataFrame uten `category` | Funksjonen feiler med exception som nevner `category` | Exception og tekstutdrag | Manglende obligatorisk input skal oppdages tydelig | Skjult feil, utydelig feilmelding eller stille feiloutput | `MTM_005` |
    """)
    return


@app.cell(hide_code=True)
def MTM_001():
    def test_legg_til_verdi_m1941_mtm_001():
        """MTM-001: alle tillatte kategorier mappes til forventet M1941-verdi."""
        category_mapping = {
            "CR": "Svært stor verdi",
            "EN": "Svært stor verdi",
            "VU": "Stor verdi",
            "NT": "Middels verdi",
            "LC": "Noe verdi",
            "RE": "Ingen",
            "DD": "Ingen",
            "SE": "Ingen",
            "HI": "Ingen",
            "PH": "Ingen",
            "LO": "Ingen",
            "NK": "Ingen",
            "NA": "Ingen",
            "NE": "Ingen",
            "Unknown": "Ingen",
        }
        categories = list(category_mapping)
        test_df = pl.DataFrame({"category": categories})

        result = legg_til_verdi_m1941(test_df)

        assert set(categories) == get_allowed_categories(), "MTM-001 testen skal dekke alle tillatte kategorier"
        assert result.get_column("verdi_rodliste_artskart").to_list() == list(category_mapping.values()), (
            "MTM-001 rødlistekategori skal mappes til riktig M1941-verdi"
        )


    test_legg_til_verdi_m1941_mtm_001()
    return


@app.cell(hide_code=True)
def MTM_002():
    def test_legg_til_verdi_m1941_mtm_002():
        """MTM-002: originale rader, rekkefølge og kolonner bevares."""
        from polars.testing import assert_frame_equal

        test_df = pl.DataFrame(
            {
                "category": ["LC", "EN", "LC", "NT"],
                "art": ["livskraftig art", "sterkt truet art", "duplikat LC", "nær truet art"],
                "rad_nr": [1, 2, 3, 4],
            }
        )

        result = legg_til_verdi_m1941(test_df)

        assert result.height == test_df.height, "MTM-002 radantall skal beholdes"
        assert result.columns == [*test_df.columns, "verdi_rodliste_artskart"], (
            "MTM-002 eksisterende kolonner skal beholdes og ny kolonne legges til sist"
        )
        assert_frame_equal(result.select(test_df.columns), test_df)
        assert result.get_column("verdi_rodliste_artskart").to_list() == [
            "Noe verdi",
            "Svært stor verdi",
            "Noe verdi",
            "Middels verdi",
        ], "MTM-002 verdier skal følge original radrekkefølge"


    test_legg_til_verdi_m1941_mtm_002()
    return


@app.cell(hide_code=True)
def MTM_003():
    def test_legg_til_verdi_m1941_mtm_003():
        """MTM-003: tom input med riktig schema gir tom output med verdikolonne."""
        test_df = pl.DataFrame({"category": pl.Series("category", [], dtype=pl.Utf8)})

        result = legg_til_verdi_m1941(test_df)

        assert isinstance(result, pl.DataFrame), "MTM-003 skal returnere Polars DataFrame"
        assert result.height == 0, "MTM-003 tom input skal gi tom output"
        assert result.columns == ["category", "verdi_rodliste_artskart"], "MTM-003 verdikolonnen skal finnes"
        assert result.schema["category"] == pl.Utf8, "MTM-003 category-schema skal beholdes"
        assert result.schema["verdi_rodliste_artskart"] == pl.Utf8, (
            "MTM-003 verdikolonnen skal ha teksttype også når DataFrame er tom"
        )


    test_legg_til_verdi_m1941_mtm_003()
    return


@app.cell(hide_code=True)
def MTM_004():
    def test_legg_til_verdi_m1941_mtm_004():
        """MTM-004: null-kategori ved direkte kall gir null verdi."""
        test_df = pl.DataFrame(
            {
                "category": pl.Series("category", [None], dtype=pl.Utf8),
                "art": ["mangler kategori"],
            }
        )

        result = legg_til_verdi_m1941(test_df)

        assert result.height == 1, "MTM-004 nullrad skal beholdes"
        assert result.get_column("art").to_list() == ["mangler kategori"], "MTM-004 originalkolonne skal beholdes"
        assert result.get_column("verdi_rodliste_artskart").to_list() == [None], (
            "MTM-004 null-kategori skal gi null, ikke 'Ingen' eller annen tekstverdi"
        )


    test_legg_til_verdi_m1941_mtm_004()
    return


@app.cell(hide_code=True)
def MTM_005():
    def test_legg_til_verdi_m1941_mtm_005():
        """MTM-005: manglende category-kolonne feiler tydelig."""
        test_df = pl.DataFrame({"art": ["mangler category"]})

        with pytest.raises(Exception) as exc_info:
            legg_til_verdi_m1941(test_df)

        assert "category" in str(exc_info.value), "MTM-005 feilmeldingen skal nevne manglende category-kolonne"


    test_legg_til_verdi_m1941_mtm_005()
    return


@app.cell(hide_code=True)
def md_arter_av_nasjonal_forvaltningsinteresse():
    mo.md(r"""
    ### Arter av nasjonal forvaltningsinteresse
    """)
    return


@app.cell(hide_code=True)
def definer_anf_kriterier_og_m1941(bird_data):
    def legg_til_arter_av_nasjonal_forvaltningsinteresse(df_enriched: pl.DataFrame) -> pl.DataFrame:
        """Slå opp ANF/Mdir-kriterier og velg endelig M1941-verdi.

        Args:
            df_enriched: Beriket Artskart-data med `validScientificNameId`,
                `validScientificName` og `verdi_rodliste_artskart`.

        Returns:
            DataFrame med kriteriekolonner, `verdi_m1941_nasjonal` og
            `Verdi M1941`.

        Notes:
            Leser ANF/Mdir-tabellen fra `bird_data`. Oppslag skjer først på
            arts-ID og deretter på vitenskapelig navn. Nasjonal M1941-verdi
            prioriteres over rødlisteverdien.
        """

        # Last inn kriterier fra ANF/Mdir-tabellen
        df_arter_nf = bird_data.execute("SELECT * FROM arter_av_nasjonal_forvaltningsinteresse").pl()

        df_arter_nf_ryddet = df_arter_nf.select(
            [
                pl.col("vitenskapelig_navn_id").alias("arts_id_mdir"),
                pl.col("vitenskapelig_navn").alias("vitenskapelig_navn_mdir"),
                pl.col("forvaltningsverdi").cast(pl.Utf8).str.replace_all("-", "Ingen").alias("verdi_m1941_nasjonal"),
                pl.col("kriterium_prioriterte_arter").alias("Prioriterte arter"),
                pl.col("kriterium_fredete_arter").alias("Fredete arter"),
                pl.col("kriterium_andre_spesielt_hensynskrevende_arter").alias("Andre spesielt hensynskrevende arter"),
                pl.col("kriterium_spesielle_okologiske_former").alias("Spesielle økologiske former"),
                pl.col("kriterium_dd").alias("Datamangel"),
                pl.col("kriterium_hensynskrevende_arter").alias("Hensynskrevende arter"),
                pl.col("kriterium_ansvarsart").alias("Ansvarsarter"),
                pl.col("kriterium_fremmede_arter").alias("Fremmede arter"),
            ]
        ).with_columns(
            pl.col("verdi_m1941_nasjonal").str.replace_all("-", "Ingen")
        )  # fremmed arter har ingen verdi i rødlista skriver eksplisitt "ingen"

        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
        ]
        lookup_cols = criteria_cols + ["verdi_m1941_nasjonal"]

        criteria_data = df_arter_nf_ryddet.with_columns(
            *[pl.when(pl.col(c) == 1).then(pl.lit("Ja")).otherwise(pl.lit("Nei")).alias(c) for c in criteria_cols]
        )

        # Først match på arts-ID, deretter fallback på vitenskapelig navn.
        df_with_criteria = (
            df_enriched.join(
                criteria_data,
                left_on="validScientificNameId",
                right_on="arts_id_mdir",
                how="left",
            )
            .join(
                criteria_data,
                left_on="validScientificName",
                right_on="vitenskapelig_navn_mdir",
                how="left",
                suffix="_fallback",
            )
            .with_columns(*[pl.coalesce(pl.col(c), pl.col(f"{c}_fallback")).alias(c) for c in lookup_cols])
            .with_columns(
                *[pl.col(c).fill_null("Nei").alias(c) for c in criteria_cols]
            )  # altså de artene som ikke er ANF tabellen = Nei ikke ANF
            .with_columns(
                pl.coalesce(pl.col("verdi_m1941_nasjonal"), pl.col("verdi_rodliste_artskart")).alias(
                    "Verdi M1941"
                )  # tar først å bruker verdien i fra mdir tabellen og for de artene som ikke er der så bruker vi verdien som gitt av rødlista. coalece funker slik at den fyller null verdiene i den første kolonnen med den andre.
            )
            .drop([f"{c}_fallback" for c in lookup_cols])
            .drop("vitenskapelig_navn_mdir", "arts_id_mdir")
        )

        return df_with_criteria

    return (legg_til_arter_av_nasjonal_forvaltningsinteresse,)


@app.cell(hide_code=True)
def md_testmatrise_legg_til_arter_av_nasjonal_forvaltningsinteresse():
    mo.md(r"""
    ### Testmatrise: `legg_til_arter_av_nasjonal_forvaltningsinteresse`

    **Tiltenkt oppførsel:** Funksjonen skal slå opp arter i ANF/Mdir-tabellen først på `validScientificNameId`, deretter som fallback på `validScientificName`, og velge endelig `Verdi M1941` der ANF/Mdir-verdi prioriteres over rødlisteverdien fra `verdi_rodliste_artskart`.

    **Kilde til sannhet:** Brukergodkjent matrise 2026-06-05, funksjonsdocstring og lokal DuckDB-tabell `arter_av_nasjonal_forvaltningsinteresse`.

    **Inputkontrakt:** `df_enriched` er en `pl.DataFrame` med kolonnene `validScientificNameId`, `validScientificName` og `verdi_rodliste_artskart`. `validScientificNameId` forventes som tall/null; `validScientificName` og `verdi_rodliste_artskart` som tekst/null.

    **Outputkontrakt:** Returnerer `pl.DataFrame` med alle originalrader og originalkolonner bevart, pluss kriteriekolonnene `Prioriterte arter`, `Fredete arter`, `Andre spesielt hensynskrevende arter`, `Spesielle økologiske former`, `Datamangel`, `Hensynskrevende arter`, `Ansvarsarter`, `Fremmede arter`, samt `verdi_m1941_nasjonal` og `Verdi M1941`. Interne join-kolonner skal ikke være med i output.

    **Godkjenningsstatus:** Godkjent av bruker 2026-06-05; ANF-MTM-013 lagt til etter brukeravklaring samme dato.

    **Revisjonspolicy:** Hvis forventet oppførsel endres, oppdater og godkjenn denne matrisen på nytt før testene endres.

    | ID | Scenario | Input | Forventet output/invariant | Toleranse | Hvorfor det betyr noe | Feilmodus testen beskytter mot | Testcelle |
    |---|---|---|---|---|---|---|---|
    | ANF-MTM-001 | Kilde-/oppslagskontrakt | Hele `arter_av_nasjonal_forvaltningsinteresse` | Ikke-null ID-er og navn er unike; kriterier har bare `1`/null; `forvaltningsverdi` er aldri null | Eksakt kontraktsjekk | Join-forutsetningene må holde for å unngå radeksplosjon | Duplikate nøkler eller uventede kriterieverdier gir dupliserte/feil rader | `ANF_MTM_001` |
    | ANF-MTM-002 | ID-oppslag dekker alle kriterier | Kjente ID-er: dverggås, blodigle, nordlig sildemåke, pelekreps, krikkand og kanadagås | Riktig `Ja` for relevante kriterier og `Nei` for øvrige | Eksakt tekstlikhet | Bekrefter mapping av alle kriteriekolonner | Feil alias/kriteriekolonne eller feil 1/null → Ja/Nei-konvertering | `ANF_MTM_002` |
    | ANF-MTM-003 | ID prioriteres over konfliktende navn | ID `3478` + navn `Clangula hyemalis` | Output bruker dverggås fra ID: `Svært stor verdi`, `Ansvarsarter=Ja`; ikke havelleverdier fra navn | Eksakt tekstlikhet | Dokumenterer prioritet når begge nøkler finnes men peker på ulike arter | Fallback på navn overstyrer korrekt ID-treff | `ANF_MTM_003` |
    | ANF-MTM-004 | ID-oppslag fungerer med manglende input-navn | ID `3506`, `validScientificName=None` | Treffer havelle via ID; `Stor verdi`, `Andre spesielt hensynskrevende arter=Ja` | Eksakt tekst-/nullsjekk | Artskart kan ha manglende navn selv om ID finnes | Gyldige ID-treff mistes når navn mangler | `ANF_MTM_004` |
    | ANF-MTM-005 | Navnefallback når ID er ukjent | Ukjent ID `295741`, navn `Accipiter gentilis` | Treffer via navn; `verdi_m1941_nasjonal=Stor verdi`, `Andre spesielt hensynskrevende arter=Ja` | Eksakt tekstlikhet | Arts-ID kan være ulik mellom kilder | Manglende fallback på vitenskapelig navn | `ANF_MTM_005` |
    | ANF-MTM-006 | Navnefallback når ID er null | `validScientificNameId=None`, navn `Accipiter gentilis` | Treffer via navn og får samme forventede ANF-verdier som ANF-MTM-005 | Eksakt tekst-/radantallssjekk | Reelle rader kan mangle ID men ha navn | Null-ID-rader droppes eller berikes ikke via navn | `ANF_MTM_006` |
    | ANF-MTM-007 | Null-navn skal ikke matche null-navn i kildetabell | `validScientificNameId=None`, `validScientificName=None` | Én outputrad, ingen ANF-verdi, kriterier `Nei`, `Verdi M1941` faller tilbake til rødlisteverdi | Eksakt radantall/nullsjekk | Kildetabellen har noen null-navn | Null=null-join kan gi falske treff og radduplisering | `ANF_MTM_007` |
    | ANF-MTM-008 | Ingen ANF-treff | Ukjent ID og ukjent navn med rødlisteverdi `Middels verdi` | Alle kriterier `Nei`; `verdi_m1941_nasjonal=null`; `Verdi M1941=Middels verdi` | Eksakt tekst-/nullsjekk | Ukjente arter skal beholde rødlistefallback | Ukjente arter får feil kriterietreff eller mister M1941-verdi | `ANF_MTM_008` |
    | ANF-MTM-009 | `-` i ANF-tabell normaliseres | Kanadagås `3495` med rødlisteverdi `Svært stor verdi` | `verdi_m1941_nasjonal=Ingen`; `Verdi M1941=Ingen`; `Fremmede arter=Ja` | Eksakt tekstlikhet | `-` i Mdir betyr ingen verdi og skal være lesbart | `-` lekker til output eller rødlisteverdi overstyrer ANF | `ANF_MTM_009` |
    | ANF-MTM-010 | Rad-/kolonnekontrakt med duplikater | Flere rader inkl. duplikat-ID og ukjent art | Radantall og radrekkefølge beholdes; originalkolonner beholdes; interne join-kolonner droppes; kriterier kun `Ja`/`Nei` | Eksakt liste-/kolonnesjekk | Beriking skal ikke endre observasjonsgrunnlaget | Join mister/dupliserer/sorterer rader eller lekker hjelpekolonner | `ANF_MTM_010` |
    | ANF-MTM-011 | Tom input med riktig schema | Tom `pl.DataFrame` med nødvendige kolonner | Returnerer tom DataFrame med kriterie- og verdikolonner | Eksakt schema-/kolonnesjekk | Pipeline-steg bør tåle tomme datasett etter filtrering | Tom input gir crash eller manglende outputkolonner | `ANF_MTM_011` |
    | ANF-MTM-012 | Manglende obligatoriske kolonner | DataFrame mangler én av `validScientificNameId`, `validScientificName`, `verdi_rodliste_artskart` | Feiler med exception som nevner manglende kolonne | Exception og tekstutdrag | Inputfeil skal være tydelige | Skjult feil eller utydelig feilmelding ved kontraktsbrudd | `ANF_MTM_012` |
    | ANF-MTM-013 | Preferansekjede for `Verdi M1941` | To rader: dverggås `3478` med rødlisteverdi `Noe verdi`, og ukjent art med rødlisteverdi `Middels verdi` | Dverggås får `verdi_m1941_nasjonal=Svært stor verdi` og `Verdi M1941=Svært stor verdi`; ukjent art får `verdi_m1941_nasjonal=null` og `Verdi M1941=Middels verdi` | Eksakt tekst-/nullsjekk | Dokumenterer at ANF/Mdir-verdi prioriteres når den finnes, mens rødlisteverdien bare er fallback uten ANF-treff | Rødlisteverdi overstyrer ANF/Mdir-verdi, eller arter uten ANF-treff mister rødlistefallback | `ANF_MTM_013` |
    """)
    return


@app.cell(hide_code=True)
def ANF_MTM_001(bird_data):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_001():
        """ANF-MTM-001: kilde-/oppslagskontrakt for ANF-tabellen."""
        source = bird_data.execute("SELECT * FROM arter_av_nasjonal_forvaltningsinteresse").pl()
        criteria_source_cols = [
            "kriterium_prioriterte_arter",
            "kriterium_fredete_arter",
            "kriterium_andre_spesielt_hensynskrevende_arter",
            "kriterium_spesielle_okologiske_former",
            "kriterium_dd",
            "kriterium_hensynskrevende_arter",
            "kriterium_ansvarsart",
            "kriterium_fremmede_arter",
        ]

        duplicate_ids = (
            source.filter(pl.col.vitenskapelig_navn_id.is_not_null())
            .group_by("vitenskapelig_navn_id")
            .len()
            .filter(pl.col.len > 1)
        )
        duplicate_names = (
            source.filter(pl.col.vitenskapelig_navn.is_not_null())
            .group_by("vitenskapelig_navn")
            .len()
            .filter(pl.col.len > 1)
        )

        assert duplicate_ids.height == 0, "ANF-MTM-001 ikke-null vitenskapelig_navn_id skal være unik"
        assert duplicate_names.height == 0, "ANF-MTM-001 ikke-null vitenskapelig_navn skal være unikt"
        assert source.get_column("forvaltningsverdi").null_count() == 0, (
            "ANF-MTM-001 forvaltningsverdi skal ikke ha nullverdier"
        )

        for col in criteria_source_cols:
            observed_values = set(source.get_column(col).unique().to_list())
            assert observed_values <= {None, 1}, f"ANF-MTM-001 {col} skal bare inneholde 1/null, fikk {observed_values}"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_001()
    return


@app.cell(hide_code=True)
def ANF_MTM_002(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_002():
        """ANF-MTM-002: ID-oppslag dekker alle ANF-kriteriekolonner."""
        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
        ]
        expected_yes = {
            "dverggås": {"Prioriterte arter", "Andre spesielt hensynskrevende arter", "Ansvarsarter"},
            "blodigle": {"Fredete arter"},
            "nordlig_sildemåke": {"Spesielle økologiske former"},
            "pelekreps": {"Datamangel"},
            "krikkand": {"Hensynskrevende arter"},
            "kanadagås": {"Fremmede arter"},
        }
        expected_values = {
            "dverggås": "Svært stor verdi",
            "blodigle": "Svært stor verdi",
            "nordlig_sildemåke": "Stor verdi",
            "pelekreps": "Middels verdi",
            "krikkand": "Noe verdi",
            "kanadagås": "Ingen",
        }
        test_df = pl.DataFrame(
            {
                "obs_id": list(expected_yes),
                "validScientificNameId": [3478, 4910, 3685, 1807, 3454, 3495],
                "validScientificName": [
                    "Anser erythropus",
                    "Hirudo medicinalis",
                    "Larus fuscus fuscus",
                    "Chelura terebrans",
                    "Anas crecca",
                    "Branta canadensis",
                ],
                "verdi_rodliste_artskart": ["Noe verdi"] * 6,
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)

        assert result.height == test_df.height, "ANF-MTM-002 ID-oppslag skal ikke endre radantall"
        for obs_id, yes_columns in expected_yes.items():
            row = result.filter(pl.col.obs_id == obs_id)
            assert row.height == 1, f"ANF-MTM-002 forventet én rad for {obs_id}"
            assert row.get_column("Verdi M1941").item() == expected_values[obs_id], (
                f"ANF-MTM-002 feil M1941-verdi for {obs_id}"
            )
            for col in criteria_cols:
                expected = "Ja" if col in yes_columns else "Nei"
                actual = row.get_column(col).item()
                assert actual == expected, f"ANF-MTM-002 {obs_id}: {col} skal være {expected}, fikk {actual}"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_002()
    return


@app.cell(hide_code=True)
def ANF_MTM_003(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_003():
        """ANF-MTM-003: ID-oppslag prioriteres over konfliktende vitenskapelig navn."""
        test_df = pl.DataFrame(
            {
                "validScientificNameId": [3478],
                "validScientificName": ["Clangula hyemalis"],
                "verdi_rodliste_artskart": ["Noe verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-003 konfliktende ID/navn skal gi én rad"
        assert row["verdi_m1941_nasjonal"] == "Svært stor verdi", (
            "ANF-MTM-003 ID-treff for dverggås skal overstyre navnetreff for havelle"
        )
        assert row["Verdi M1941"] == "Svært stor verdi", "ANF-MTM-003 endelig verdi skal komme fra ID-treff"
        assert row["Ansvarsarter"] == "Ja", "ANF-MTM-003 dverggås er ansvarsart"
        assert row["Prioriterte arter"] == "Ja", "ANF-MTM-003 dverggås er prioritert art"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_003()
    return


@app.cell(hide_code=True)
def ANF_MTM_004(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_004():
        """ANF-MTM-004: ID-oppslag fungerer selv om input mangler vitenskapelig navn."""
        test_df = pl.DataFrame(
            {
                "validScientificNameId": pl.Series("validScientificNameId", [3506], dtype=pl.Int64),
                "validScientificName": pl.Series("validScientificName", [None], dtype=pl.Utf8),
                "verdi_rodliste_artskart": ["Noe verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-004 manglende navn skal ikke duplisere eller droppe raden"
        assert row["validScientificName"] is None, "ANF-MTM-004 originalt manglende navn skal beholdes"
        assert row["verdi_m1941_nasjonal"] == "Stor verdi", "ANF-MTM-004 havelle skal treffes via ID"
        assert row["Verdi M1941"] == "Stor verdi", "ANF-MTM-004 ID-basert ANF-verdi skal brukes"
        assert row["Andre spesielt hensynskrevende arter"] == "Ja", (
            "ANF-MTM-004 havelle er andre spesielt hensynskrevende art"
        )


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_004()
    return


@app.cell(hide_code=True)
def ANF_MTM_005(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_005():
        """ANF-MTM-005: ukjent ID berikes via fallback på vitenskapelig navn."""
        test_df = pl.DataFrame(
            {
                "validScientificNameId": [295741],
                "validScientificName": ["Accipiter gentilis"],
                "verdi_rodliste_artskart": ["Stor verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-005 navnefallback skal gi én rad"
        assert row["validScientificNameId"] == 295741, "ANF-MTM-005 original Artskart-ID skal beholdes"
        assert row["verdi_m1941_nasjonal"] == "Stor verdi", "ANF-MTM-005 hønsehauk skal treffes via navn"
        assert row["Verdi M1941"] == "Stor verdi", "ANF-MTM-005 ANF-verdi fra navnefallback skal brukes"
        assert row["Andre spesielt hensynskrevende arter"] == "Ja", (
            "ANF-MTM-005 hønsehauk er andre spesielt hensynskrevende art"
        )
        assert row["Ansvarsarter"] == "Nei", "ANF-MTM-005 øvrige kriterier skal fylles som Nei"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_005()
    return


@app.cell(hide_code=True)
def ANF_MTM_006(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_006():
        """ANF-MTM-006: null-ID berikes via fallback på vitenskapelig navn."""
        test_df = pl.DataFrame(
            {
                "validScientificNameId": pl.Series("validScientificNameId", [None], dtype=pl.Int64),
                "validScientificName": ["Accipiter gentilis"],
                "verdi_rodliste_artskart": ["Stor verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-006 null-ID med navnetreff skal gi én rad"
        assert row["validScientificNameId"] is None, "ANF-MTM-006 original null-ID skal beholdes"
        assert row["verdi_m1941_nasjonal"] == "Stor verdi", "ANF-MTM-006 skal finne hønsehauk via navn"
        assert row["Verdi M1941"] == "Stor verdi", "ANF-MTM-006 navnefallback skal gi endelig M1941-verdi"
        assert row["Andre spesielt hensynskrevende arter"] == "Ja", (
            "ANF-MTM-006 navnefallback skal hente kriterier fra ANF-tabellen"
        )


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_006()
    return


@app.cell(hide_code=True)
def ANF_MTM_007(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_007():
        """ANF-MTM-007: null-navn skal ikke matche null-navn i ANF-tabellen."""
        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
        ]
        test_df = pl.DataFrame(
            {
                "validScientificNameId": pl.Series("validScientificNameId", [None], dtype=pl.Int64),
                "validScientificName": pl.Series("validScientificName", [None], dtype=pl.Utf8),
                "verdi_rodliste_artskart": ["Stor verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-007 null/null skal ikke matche flere null-navn i ANF-tabellen"
        assert row["verdi_m1941_nasjonal"] is None, "ANF-MTM-007 null/null skal ikke få ANF-verdi"
        assert row["Verdi M1941"] == "Stor verdi", "ANF-MTM-007 rødlisteverdi skal brukes som fallback"
        for col in criteria_cols:
            assert row[col] == "Nei", f"ANF-MTM-007 {col} skal være Nei når ANF-treff mangler"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_007()
    return


@app.cell(hide_code=True)
def ANF_MTM_008(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_008():
        """ANF-MTM-008: ukjent ID/navn bruker rødlisteverdi som fallback."""
        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
        ]
        test_df = pl.DataFrame(
            {
                "validScientificNameId": [999999],
                "validScientificName": ["Nonexistent species"],
                "verdi_rodliste_artskart": ["Middels verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-008 ukjent art skal beholdes som én rad"
        assert row["verdi_m1941_nasjonal"] is None, "ANF-MTM-008 ukjent art skal ikke få ANF-verdi"
        assert row["Verdi M1941"] == "Middels verdi", "ANF-MTM-008 rødlisteverdi skal brukes som fallback"
        for col in criteria_cols:
            assert row[col] == "Nei", f"ANF-MTM-008 {col} skal være Nei når ANF-treff mangler"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_008()
    return


@app.cell(hide_code=True)
def ANF_MTM_009(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_009():
        """ANF-MTM-009: '-' i ANF-tabellen normaliseres til 'Ingen'."""
        test_df = pl.DataFrame(
            {
                "validScientificNameId": [3495],
                "validScientificName": ["Branta canadensis"],
                "verdi_rodliste_artskart": ["Svært stor verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        row = result.row(0, named=True)

        assert result.height == 1, "ANF-MTM-009 kanadagås skal gi én rad"
        assert row["verdi_rodliste_artskart"] == "Svært stor verdi", "ANF-MTM-009 rødlisteverdien skal beholdes"
        assert row["verdi_m1941_nasjonal"] == "Ingen", "ANF-MTM-009 '-' fra ANF skal normaliseres til Ingen"
        assert row["Verdi M1941"] == "Ingen", "ANF-MTM-009 normalisert ANF-verdi skal overstyre rødlisteverdi"
        assert row["Fremmede arter"] == "Ja", "ANF-MTM-009 kanadagås skal være fremmed art"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_009()
    return


@app.cell(hide_code=True)
def ANF_MTM_010(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_010():
        """ANF-MTM-010: rad-/kolonnekontrakt med duplikate inputrader."""
        from polars.testing import assert_frame_equal

        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
        ]
        test_df = pl.DataFrame(
            {
                "obs_id": ["dup-1", "dup-2", "ukjent", "havelle"],
                "validScientificNameId": [3478, 3478, 999999, 3506],
                "validScientificName": [
                    "Anser erythropus",
                    "Anser erythropus",
                    "Nonexistent species",
                    "Clangula hyemalis",
                ],
                "verdi_rodliste_artskart": ["Noe verdi", "Noe verdi", "Middels verdi", "Noe verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        internal_cols = ["arts_id_mdir", "vitenskapelig_navn_mdir"] + [
            f"{col}_fallback" for col in [*criteria_cols, "verdi_m1941_nasjonal"]
        ]

        assert result.height == test_df.height, "ANF-MTM-010 radantall skal beholdes"
        assert result.get_column("obs_id").to_list() == test_df.get_column("obs_id").to_list(), (
            "ANF-MTM-010 radrekkefølge skal beholdes"
        )
        assert_frame_equal(result.select(test_df.columns), test_df)
        assert all(col not in result.columns for col in internal_cols), "ANF-MTM-010 interne join-kolonner skal droppes"

        duplicate_rows = result.filter(pl.col.validScientificNameId == 3478)
        assert duplicate_rows.height == 2, "ANF-MTM-010 duplikate observasjoner skal ikke slås sammen"
        assert duplicate_rows.get_column("Verdi M1941").to_list() == ["Svært stor verdi", "Svært stor verdi"], (
            "ANF-MTM-010 duplikate ID-treff skal få lik beriking"
        )

        for col in criteria_cols:
            assert result.get_column(col).null_count() == 0, f"ANF-MTM-010 {col} skal ikke ha nullverdier"
            observed_values = set(result.get_column(col).unique().to_list())
            assert observed_values <= {"Ja", "Nei"}, f"ANF-MTM-010 {col} har ugyldige verdier: {observed_values}"


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_010()
    return


@app.cell(hide_code=True)
def ANF_MTM_011(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_011():
        """ANF-MTM-011: tom input med riktig schema gir tom output med ANF-kolonner."""
        criteria_cols = [
            "Prioriterte arter",
            "Fredete arter",
            "Andre spesielt hensynskrevende arter",
            "Spesielle økologiske former",
            "Datamangel",
            "Hensynskrevende arter",
            "Ansvarsarter",
            "Fremmede arter",
        ]
        value_cols = ["verdi_m1941_nasjonal", "Verdi M1941"]
        test_df = pl.DataFrame(
            {
                "validScientificNameId": pl.Series("validScientificNameId", [], dtype=pl.Int64),
                "validScientificName": pl.Series("validScientificName", [], dtype=pl.Utf8),
                "verdi_rodliste_artskart": pl.Series("verdi_rodliste_artskart", [], dtype=pl.Utf8),
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)

        assert result.height == 0, "ANF-MTM-011 tom input skal gi tom output"
        assert result.select(test_df.columns).height == 0, "ANF-MTM-011 originalkolonner skal finnes i tom output"
        for col in [*criteria_cols, *value_cols]:
            assert col in result.columns, f"ANF-MTM-011 {col} skal finnes i output"
        assert all(col not in result.columns for col in ["arts_id_mdir", "vitenskapelig_navn_mdir"]), (
            "ANF-MTM-011 interne join-kolonner skal ikke finnes i tom output"
        )


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_011()
    return


@app.cell(hide_code=True)
def ANF_MTM_012(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_012():
        """ANF-MTM-012: manglende obligatoriske kolonner feiler tydelig."""

        def assert_missing_column_raises(column_name: str, test_df: pl.DataFrame) -> None:
            try:
                legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
            except Exception as exc:
                assert column_name in str(exc), (
                    f"ANF-MTM-012 feilmelding for manglende {column_name} skal nevne kolonnen; fikk {exc!r}"
                )
            else:
                raise AssertionError(f"ANF-MTM-012 manglende {column_name} skulle gi feil")

        assert_missing_column_raises(
            "validScientificNameId",
            pl.DataFrame(
                {
                    "validScientificName": ["Anser erythropus"],
                    "verdi_rodliste_artskart": ["Noe verdi"],
                }
            ),
        )
        assert_missing_column_raises(
            "validScientificName",
            pl.DataFrame(
                {
                    "validScientificNameId": [3478],
                    "verdi_rodliste_artskart": ["Noe verdi"],
                }
            ),
        )
        assert_missing_column_raises(
            "verdi_rodliste_artskart",
            pl.DataFrame(
                {
                    "validScientificNameId": [3478],
                    "validScientificName": ["Anser erythropus"],
                }
            ),
        )


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_012()
    return


@app.cell(hide_code=True)
def ANF_MTM_013(legg_til_arter_av_nasjonal_forvaltningsinteresse):
    def test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_013():
        """ANF-MTM-013: Verdi M1941 bruker ANF/Mdir først, deretter rødlistefallback."""
        test_df = pl.DataFrame(
            {
                "validScientificNameId": [3478, 999999],
                "validScientificName": ["Anser erythropus", "Nonexistent species"],
                "verdi_rodliste_artskart": ["Noe verdi", "Middels verdi"],
            }
        )

        result = legg_til_arter_av_nasjonal_forvaltningsinteresse(test_df)
        rows_by_id = {row["validScientificNameId"]: row for row in result.iter_rows(named=True)}

        assert result.height == 2, "ANF-MTM-013 skal bevare begge inputrader"

        dverggaas = rows_by_id[3478]
        assert dverggaas["verdi_rodliste_artskart"] == "Noe verdi", "ANF-MTM-013 original rødlisteverdi skal beholdes"
        assert dverggaas["verdi_m1941_nasjonal"] == "Svært stor verdi", "ANF-MTM-013 dverggås skal få ANF/Mdir-verdi"
        assert dverggaas["Verdi M1941"] == "Svært stor verdi", (
            "ANF-MTM-013 ANF/Mdir-verdi skal prioriteres over rødlisteverdi når ANF-treff finnes"
        )

        ukjent_art = rows_by_id[999999]
        assert ukjent_art["verdi_m1941_nasjonal"] is None, "ANF-MTM-013 ukjent art skal mangle ANF/Mdir-verdi"
        assert ukjent_art["Verdi M1941"] == "Middels verdi", (
            "ANF-MTM-013 rødlisteverdi skal brukes som fallback når ANF-treff mangler"
        )


    test_legg_til_arter_av_nasjonal_forvaltningsinteresse_anf_mtm_013()
    return


@app.cell(hide_code=True)
def md_oppsummer_anf_kriterier():
    mo.md(r"""
    ### Lager en ny kolonne som inneholder mulige verdier av arter av nasjonal forvaltning
    """)
    return


@app.function
def legg_til_kolonne_arteravnasjonal(input_df: pl.DataFrame) -> pl.DataFrame:
    """Oppsummer ANF-kriterier i én lesbar tekstkolonne.

    Args:
        input_df: DataFrame med ANF-kriteriekolonner kodet som "Ja"/"Nei".

    Returns:
        DataFrame med en oppsummeringskolonne for arter av nasjonal
        forvaltningsinteresse. Kolonnen inneholder kommaseparerte
        kriterier eller "Nei".

    Notes:
        Bare kriteriekolonner med verdien "Ja" tas med i oppsummeringen.
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

    category_list = (
        pl.concat_list(  # slår sammen alle anf til en kolonne, men merk List Concatenation  = packing items into a list within a single row ( noe annet enn a stacke tabbeller)
            *[
                pl.when(pl.col(col) == "Ja")
                .then(pl.lit(col))
                .otherwise(pl.lit(None))  # erstatter Ja/Nei med kolonne navnet
                for col in category_columns
            ]
        ).list.drop_nulls()  # fjerner null verdier slik at du ikke får NT, null, null, Fremmed art
    )

    output_df = input_df.with_columns(
        pl.when(category_list.list.len() > 0)
        .then(category_list.list.join(", "))
        .otherwise(pl.lit("Nei"))
        .alias("Art av nasjonal forvaltningsinteresse (eks. rødlista)")
    )

    # You need .list.join(", ") because pl.concat_list() gives you the computer-code format (a list object), and you want the human-readable format (a single text string). Hvor du da joiner tingene i listen med ,

    return output_df


@app.cell(hide_code=True)
def md_testmatrise_legg_til_kolonne_arteravnasjonal():
    mo.md(r"""
    ### Testmatrise: `legg_til_kolonne_arteravnasjonal`

    **Tiltenkt oppførsel:** Funksjonen skal oppsummere ANF-kriteriekolonner til én lesbar tekstkolonne. Bare verdier som er eksakt `"Ja"` tas med. Hvis ingen kriterier er `"Ja"`, blir oppsummeringen `"Nei"`.

    **Kilde til sannhet:** Godkjent matrise 2026-06-05, funksjonsdocstring, godkjent ANF-kontrakt fra forrige steg og håndberegnede små eksempler.

    **Inputkontrakt:** `input_df` er en `pl.DataFrame` med kriteriekolonnene `Ansvarsarter`, `Andre spesielt hensynskrevende arter`, `Hensynskrevende arter`, `Spesielle økologiske former`, `Datamangel`, `Prioriterte arter`, `Fredete arter` og `Fremmede arter`. Ordinær pipeline koder disse som `"Ja"`/`"Nei"`, men funksjonen skal bare telle eksakt `"Ja"`.

    **Outputkontrakt:** Returnerer `pl.DataFrame` med samme rader, radrekkefølge og originalkolonner bevart, pluss tekstkolonnen `Art av nasjonal forvaltningsinteresse (eks. rødlista)`. Kolonnen inneholder kommaseparerte kriterinavn i fast rekkefølge, eller `"Nei"` når ingen kriterier er eksakt `"Ja"`.

    **Godkjenningsstatus:** Godkjent av bruker 2026-06-05.

    **Revisjonspolicy:** Hvis forventet oppførsel endres, oppdater og godkjenn denne matrisen på nytt før tester eller funksjonslogikk endres.

    | ID | Scenario | Input | Forventet output/invariant | Toleranse | Hvorfor det betyr noe | Feilmodus testen beskytter mot | Testcelle |
    |---|---|---|---|---|---|---|---|
    | ANF-SUM-MTM-001 | Enkel og sammensatt oppsummering | Rader med 0, 1 og flere `"Ja"` i kriteriekolonnene | `0 Ja → "Nei"`, `1 Ja → kriteriets navn`, flere `Ja → "Ansvarsarter, Prioriterte arter, Fredete arter"` i fast rekkefølge | Eksakt tekstlikhet | Dekker hovedformålet med funksjonen | Feil kriterier tas med, eller `"Nei"` håndteres feil | `ANF_SUM_MTM_001` |
    | ANF-SUM-MTM-002 | Alle kriterier er `"Ja"` | Én rad der alle 8 kriteriekolonner er `"Ja"` | Alle 8 kriterier inngår én gang, kommaseparert, i denne rekkefølgen: `Ansvarsarter`, `Andre spesielt hensynskrevende arter`, `Hensynskrevende arter`, `Spesielle økologiske former`, `Datamangel`, `Prioriterte arter`, `Fredete arter`, `Fremmede arter` | Eksakt tekstlikhet | Sikrer komplett dekning av alle kriteriekolonner | Manglende kriteriekolonne, feil rekkefølge eller feil separator | `ANF_SUM_MTM_002` |
    | ANF-SUM-MTM-003 | Streng Ja-policy | Verdier som `"Nei"`, `None`, `"ja"`, `" JA "` og `"Treff ikke funnet i ANF tabell "` | Ingen av disse tas med; hvis ingen eksakt `"Ja"` finnes blir output `"Nei"` | Eksakt tekstlikhet | Dokumenterer at bare eksakt `"Ja"` betyr ANF-treff | Ugyldige eller uklare verdier blir feilaktig tolket som treff | `ANF_SUM_MTM_003` |
    | ANF-SUM-MTM-004 | Rad- og kolonnekontrakt | DataFrame med ekstra kolonner og dupliserte rader | Radantall, radrekkefølge og originalkolonner beholdes uendret; ny kolonne legges til | Eksakt schema-/rekkefølgesjekk | Funksjonen skal bare legge til oppsummering, ikke endre observasjoner | Rader droppes, sorteres, dupliseres eller originaldata endres | `ANF_SUM_MTM_004` |
    | ANF-SUM-MTM-005 | Tom input med riktig schema | Tom `pl.DataFrame` med alle påkrevde kriteriekolonner | Returnerer tom DataFrame med oppsummeringskolonnen `Art av nasjonal forvaltningsinteresse (eks. rødlista)` | Eksakt schema-/kolonnesjekk | Pipeline bør tåle tomme datasett etter filtrering | Tom input gir crash eller manglende outputkolonne | `ANF_SUM_MTM_005` |
    | ANF-SUM-MTM-006 | Manglende obligatorisk kriteriekolonne | DataFrame som mangler én påkrevd kriteriekolonne | Feiler med exception som nevner manglende kolonne | Exception og tekstutdrag | Inputfeil skal være tydelige | Skjult feil eller utydelig kontraktsbrudd ved manglende inputkolonne | `ANF_SUM_MTM_006` |
    """)
    return


@app.cell(hide_code=True)
def _():
    def test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_001():
        """ANF-SUM-MTM-001: enkel og sammensatt oppsummering."""
        output_col = "Art av nasjonal forvaltningsinteresse (eks. rødlista)"
        test_df = pl.DataFrame(
            {
                "art": ["ingen ANF-kriterier", "datamangel", "hubro"],
                "Ansvarsarter": ["Nei", "Nei", "Ja"],
                "Andre spesielt hensynskrevende arter": ["Nei", "Nei", "Nei"],
                "Hensynskrevende arter": ["Nei", "Nei", "Nei"],
                "Spesielle økologiske former": ["Nei", "Nei", "Nei"],
                "Datamangel": ["Nei", "Ja", "Nei"],
                "Prioriterte arter": ["Nei", "Nei", "Ja"],
                "Fredete arter": ["Nei", "Nei", "Ja"],
                "Fremmede arter": ["Nei", "Nei", "Nei"],
            }
        )

        result = legg_til_kolonne_arteravnasjonal(test_df)

        assert output_col in result.columns, "ANF-SUM-MTM-001 oppsummeringskolonnen skal finnes"
        assert result.get_column(output_col).to_list() == [
            "Nei",
            "Datamangel",
            "Ansvarsarter, Prioriterte arter, Fredete arter",
        ], "ANF-SUM-MTM-001 feil oppsummering for 0, 1 eller flere Ja-verdier"


    test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_001()
    return


@app.cell(hide_code=True)
def ANF_SUM_MTM_002():
    def test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_002():
        """ANF-SUM-MTM-002: alle kriterier er Ja og inngår i fast rekkefølge."""
        output_col = "Art av nasjonal forvaltningsinteresse (eks. rødlista)"
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
        test_df = pl.DataFrame({col: ["Ja"] for col in category_columns})

        result = legg_til_kolonne_arteravnasjonal(test_df)
        expected = ", ".join(category_columns)

        assert result.height == 1, "ANF-SUM-MTM-002 radantall skal beholdes"
        assert result.get_column(output_col).to_list() == [expected], (
            "ANF-SUM-MTM-002 alle kriterier skal inngå én gang i fast rekkefølge"
        )


    test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_002()
    return


@app.cell(hide_code=True)
def ANF_SUM_MTM_003():
    def test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_003():
        """ANF-SUM-MTM-003: bare eksakt Ja telles som ANF-treff."""
        output_col = "Art av nasjonal forvaltningsinteresse (eks. rødlista)"
        ikke_treff = "Treff ikke funnet i ANF tabell "
        test_df = pl.DataFrame(
            {
                "art": ["ingen eksakt Ja", "kun fredete er eksakt Ja"],
                "Ansvarsarter": ["ja", ikke_treff],
                "Andre spesielt hensynskrevende arter": [None, " JA "],
                "Hensynskrevende arter": [" JA ", "Nei"],
                "Spesielle økologiske former": [ikke_treff, "ja"],
                "Datamangel": ["Nei", None],
                "Prioriterte arter": ["", "TRUE"],
                "Fredete arter": ["nei", "Ja"],
                "Fremmede arter": ["1", ikke_treff],
            }
        )

        result = legg_til_kolonne_arteravnasjonal(test_df)

        assert result.get_column(output_col).to_list() == ["Nei", "Fredete arter"], (
            "ANF-SUM-MTM-003 bare eksakt 'Ja' skal telles; andre verdier skal ignoreres"
        )


    test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_003()
    return


@app.cell(hide_code=True)
def ANF_SUM_MTM_004():
    def test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_004():
        """ANF-SUM-MTM-004: radrekkefølge, duplikater og originalkolonner bevares."""
        from polars.testing import assert_frame_equal

        output_col = "Art av nasjonal forvaltningsinteresse (eks. rødlista)"
        test_df = pl.DataFrame(
            {
                "obs_id": ["obs-1", "obs-2", "obs-2"],
                "art": ["hubro", "gråspurv", "gråspurv"],
                "Ansvarsarter": ["Ja", "Nei", "Nei"],
                "Andre spesielt hensynskrevende arter": ["Nei", "Nei", "Nei"],
                "Hensynskrevende arter": ["Nei", "Nei", "Nei"],
                "Spesielle økologiske former": ["Nei", "Nei", "Nei"],
                "Datamangel": ["Nei", "Nei", "Nei"],
                "Prioriterte arter": ["Ja", "Nei", "Nei"],
                "Fredete arter": ["Ja", "Nei", "Nei"],
                "Fremmede arter": ["Nei", "Nei", "Nei"],
                "ekstra_kolonne": [10, 20, 20],
            }
        )
        original_cols = test_df.columns

        result = legg_til_kolonne_arteravnasjonal(test_df)

        assert result.height == test_df.height, "ANF-SUM-MTM-004 radantall skal beholdes"
        assert result.columns == [*original_cols, output_col], (
            "ANF-SUM-MTM-004 originalkolonner skal beholdes og ny kolonne legges til sist"
        )
        assert result.get_column("obs_id").to_list() == ["obs-1", "obs-2", "obs-2"], (
            "ANF-SUM-MTM-004 radrekkefølge og duplikate observasjoner skal beholdes"
        )
        assert_frame_equal(result.select(original_cols), test_df)


    test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_004()
    return


@app.cell(hide_code=True)
def ANF_SUM_MTM_005():
    def test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_005():
        """ANF-SUM-MTM-005: tom input med riktig schema gir tom output med oppsummeringskolonne."""
        output_col = "Art av nasjonal forvaltningsinteresse (eks. rødlista)"
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
        test_df = pl.DataFrame({col: pl.Series(col, [], dtype=pl.Utf8) for col in category_columns})

        result = legg_til_kolonne_arteravnasjonal(test_df)

        assert result.height == 0, "ANF-SUM-MTM-005 tom input skal gi tom output"
        assert result.columns == [*category_columns, output_col], (
            "ANF-SUM-MTM-005 oppsummeringskolonnen skal finnes i tom output"
        )
        assert result.schema[output_col] == pl.Utf8, "ANF-SUM-MTM-005 oppsummeringskolonnen skal være tekst"


    test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_005()
    return


@app.cell(hide_code=True)
def ANF_SUM_MTM_006():
    def test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_006():
        """ANF-SUM-MTM-006: manglende kriteriekolonne feiler tydelig."""
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

        def assert_missing_column_raises(column_name: str) -> None:
            test_df = pl.DataFrame({col: ["Nei"] for col in category_columns if col != column_name})
            try:
                legg_til_kolonne_arteravnasjonal(test_df)
            except Exception as exc:
                assert column_name in str(exc), (
                    f"ANF-SUM-MTM-006 feilmeldingen for manglende {column_name} skal nevne kolonnen; fikk {exc!r}"
                )
            else:
                raise AssertionError(f"ANF-SUM-MTM-006 manglende {column_name} skulle gi feil")

        assert_missing_column_raises("Ansvarsarter")
        assert_missing_column_raises("Fremmede arter")


    test_legg_til_kolonne_arteravnasjonal_anf_sum_mtm_006()
    return


@app.cell(hide_code=True)
def md_rydd_navn_og_datatyper():
    mo.md(r"""
    ### Rydder opp i navn og datatyper
    """)
    return


@app.cell(hide_code=True)
def md_testmatrise_rydd_navn_og_datatyper():
    mo.md(r"""
    ### Testmatrise: `rydd_navn_og_datatyper`

    **Tiltenkt oppførsel:** Funksjonen skal lage sluttabellen med norske kolonnenavn, riktige datatyper, faste sluttkolonner og sortering etter `Verdi M1941` og `Kategori`.

    **Kilde til sannhet:** Godkjent matrise 2026-06-05, funksjonsdocstring, pipeline-kontrakt fra forrige steg, godkjent nytt ANF-kolonnenavn og håndberegnede små eksempler.

    **Inputkontrakt:** `df_input` er en `pl.DataFrame` etter ANF-beriking, med kolonnene `Verdi M1941`, `category`, `Art av nasjonal forvaltningsinteresse (eks. rødlista)`, `preferredPopularName`, `validScientificName`, `individualCount`, `behavior`, `dateTimeCollected`, `coordinateUncertaintyInMeters`, taksonomi-, lokalitets-, koordinat- og ANF-kriteriekolonner.

    **Outputkontrakt:** Returnerer `pl.DataFrame` med kun sluttbrukerrettede kolonner i fast rekkefølge. Radantall beholdes, men rader sorteres. Ekstra inputkolonner droppes. `Verdi M1941` sorteres slik: `Svært stor verdi`, `Stor verdi`, `Middels verdi`, `Noe verdi`, `Ingen`, der ukjente/default-verdier kommer sist.

    **Godkjenningsstatus:** Godkjent av bruker 2026-06-05.

    **Revisjonspolicy:** Hvis forventet oppførsel endres, oppdater og godkjenn denne matrisen på nytt før tester eller funksjonslogikk endres.

    | ID | Scenario | Input | Forventet output/invariant | Toleranse | Hvorfor det betyr noe | Feilmodus testen beskytter mot | Testcelle |
    |---|---|---|---|---|---|---|---|
    | RYDD-MTM-001 | Sluttschema og kolonnerekkefølge | Liten komplett DataFrame med alle påkrevde kolonner + én ekstra kolonne | Kun sluttkolonner beholdes, i godkjent rekkefølge. ANF-kolonnen heter `Art av nasjonal forvaltningsinteresse (eks. rødlista)` | Eksakt kolonneliste | Sluttskjemaet er kontrakten mot eksport/analyse | Gammelt ANF-kolonnenavn, feil kolonnerekkefølge eller lekkasje av hjelpekolonner | `RYDD_MTM_001` |
    | RYDD-MTM-002 | Sortering etter M1941 og kategori | Rader i usortert rekkefølge med `Svært stor verdi`, `Stor verdi`, `Middels verdi`, `Noe verdi`, `Ingen` og ukjent verdi | Sortert etter M1941-prioritet først: 0–4, ukjente verdier sist; deretter kategori-prioritet. Like sorteringsnøkler beholder inputrekkefølge | Eksakt radrekkefølge | Riktig prioritering er viktig for sluttpresentasjon | Feil sorteringsrekkefølge, `Ingen` feilplassert eller ustabil sortering | `RYDD_MTM_002` |
    | RYDD-MTM-003 | `individualCount` → `Antall` | Verdier `"6"`, `None`, `"3/1"` | `"6" → 6`, `None → 1`, `"3/1" → 3`; dtype `Int64` | Eksakt verdi og dtype | Antall brukes i summeringer og må være numerisk | Null tolkes feil, brøkformat håndteres feil eller dtype blir tekst | `RYDD_MTM_003` |
    | RYDD-MTM-004 | Dato, usikkerhet og koordinater | Datetime, null/usikkerhet, latitude/longitude med komma og punktum | `dateTimeCollected → Observert dato` som `pl.Date`; `coordinateUncertaintyInMeters → Int64`; koordinater → `Float64` | Eksakt dtype, dato og numerisk toleranse `1e-6` | Sluttdata må ha riktige typer for kart og filtrering | Koordinater forblir tekst, dato beholder tid eller usikkerhet får feil type | `RYDD_MTM_004` |
    | RYDD-MTM-005 | Omdøping og passthrough-verdier | Kjente verdier for artsnavn, taksonomi, lokalitet, ANF-kriterier, geometry og ID | Norske kolonnenavn får riktige verdier etter sortering; ANF-kriteriekolonner og geometry bevares | Eksakt tekst-/listekontroll | Sikrer at sluttkolonner peker på riktig kildekolonne | Feil alias, verdier forskyves etter sortering eller kriterier mistes | `RYDD_MTM_005` |
    | RYDD-MTM-006 | Tom input med riktig schema | Tom DataFrame med alle påkrevde inputkolonner og riktige datatyper | Returnerer tom DataFrame med sluttkolonner og forventede dtypes | Eksakt schema-/kolonnesjekk | Pipeline bør tåle tomme datasett etter filtrering | Tom input gir crash eller manglende outputkolonner | `RYDD_MTM_006` |
    | RYDD-MTM-007 | Manglende obligatorisk kolonne | DataFrame mangler én påkrevd inputkolonne | Feiler med exception som nevner manglende kolonne | Exception og tekstutdrag | Inputfeil skal være tydelige | Skjult feil eller utydelig kontraktsbrudd ved manglende inputkolonne | `RYDD_MTM_007` |
    """)
    return


@app.function(hide_code=True)
def rydd_navn_og_datatyper(df_input: pl.DataFrame) -> pl.DataFrame:
    """Lag sluttabellen med norske kolonnenavn, riktige typer og sortering.

    Args:
        df_input: Beriket Artskart-data etter taksonomi-, M1941- og
            ANF-beriking.

    Returns:
        DataFrame med sluttkolonner, norske kolonnenavn og sortering etter
        M1941-verdi og kategori.

    Notes:
        Manglende `individualCount` tolkes som én observasjon. Verdier på
        formen "1/1" bruker første tall som observert antall.
    """

    VERDI_M1941_ORDER = {
        "Svært stor verdi": 0,
        "Stor verdi": 1,
        "Middels verdi": 2,
        "Noe verdi": 3,
        "Ingen": 4,
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
                pl.col("Art av nasjonal forvaltningsinteresse (eks. rødlista)"),
                pl.col("preferredPopularName").alias("Navn"),
                pl.col("validScientificName").alias("Art"),
                pl.col("individualCount")
                .fill_null("1")  # antar at alle obs = minimum 1 når observatøren ikke har lagt inn spesifikt antall
                .str.split("/")  # Noen har en 1/1 antall - antar at det er det første tallet som git antall
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
                pl.col("Andre spesielt hensynskrevende arter"),
                pl.col("Hensynskrevende arter"),
                pl.col("Spesielle økologiske former"),
                pl.col("Prioriterte arter"),
                pl.col("Fredete arter"),
                pl.col("Datamangel"),
                pl.col("Ansvarsarter"),
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


@app.cell(hide_code=True)
def rydd_navn_og_datatyper_testhjelpere():
    def rydd_navn_og_datatyper_forventede_kolonner() -> list[str]:
        """Returner godkjent sluttkolonnerekkefølge for rydd_navn_og_datatyper-testene."""
        return [
            "Verdi M1941",
            "Kategori",
            "Art av nasjonal forvaltningsinteresse (eks. rødlista)",
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
            "Andre spesielt hensynskrevende arter",
            "Hensynskrevende arter",
            "Spesielle økologiske former",
            "Prioriterte arter",
            "Fredete arter",
            "Datamangel",
            "Ansvarsarter",
            "Fremmede arter",
            "latitude",
            "longitude",
            "geometry",
            "Artens ID",
        ]


    def lag_rydd_navn_og_datatyper_input(rad_overrides: list[dict[str, object]] | None = None) -> pl.DataFrame:
        """Lag komplett, liten inputfixture for rydd_navn_og_datatyper."""
        if rad_overrides is None:
            rad_overrides = [{}]

        grunnrad = {
            "Verdi M1941": "Noe verdi",
            "category": "LC",
            "Art av nasjonal forvaltningsinteresse (eks. rødlista)": "Nei",
            "preferredPopularName": "standardnavn",
            "validScientificName": "Species standardus",
            "individualCount": "1",
            "behavior": None,
            "dateTimeCollected": datetime(2020, 1, 1, 12, 0, 0),
            "coordinateUncertaintyInMeters": None,
            "FamilieNavn": "Standardfamilie",
            "OrdenNavn": "Standardorden",
            "taxonGroupName": "Fugler",
            "collector": "Standard observatør",
            "locality": "Standardlokalitet",
            "municipality": "Standardkommune",
            "county": "Standardfylke",
            "scientificNameRank": "species",
            "Andre spesielt hensynskrevende arter": "Nei",
            "Hensynskrevende arter": "Nei",
            "Spesielle økologiske former": "Nei",
            "Prioriterte arter": "Nei",
            "Fredete arter": "Nei",
            "Datamangel": "Nei",
            "Ansvarsarter": "Nei",
            "Fremmede arter": "Nei",
            "latitude": "68,000000",
            "longitude": "15,000000",
            "geometry": "POINT (0 0)",
            "validScientificNameId": 1000,
            "ekstra_inputkolonne": "skal droppes",
        }

        rader = []
        for indeks, overrides in enumerate(rad_overrides):
            rad = {
                **grunnrad,
                "preferredPopularName": f"standardnavn-{indeks}",
                "validScientificName": f"Species standardus {indeks}",
                "validScientificNameId": 1000 + indeks,
                "geometry": f"POINT ({indeks} {indeks})",
            }
            rad.update(overrides)
            rader.append(rad)

        return pl.DataFrame(rader)


    def lag_tom_rydd_navn_og_datatyper_input() -> pl.DataFrame:
        """Lag tom inputfixture med riktig schema for rydd_navn_og_datatyper."""
        tekstkolonner = [
            "Verdi M1941",
            "category",
            "Art av nasjonal forvaltningsinteresse (eks. rødlista)",
            "preferredPopularName",
            "validScientificName",
            "individualCount",
            "behavior",
            "FamilieNavn",
            "OrdenNavn",
            "taxonGroupName",
            "collector",
            "locality",
            "municipality",
            "county",
            "scientificNameRank",
            "Andre spesielt hensynskrevende arter",
            "Hensynskrevende arter",
            "Spesielle økologiske former",
            "Prioriterte arter",
            "Fredete arter",
            "Datamangel",
            "Ansvarsarter",
            "Fremmede arter",
            "latitude",
            "longitude",
            "geometry",
            "ekstra_inputkolonne",
        ]
        kolonner = {kolonne: pl.Series(kolonne, [], dtype=pl.Utf8) for kolonne in tekstkolonner}
        kolonner["dateTimeCollected"] = pl.Series("dateTimeCollected", [], dtype=pl.Datetime)
        kolonner["coordinateUncertaintyInMeters"] = pl.Series("coordinateUncertaintyInMeters", [], dtype=pl.Int64)
        kolonner["validScientificNameId"] = pl.Series("validScientificNameId", [], dtype=pl.Int64)
        return pl.DataFrame(kolonner)

    return (
        lag_rydd_navn_og_datatyper_input,
        lag_tom_rydd_navn_og_datatyper_input,
        rydd_navn_og_datatyper_forventede_kolonner,
    )


@app.cell(hide_code=True)
def RYDD_MTM_001(
    lag_rydd_navn_og_datatyper_input,
    rydd_navn_og_datatyper_forventede_kolonner,
):
    def test_rydd_navn_og_datatyper_rydd_mtm_001():
        """RYDD-MTM-001: sluttschema og kolonnerekkefølge."""
        test_df = lag_rydd_navn_og_datatyper_input(
            [
                {
                    "preferredPopularName": "dompap",
                    "validScientificName": "Pyrrhula pyrrhula",
                    "Art av nasjonal forvaltningsinteresse (eks. rødlista)": "Ansvarsarter",
                }
            ]
        )

        result = rydd_navn_og_datatyper(test_df)
        expected_cols = rydd_navn_og_datatyper_forventede_kolonner()

        assert result.height == test_df.height, "RYDD-MTM-001 radantall skal beholdes"
        assert result.columns == expected_cols, (
            f"RYDD-MTM-001 forventet sluttkolonner i fast rekkefølge; fikk {result.columns}"
        )
        assert "ekstra_inputkolonne" not in result.columns, "RYDD-MTM-001 ekstra inputkolonner skal droppes"
        assert "Art av nasjonal forvaltningsinteresse" not in result.columns, (
            "RYDD-MTM-001 gammelt ANF-kolonnenavn uten '(eks. rødlista)' skal ikke være i output"
        )
        assert "Art av nasjonal forvaltningsinteresse (eks. rødlista)" in result.columns, (
            "RYDD-MTM-001 nytt ANF-kolonnenavn skal være i output"
        )


    test_rydd_navn_og_datatyper_rydd_mtm_001()
    return


@app.cell(hide_code=True)
def RYDD_MTM_002(lag_rydd_navn_og_datatyper_input):
    def test_rydd_navn_og_datatyper_rydd_mtm_002():
        """RYDD-MTM-002: sortering etter Verdi M1941 og kategori."""
        test_df = lag_rydd_navn_og_datatyper_input(
            [
                {"validScientificName": "noe-lc", "Verdi M1941": "Noe verdi", "category": "LC"},
                {"validScientificName": "svært-en", "Verdi M1941": "Svært stor verdi", "category": "EN"},
                {"validScientificName": "ingen-se", "Verdi M1941": "Ingen", "category": "SE"},
                {"validScientificName": "stor-lc", "Verdi M1941": "Stor verdi", "category": "LC"},
                {"validScientificName": "middels-nt", "Verdi M1941": "Middels verdi", "category": "NT"},
                {"validScientificName": "noe-cr-1", "Verdi M1941": "Noe verdi", "category": "CR"},
                {"validScientificName": "svært-cr", "Verdi M1941": "Svært stor verdi", "category": "CR"},
                {"validScientificName": "ukjent-cr", "Verdi M1941": "Ukjent verdi", "category": "CR"},
                {"validScientificName": "ingen-lc", "Verdi M1941": "Ingen", "category": "LC"},
                {"validScientificName": "noe-cr-2", "Verdi M1941": "Noe verdi", "category": "CR"},
            ]
        )

        result = rydd_navn_og_datatyper(test_df)

        assert result.get_column("Art").to_list() == [
            "svært-cr",
            "svært-en",
            "stor-lc",
            "middels-nt",
            "noe-cr-1",
            "noe-cr-2",
            "noe-lc",
            "ingen-lc",
            "ingen-se",
            "ukjent-cr",
        ], "RYDD-MTM-002 rader skal sorteres etter M1941, kategori og stabil inputrekkefølge"
        assert result.get_column("Verdi M1941").to_list() == [
            "Svært stor verdi",
            "Svært stor verdi",
            "Stor verdi",
            "Middels verdi",
            "Noe verdi",
            "Noe verdi",
            "Noe verdi",
            "Ingen",
            "Ingen",
            "Ukjent verdi",
        ], "RYDD-MTM-002 'Ingen' skal komme etter 'Noe verdi' og ukjente verdier sist"
        assert result.get_column("Kategori").to_list() == [
            "CR",
            "EN",
            "LC",
            "NT",
            "CR",
            "CR",
            "LC",
            "LC",
            "SE",
            "CR",
        ], "RYDD-MTM-002 kategori skal sorteres innenfor samme M1941-verdi"


    test_rydd_navn_og_datatyper_rydd_mtm_002()
    return


@app.cell(hide_code=True)
def RYDD_MTM_003(lag_rydd_navn_og_datatyper_input):
    def test_rydd_navn_og_datatyper_rydd_mtm_003():
        """RYDD-MTM-003: individualCount transformeres til Antall."""
        test_df = lag_rydd_navn_og_datatyper_input(
            [
                {
                    "individualCount": "6",
                    "validScientificName": "antall-seks",
                    "Verdi M1941": "Middels verdi",
                    "category": "LC",
                },
                {
                    "individualCount": None,
                    "validScientificName": "antall-null",
                    "Verdi M1941": "Middels verdi",
                    "category": "LC",
                },
                {
                    "individualCount": "3/1",
                    "validScientificName": "antall-brøk",
                    "Verdi M1941": "Middels verdi",
                    "category": "LC",
                },
            ]
        )

        result = rydd_navn_og_datatyper(test_df)
        antall = result.get_column("Antall")

        assert antall.dtype == pl.Int64, f"RYDD-MTM-003 Antall skal være Int64, fikk {antall.dtype}"
        assert antall.to_list() == [6, 1, 3], "RYDD-MTM-003 individualCount skal bli [6, 1, 3] for '6', null og '3/1'"
        assert result.get_column("Art").to_list() == ["antall-seks", "antall-null", "antall-brøk"], (
            "RYDD-MTM-003 like sorteringsnøkler skal beholde inputrekkefølge"
        )


    test_rydd_navn_og_datatyper_rydd_mtm_003()
    return


@app.cell(hide_code=True)
def RYDD_MTM_004(lag_rydd_navn_og_datatyper_input):
    def test_rydd_navn_og_datatyper_rydd_mtm_004():
        """RYDD-MTM-004: dato, usikkerhet og koordinater får riktige typer."""
        test_df = lag_rydd_navn_og_datatyper_input(
            [
                {
                    "validScientificName": "rad-med-komma",
                    "dateTimeCollected": datetime(2022, 5, 15, 10, 30, 0),
                    "coordinateUncertaintyInMeters": 300,
                    "latitude": "68,904168",
                    "longitude": "15,066918",
                    "Verdi M1941": "Middels verdi",
                    "category": "LC",
                },
                {
                    "validScientificName": "rad-med-punktum-og-null",
                    "dateTimeCollected": datetime(2023, 7, 4, 14, 0, 0),
                    "coordinateUncertaintyInMeters": None,
                    "latitude": "68.962388",
                    "longitude": "15.148183",
                    "Verdi M1941": "Middels verdi",
                    "category": "LC",
                },
            ]
        )

        result = rydd_navn_og_datatyper(test_df)

        obs_dato = result.get_column("Observert dato")
        assert obs_dato.dtype == pl.Date, f"RYDD-MTM-004 Observert dato skal være Date, fikk {obs_dato.dtype}"
        assert obs_dato.to_list() == [dt_date(2022, 5, 15), dt_date(2023, 7, 4)], (
            "RYDD-MTM-004 dateTimeCollected skal konverteres til dato uten klokkeslett"
        )

        usikkerhet = result.get_column("Usikkerhet meter")
        assert usikkerhet.dtype == pl.Int64, f"RYDD-MTM-004 Usikkerhet meter skal være Int64, fikk {usikkerhet.dtype}"
        assert usikkerhet.to_list() == [300, None], "RYDD-MTM-004 usikkerhet skal castes til Int64 og bevare null"

        lat = result.get_column("latitude")
        lon = result.get_column("longitude")
        assert lat.dtype == pl.Float64, f"RYDD-MTM-004 latitude skal være Float64, fikk {lat.dtype}"
        assert lon.dtype == pl.Float64, f"RYDD-MTM-004 longitude skal være Float64, fikk {lon.dtype}"
        assert abs(lat[0] - 68.904168) < 1e-6, f"RYDD-MTM-004 latitude med komma feil: {lat[0]}"
        assert abs(lat[1] - 68.962388) < 1e-6, f"RYDD-MTM-004 latitude med punktum feil: {lat[1]}"
        assert abs(lon[0] - 15.066918) < 1e-6, f"RYDD-MTM-004 longitude med komma feil: {lon[0]}"
        assert abs(lon[1] - 15.148183) < 1e-6, f"RYDD-MTM-004 longitude med punktum feil: {lon[1]}"


    test_rydd_navn_og_datatyper_rydd_mtm_004()
    return


@app.cell(hide_code=True)
def RYDD_MTM_005(lag_rydd_navn_og_datatyper_input):
    def test_rydd_navn_og_datatyper_rydd_mtm_005():
        """RYDD-MTM-005: omdøping og passthrough-verdier bevares etter sortering."""
        output_col = "Art av nasjonal forvaltningsinteresse (eks. rødlista)"
        test_df = lag_rydd_navn_og_datatyper_input(
            [
                {
                    "Verdi M1941": "Noe verdi",
                    "category": "LC",
                    output_col: "Ansvarsarter",
                    "preferredPopularName": "dompap",
                    "validScientificName": "Pyrrhula pyrrhula",
                    "behavior": "singing",
                    "FamilieNavn": "Fringillidae",
                    "OrdenNavn": "Passeriformes",
                    "taxonGroupName": "Fugler",
                    "collector": "Ola Nordmann",
                    "locality": "Sommarøyveien 21",
                    "municipality": "Øksnes",
                    "county": "Nordland",
                    "scientificNameRank": "species",
                    "Ansvarsarter": "Ja",
                    "Andre spesielt hensynskrevende arter": "Nei",
                    "Hensynskrevende arter": "Nei",
                    "Spesielle økologiske former": "Nei",
                    "Prioriterte arter": "Nei",
                    "Fredete arter": "Nei",
                    "Datamangel": "Nei",
                    "Fremmede arter": "Nei",
                    "geometry": "POINT (502688 7643678)",
                    "validScientificNameId": 4263,
                },
                {
                    "Verdi M1941": "Svært stor verdi",
                    "category": "EN",
                    output_col: "Prioriterte arter, Fredete arter",
                    "preferredPopularName": "tjeld",
                    "validScientificName": "Haematopus ostralegus",
                    "behavior": "flying",
                    "FamilieNavn": "Haematopodidae",
                    "OrdenNavn": "Charadriiformes",
                    "taxonGroupName": "Fugler",
                    "collector": "Kari Nordmann",
                    "locality": "Strengelvågfjorden",
                    "municipality": "Øksnes",
                    "county": "Nordland",
                    "scientificNameRank": "species",
                    "Ansvarsarter": "Nei",
                    "Andre spesielt hensynskrevende arter": "Ja",
                    "Hensynskrevende arter": "Nei",
                    "Spesielle økologiske former": "Ja",
                    "Prioriterte arter": "Ja",
                    "Fredete arter": "Ja",
                    "Datamangel": "Nei",
                    "Fremmede arter": "Nei",
                    "geometry": "POINT (497884 7651480)",
                    "validScientificNameId": 3664,
                },
            ]
        )

        result = rydd_navn_og_datatyper(test_df)

        assert result.get_column("Kategori").to_list() == ["EN", "LC"], "RYDD-MTM-005 category skal bli Kategori"
        assert result.get_column("Navn").to_list() == ["tjeld", "dompap"], "RYDD-MTM-005 preferredPopularName skal bli Navn"
        assert result.get_column("Art").to_list() == ["Haematopus ostralegus", "Pyrrhula pyrrhula"], (
            "RYDD-MTM-005 validScientificName skal bli Art"
        )
        assert result.get_column("Artens ID").to_list() == [3664, 4263], (
            "RYDD-MTM-005 validScientificNameId skal bli Artens ID"
        )
        assert result.get_column("Familie").to_list() == ["Haematopodidae", "Fringillidae"], (
            "RYDD-MTM-005 FamilieNavn skal bli Familie"
        )
        assert result.get_column("Orden").to_list() == ["Charadriiformes", "Passeriformes"], (
            "RYDD-MTM-005 OrdenNavn skal bli Orden"
        )
        assert result.get_column("Artsgruppe").to_list() == ["Fugler", "Fugler"], (
            "RYDD-MTM-005 taxonGroupName skal bli Artsgruppe"
        )
        assert result.get_column("Observatør").to_list() == ["Kari Nordmann", "Ola Nordmann"], (
            "RYDD-MTM-005 collector skal bli Observatør"
        )
        assert result.get_column("Lokalitet").to_list() == ["Strengelvågfjorden", "Sommarøyveien 21"], (
            "RYDD-MTM-005 locality skal bli Lokalitet"
        )
        assert result.get_column("Kommune").to_list() == ["Øksnes", "Øksnes"], "RYDD-MTM-005 municipality skal bli Kommune"
        assert result.get_column("Fylke").to_list() == ["Nordland", "Nordland"], "RYDD-MTM-005 county skal bli Fylke"
        assert result.get_column("Taksonomisk nivå").to_list() == ["species", "species"], (
            "RYDD-MTM-005 scientificNameRank skal bli Taksonomisk nivå"
        )
        assert result.get_column(output_col).to_list() == ["Prioriterte arter, Fredete arter", "Ansvarsarter"], (
            "RYDD-MTM-005 ANF-oppsummeringskolonnen skal bevares med nytt navn"
        )
        assert result.get_column("Prioriterte arter").to_list() == ["Ja", "Nei"], (
            "RYDD-MTM-005 Prioriterte arter skal bevares"
        )
        assert result.get_column("Fredete arter").to_list() == ["Ja", "Nei"], "RYDD-MTM-005 Fredete arter skal bevares"
        assert result.get_column("Ansvarsarter").to_list() == ["Nei", "Ja"], "RYDD-MTM-005 Ansvarsarter skal bevares"
        assert result.get_column("Andre spesielt hensynskrevende arter").to_list() == ["Ja", "Nei"], (
            "RYDD-MTM-005 Andre spesielt hensynskrevende arter skal bevares"
        )
        assert result.get_column("Spesielle økologiske former").to_list() == ["Ja", "Nei"], (
            "RYDD-MTM-005 Spesielle økologiske former skal bevares"
        )
        assert result.get_column("geometry").to_list() == ["POINT (497884 7651480)", "POINT (502688 7643678)"], (
            "RYDD-MTM-005 geometry skal bevares i sortert radrekkefølge"
        )


    test_rydd_navn_og_datatyper_rydd_mtm_005()
    return


@app.cell(hide_code=True)
def RYDD_MTM_006(
    lag_tom_rydd_navn_og_datatyper_input,
    rydd_navn_og_datatyper_forventede_kolonner,
):
    def test_rydd_navn_og_datatyper_rydd_mtm_006():
        """RYDD-MTM-006: tom input med riktig schema gir tom sluttabell."""
        test_df = lag_tom_rydd_navn_og_datatyper_input()

        result = rydd_navn_og_datatyper(test_df)
        expected_cols = rydd_navn_og_datatyper_forventede_kolonner()

        assert result.height == 0, "RYDD-MTM-006 tom input skal gi tom output"
        assert result.columns == expected_cols, "RYDD-MTM-006 tom output skal ha godkjent sluttkolonnerekkefølge"

        expected_dtypes = {
            "Verdi M1941": pl.Utf8,
            "Kategori": pl.Utf8,
            "Art av nasjonal forvaltningsinteresse (eks. rødlista)": pl.Utf8,
            "Navn": pl.Utf8,
            "Art": pl.Utf8,
            "Antall": pl.Int64,
            "Atferd": pl.Utf8,
            "Observert dato": pl.Date,
            "Usikkerhet meter": pl.Int64,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "Artens ID": pl.Int64,
        }
        for column_name, expected_dtype in expected_dtypes.items():
            assert result.schema[column_name] == expected_dtype, (
                f"RYDD-MTM-006 {column_name} skal ha dtype {expected_dtype}, fikk {result.schema[column_name]}"
            )


    test_rydd_navn_og_datatyper_rydd_mtm_006()
    return


@app.cell(hide_code=True)
def RYDD_MTM_007(lag_rydd_navn_og_datatyper_input):
    def test_rydd_navn_og_datatyper_rydd_mtm_007():
        """RYDD-MTM-007: manglende obligatorisk inputkolonne feiler tydelig."""
        full_df = lag_rydd_navn_og_datatyper_input()

        def assert_missing_column_raises(column_name: str) -> None:
            test_df = full_df.drop(column_name)
            try:
                rydd_navn_og_datatyper(test_df)
            except Exception as exc:
                assert column_name in str(exc), (
                    f"RYDD-MTM-007 feilmeldingen for manglende {column_name} skal nevne kolonnen; fikk {exc!r}"
                )
            else:
                raise AssertionError(f"RYDD-MTM-007 manglende {column_name} skulle gi feil")

        assert_missing_column_raises("Art av nasjonal forvaltningsinteresse (eks. rødlista)")
        assert_missing_column_raises("Hensynskrevende arter")
        assert_missing_column_raises("validScientificNameId")


    test_rydd_navn_og_datatyper_rydd_mtm_007()
    return


@app.cell(hide_code=True)
def md_manglende_artsnavn():
    mo.md(r"""
    ### Legger til manglende artsnavn
    """)
    return


@app.function(hide_code=True)
def finn_mangler_navn(df: pl.DataFrame) -> pl.DataFrame:
    """Finn unike arter som mangler norsk navn.

    Args:
        df: Slutttabell med `Art`, `Navn`, `Familie` og `Orden`.

    Returns:
        DataFrame med unike arter der `Navn` er null, sortert på `Art`.
    """

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
def definer_prompt_mangler_navn(console):
    def prompt_mangler_navn(mangler_df: pl.DataFrame) -> dict[str, str]:
        """Be brukeren fylle inn norske navn for arter som mangler navn.

        Args:
            mangler_df: DataFrame fra `finn_mangler_navn`.

        Returns:
            Mapping fra latinsk artsnavn (`Art`) til oppgitt norsk navn.

        Raises:
            typer.Exit: Når brukeren sender inn tomt navn for en art.

        Notes:
            Funksjonen skriver en Rich-tabell og leser interaktiv input fra
            terminalen.
        """

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
def test_prompt_mangler_navn_cell(prompt_mangler_navn):
    def test_prompt_mangler_navn():
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
    """Fyll manglende norske navn fra en manuell navnemapping.

    Args:
        df: DataFrame med kolonnene `Art` og `Navn`.
        navn_mapping: Mapping fra latinsk artsnavn til norsk navn.

    Returns:
        DataFrame der nullverdier i `Navn` er fylt når `Art` finnes i
        mappingen. Eksisterende navn overskrives ikke.
    """

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
def md_cli_og_pipeline():
    mo.md(r"""
    ### Setter sammen alt og definerer rich
    """)
    return


@app.cell
def definer_les_data_cli(
    console,
    les_data_og_kjør_alle_funksjoner,
    prompt_mangler_navn,
):
    cli_app = typer.Typer()


    @cli_app.command()
    def les_data_cli(
        input_fil_sti: str = typer.Argument(..., help="Sti til CSV-fil med fugledata"),
        filter_year: int = typer.Option(1990, help="Filtrer observasjoner fra og med dette året"),
        output: str = typer.Option("output.parquet", help="Sti til utfil (Parquet)"),
    ):
        """Kjør hele databehandlingsløpet fra kommandolinjen.

        Args:
            input_fil_sti: Sti til CSV-fil fra Artskart.
            filter_year: Første observasjonsår som beholdes.
            output: Sti til Parquet-filen som skal skrives.

        Notes:
            Kommandoen leser data, beriker, spør om manglende norske navn og
            skriver resultatet til disk.
        """
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
            df.filter(pl.col("Art av nasjonal forvaltningsinteresse") == "Treff ikke funnet i ANF tabell ").height
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
def definer_les_data_og_kjor_alle_funksjoner(
    console,
    legg_til_arter_av_nasjonal_forvaltningsinteresse,
    process_and_enrich_data,
):
    def les_data_og_kjør_alle_funksjoner(input_fil_sti: str, filter_year: int = 1990) -> pl.DataFrame:
        """Les Artskart-CSV, filtrer på år og kjør hele berikingsløpet.

        Args:
            input_fil_sti: Sti til CSV-fil fra Artskart.
            filter_year: Første observasjonsår som beholdes.

        Returns:
            Ferdig behandlet DataFrame klar for eksport.

        Raises:
            ValueError: Når input ikke følger Artskart-kontrakten eller mangler
                gyldige ID-er.
            RuntimeError: Når NorTaxa-oppslag feiler.

        Notes:
            Observasjoner uten dato fjernes av årfilteret. Funksjonen skriver
            status og advarsler til konsollen.
        """

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

        with console.status("[bold blue]Beregner M1941-verdi fra rødlistekategori..."):
            df_steg0 = df_artsdatabanken.pipe(legg_til_verdi_m1941)
        console.print("  [green]✓[/green] Beregnet M1941-verdi fra rødlistekategori")

        with console.status("[bold blue]Legger til kriterier for nasjonal interesse..."):
            df_steg1 = df_steg0.pipe(legg_til_arter_av_nasjonal_forvaltningsinteresse)
        console.print("  [green]✓[/green] Lagt til Arter av nasjonal forvaltningsinteresse")

        antall_arter_uten_verdi_m1941 = (
            df_steg1.filter(pl.col("Verdi M1941").is_null())
            .select(["validScientificNameId", "validScientificName"])
            .unique()
            .height
        )
        if antall_arter_uten_verdi_m1941 > 0:
            art_tekst = "art" if antall_arter_uten_verdi_m1941 == 1 else "arter"
            console.print(
                f"  [yellow]Advarsel:[/yellow] {antall_arter_uten_verdi_m1941} {art_tekst} "
                "har manglende verdi i Verdi M1941"
            )

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
