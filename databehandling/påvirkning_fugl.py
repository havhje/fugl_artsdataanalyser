import marimo

__generated_with = "0.23.16"
app = marimo.App(width="columns")

with app.setup(hide_code=True):
    from pathlib import Path

    import duckdb
    import marimo as mo
    import polars as pl
    import pytest
    import typer
    from rich.console import Console
    from rich.panel import Panel
    from rich.rule import Rule

    DATAMAPPE = Path(__file__).parent
    # Påvirkningsdatabasen er eneste kilde for hekkebestand og kollisjonsrisiko.
    DATABASE_STI = DATAMAPPE / "påvirkning_fugl"
    TABELLER = ["hekkebestand", "kollisjonsrisiko_hekkebestand", "synonymer"]

    # Kollisjonstabellen inneholder alle hekkebestandskolonnene pluss gradene fra
    # Bernotat & Dierschke, så den er eneste kilde for koblingen.
    ATTRIBUTTABELL = "kollisjonsrisiko_hekkebestand"

    # Kolonner som identifiserer taksonet i arket. De finnes allerede i
    # observasjonene og kopieres derfor ikke inn som attributter.
    ARK_IDENTITETSKOLONNER = [
        "Navn",
        "Art",
        "Artens ID",
        "Taksonomisk nivå",
        "ArtNavnId",
    ]
    # Utgåtte NorTaxa-ID-er som Artskart fortsatt bruker → gyldig ID i arket.
    SYNONYMTABELL = "synonymer"
    KOBLING_KOLONNE = "Påvirkning kobling"
    KOBLET_ART_KOLONNE = "Påvirkning art"


@app.cell(hide_code=True)
def md_intro():
    mo.md(r"""
    # Påvirkning fugl

    Kobler hekkebestand og kollisjonsrisiko fra DuckDB-filen `påvirkning_fugl`
    til observasjonene som `databehandling.py` eksporterer.

    **Koblingsregel:**

    1. Først `Artens ID` mot arkets `Artens ID`. Underarter med egen rad i arket,
       som polarsisik og svarthalespove *islandica*, får dermed sine egne verdier.
    2. Uten treff: er `Artens ID` et synonym i tabellen `synonymer`, brukes den
       gyldige ID-en. Artskart gir for eksempel kråke den utgåtte ID-en 4164
       (*Corvus cornix*), mens NorTaxa og arket bruker 4166 (*Corvus corone*).
    3. Uten treff: observasjonens `ArtNavnId` (foreldrearten) mot arkets rad med
       `Taksonomisk nivå = species`. Slik får for eksempel *Motacilla alba subsp. alba*
       verdiene til linerle.
    4. Uten treff i noen av stegene blir alle påvirkningskolonnene tomme (null).

    `Påvirkning kobling` viser hvilket steg som ga treff, og `Påvirkning art`
    viser hvilken rad i arket verdiene kommer fra.
    """)
    return


@app.function(hide_code=True)
def valider_påvirkningsattributter(df: pl.DataFrame, kilde: str) -> None:
    """Fail early when a sheet cannot be joined unambiguously on `Artens ID`.

    Args:
        df: Sheet with one row per taxon.
        kilde: Name used in error messages.

    Raises:
        ValueError: When required columns are missing, or `Artens ID` is
            empty or not unique.
    """
    påkrevde = {"Artens ID", "Art", "Taksonomisk nivå"}
    mangler = sorted(påkrevde - set(df.columns))
    if mangler:
        raise ValueError(f"{kilde} mangler kolonnene: {', '.join(mangler)}")

    tomme = df.filter(pl.col("Artens ID").is_null())
    if not tomme.is_empty():
        raise ValueError(
            f"{kilde} har {tomme.height} rader uten Artens ID: "
            f"{', '.join(tomme['Art'].cast(pl.String).fill_null('<uten navn>'))}"
        )

    duplikater = df.filter(pl.col("Artens ID").is_duplicated())
    if not duplikater.is_empty():
        ider = sorted(set(duplikater["Artens ID"].to_list()))
        raise ValueError(
            f"{kilde} har dupliserte Artens ID: {ider}. "
            "Hver ID må ha én rad for at koblingen skal være entydig."
        )


@app.cell(hide_code=True)
def md_koble_paavirkning():
    mo.md(r"""
    ## Koble påvirkning til observasjoner

    **Inputkontrakt:** `observasjoner` er output fra `databehandling.py` med
    `Artens ID` og `ArtNavnId` (Int64). Arket har unik `Artens ID`,
    `Art` og `Taksonomisk nivå`. Synonymtabellen har unik `Artens ID` som ikke
    finnes i arket, og en `Gyldig Artens ID` som finnes i arket.

    **Outputkontrakt:** Samme rader, radrekkefølge og kolonner som input, pluss
    `Påvirkning kobling` (`Artens ID`, `synonym`, `ArtNavnId` eller null), `Påvirkning art`
    og arkets attributtkolonner. Rader uten treff får null i alle nye kolonner.
    """)
    return


@app.function(hide_code=True)
def les_påvirkningsattributter(
    database: duckdb.DuckDBPyConnection, tabell: str = ATTRIBUTTABELL
) -> pl.DataFrame:
    """Read and validate the attribute sheet from the påvirkning database.

    Args:
        database: Open connection to the påvirkning DuckDB file.
        tabell: Table to read.

    Returns:
        The sheet as a Polars DataFrame.

    Raises:
        RuntimeError: When the table cannot be read.
        ValueError: When the sheet cannot be joined unambiguously.
    """
    try:
        df = database.table(tabell).pl()
    except duckdb.Error as exc:
        raise RuntimeError(
            f"Kunne ikke lese tabellen {tabell} fra påvirkningsdatabasen. "
            f"Sjekk at {DATABASE_STI} finnes og har tabellen."
        ) from exc
    valider_påvirkningsattributter(df, kilde=f"tabellen {tabell}")
    return df


@app.function(hide_code=True)
def les_synonymer(database: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Read the synonym table from the påvirkning database.

    Args:
        database: Open connection to the påvirkning DuckDB file.

    Returns:
        Synonyms with `Artens ID` (outdated ID) and `Gyldig Artens ID`.

    Raises:
        RuntimeError: When the table cannot be read.
    """
    try:
        return database.table(SYNONYMTABELL).pl()
    except duckdb.Error as exc:
        raise RuntimeError(
            f"Kunne ikke lese tabellen {SYNONYMTABELL} fra påvirkningsdatabasen. "
            f"Sjekk at {DATABASE_STI} finnes og har tabellen."
        ) from exc


@app.function(hide_code=True)
def valider_synonymer(synonymer: pl.DataFrame, attributter: pl.DataFrame) -> None:
    """Fail early when a synonym would be ambiguous or point nowhere.

    Args:
        synonymer: Table with `Artens ID` (outdated) and `Gyldig Artens ID`.
        attributter: The sheet the valid IDs must exist in.

    Raises:
        ValueError: When columns are missing, IDs are empty or duplicated, a
            synonym ID is itself in the sheet, or a valid ID is not.
    """
    mangler = sorted({"Artens ID", "Gyldig Artens ID"} - set(synonymer.columns))
    if mangler:
        raise ValueError(f"Synonymtabellen mangler kolonnene: {', '.join(mangler)}")

    if (
        synonymer.select(pl.col("Artens ID", "Gyldig Artens ID").null_count())
        .sum_horizontal()
        .item()
    ):
        raise ValueError(
            "Synonymtabellen har rader uten Artens ID eller Gyldig Artens ID"
        )

    duplikater = synonymer.filter(pl.col("Artens ID").is_duplicated())
    if not duplikater.is_empty():
        ider = sorted(set(duplikater["Artens ID"].to_list()))
        raise ValueError(f"Synonymtabellen har dupliserte Artens ID: {ider}")

    ark_ider = set(attributter["Artens ID"].to_list())
    i_arket = sorted(set(synonymer["Artens ID"].to_list()) & ark_ider)
    if i_arket:
        raise ValueError(
            f"Synonym-ID-ene {i_arket} finnes også i arket. "
            "Fjern synonymraden eller arkraden, ellers er koblingen tvetydig."
        )

    uten_mål = sorted(set(synonymer["Gyldig Artens ID"].to_list()) - ark_ider)
    if uten_mål:
        raise ValueError(
            f"Gyldig Artens ID {uten_mål} i synonymtabellen finnes ikke i arket"
        )


@app.function(hide_code=True)
def legg_til_påvirkning(
    observasjoner: pl.DataFrame,
    attributter: pl.DataFrame,
    synonymer: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Join sheet attributes to observations, falling back to the parent species.

    Args:
        observasjoner: Output from databehandling with `Artens ID` and
            `ArtNavnId`.
        attributter: Sheet with unique `Artens ID`, `Art` and
            `Taksonomisk nivå`.
        synonymer: Optional outdated `Artens ID` → `Gyldig Artens ID` in the
            sheet, tried after the direct match and before `ArtNavnId`.

    Returns:
        Observations with `Påvirkning kobling`, `Påvirkning art` and the
        sheet's attribute columns appended. Unmatched rows get nulls.

    Raises:
        ValueError: When required columns are missing, the sheet has an
            ambiguous key, or attribute names already exist in the
            observations.
    """
    mangler = sorted({"Artens ID", "ArtNavnId"} - set(observasjoner.columns))
    if mangler:
        raise ValueError(
            f"Observasjonene mangler kolonnene: {', '.join(mangler)}. "
            "Kjør databehandling.py på nytt; eksporten har ArtNavnId fra og med "
            "oktober 2026."
        )
    valider_påvirkningsattributter(attributter, kilde="påvirkningsarket")
    if synonymer is None:
        synonymer = pl.DataFrame(
            schema={"Artens ID": pl.Int64, "Gyldig Artens ID": pl.Int64}
        )
    valider_synonymer(synonymer, attributter)

    attributtkolonner = [
        kolonne
        for kolonne in attributter.columns
        if kolonne not in ARK_IDENTITETSKOLONNER
    ]
    nye_kolonner = [KOBLING_KOLONNE, KOBLET_ART_KOLONNE, *attributtkolonner]
    kollisjoner = sorted(set(nye_kolonner) & set(observasjoner.columns))
    if kollisjoner:
        raise ValueError(
            f"Observasjonene har allerede kolonnene: {', '.join(kollisjoner)}. "
            "Påvirkning er trolig koblet fra før."
        )

    alle_ider = attributter["Artens ID"].cast(pl.Int64).implode()
    art_ider = (
        attributter.filter(pl.col("Taksonomisk nivå") == "species")["Artens ID"]
        .cast(pl.Int64)
        .implode()
    )

    gyldig_via_synonym = pl.col("Artens ID").replace_strict(
        synonymer["Artens ID"].cast(pl.Int64),
        synonymer["Gyldig Artens ID"].cast(pl.Int64),
        default=None,
        return_dtype=pl.Int64,
    )

    med_nøkkel = observasjoner.with_columns(
        pl.when(pl.col("Artens ID").is_in(alle_ider))
        .then(pl.lit("Artens ID"))
        .when(gyldig_via_synonym.is_not_null())
        .then(pl.lit("synonym"))
        .when(pl.col("ArtNavnId").is_in(art_ider))
        .then(pl.lit("ArtNavnId"))
        .otherwise(None)
        .alias(KOBLING_KOLONNE)
    ).with_columns(
        pl.when(pl.col(KOBLING_KOLONNE) == "Artens ID")
        .then(pl.col("Artens ID"))
        .when(pl.col(KOBLING_KOLONNE) == "synonym")
        .then(gyldig_via_synonym)
        .when(pl.col(KOBLING_KOLONNE) == "ArtNavnId")
        .then(pl.col("ArtNavnId"))
        .otherwise(None)
        .cast(pl.Int64)
        .alias("_påvirkning_id")
    )

    ark = attributter.select(
        pl.col("Artens ID").cast(pl.Int64).alias("_påvirkning_id"),
        pl.col("Art").alias(KOBLET_ART_KOLONNE),
        *attributtkolonner,
    )

    return med_nøkkel.join(
        ark, on="_påvirkning_id", how="left", validate="m:1", maintain_order="left"
    ).drop("_påvirkning_id")


@app.cell(hide_code=True)
def md_testmatrise_legg_til_paavirkning():
    mo.md(r"""
    ### Testmatrise for `legg_til_påvirkning` og påvirkningsdatabasen

    | ID | Scenario | Forventet | Testcelle |
    |---|---|---|---|
    | PAAV-MTM-001 | Underarter med egen rad og samme foreldreart (polarsisik/brunsisik) | Hver får sin egen rad via `Artens ID`; foreldrearten via egen ID | `PAAV_MTM_001` |
    | PAAV-MTM-002 | Underart uten egen rad (*Motacilla alba subsp. alba*) | Får artsraden via `ArtNavnId` | `PAAV_MTM_002` |
    | PAAV-MTM-003 | Ingen treff, og `ArtNavnId` som bare finnes som underartsrad | Null i alle nye kolonner | `PAAV_MTM_003` |
    | PAAV-MTM-004 | Radantall, rekkefølge og originalkolonner | Bevares; nye kolonner kommer sist i fast rekkefølge | `PAAV_MTM_004` |
    | PAAV-MTM-005 | Manglende `ArtNavnId`, duplisert arks-ID eller kolonnekollisjon | `ValueError` med forklarende tekst | `PAAV_MTM_005` |
    | PAAV-MTM-006 | Tom input | Tom output med alle nye kolonner | `PAAV_MTM_006` |
    | PAAV-MTM-007 | Les påvirkningsdatabasen | Alle tre tabeller; arkene har 259 rader, unik `Artens ID` og tallkolonner med tallformat; synonymene er gyldige | `PAAV_MTM_007` |
    | PAAV-MTM-008 | Utgått ID i synonymtabellen (kråke 4164 → 4166) | Får arkraden for gyldig ID med `Påvirkning kobling = synonym`; direkte treff går foran | `PAAV_MTM_008` |
    | PAAV-MTM-009 | Ugyldig synonymtabell | `ValueError` ved duplikat, synonym-ID i arket eller gyldig ID utenfor arket | `PAAV_MTM_009` |
    """)
    return


@app.cell(hide_code=True)
def paavirkning_testhjelpere():
    def lag_testark() -> pl.DataFrame:
        """Small sheet covering species and subspecies rows."""
        return pl.DataFrame(
            {
                "Navn": [
                    "kråke",
                    "gråsisik",
                    "brunsisik",
                    "polarsisik",
                    "linerle",
                    "sørlig myrsnipe",
                ],
                "Art": [
                    "Corvus corone",
                    "Acanthis flammea",
                    "Acanthis flammea subsp. cabaret",
                    "Acanthis flammea subsp. exilipes",
                    "Motacilla alba",
                    "Calidris alpina subsp. schinzii",
                ],
                "Artens ID": [4166, 185969, 296650, 296651, 4332, 3736],
                "Taksonomisk nivå": [
                    "species",
                    "species",
                    "subspecies",
                    "subspecies",
                    "species",
                    "subspecies",
                ],
                "ArtNavnId": [4166, 185969, 185969, 185969, 4332, 3732],
                "Trend": [
                    "stabil",
                    "stabil",
                    "usikker",
                    "fluktuerende",
                    "stabil",
                    None,
                ],
                "de_grad_1_5": [4, 5, 4, 3, 5, 2],
            }
        )

    def lag_testobservasjoner(rader: list[tuple[str, int, int]]) -> pl.DataFrame:
        """Observations as (obs_id, Artens ID, ArtNavnId)."""
        return pl.DataFrame(
            rader,
            schema={"obs_id": pl.String, "Artens ID": pl.Int64, "ArtNavnId": pl.Int64},
            orient="row",
        )

    return lag_testark, lag_testobservasjoner


@app.cell(hide_code=True)
def PAAV_MTM_001(lag_testark, lag_testobservasjoner):
    def test_legg_til_påvirkning_paav_mtm_001():
        """PAAV-MTM-001: subspecies rows stay separate from each other and the species."""
        obs = lag_testobservasjoner(
            [("a", 296651, 185969), ("b", 296650, 185969), ("c", 185969, 185969)]
        )
        resultat = legg_til_påvirkning(obs, lag_testark())

        assert resultat[KOBLING_KOLONNE].to_list() == ["Artens ID"] * 3
        assert resultat["Trend"].to_list() == ["fluktuerende", "usikker", "stabil"]
        assert resultat["de_grad_1_5"].to_list() == [3, 4, 5]
        assert resultat[KOBLET_ART_KOLONNE].to_list() == [
            "Acanthis flammea subsp. exilipes",
            "Acanthis flammea subsp. cabaret",
            "Acanthis flammea",
        ]

    test_legg_til_påvirkning_paav_mtm_001()
    return


@app.cell(hide_code=True)
def PAAV_MTM_002(lag_testark, lag_testobservasjoner):
    def test_legg_til_påvirkning_paav_mtm_002():
        """PAAV-MTM-002: subspecies without its own row falls back to the species row."""
        obs = lag_testobservasjoner([("a", 4333, 4332)])
        resultat = legg_til_påvirkning(obs, lag_testark())

        assert resultat[KOBLING_KOLONNE].to_list() == ["ArtNavnId"]
        assert resultat[KOBLET_ART_KOLONNE].to_list() == ["Motacilla alba"]
        assert resultat["Trend"].to_list() == ["stabil"]
        assert resultat["de_grad_1_5"].to_list() == [5]

    test_legg_til_påvirkning_paav_mtm_002()
    return


@app.cell(hide_code=True)
def PAAV_MTM_003(lag_testark, lag_testobservasjoner):
    def test_legg_til_påvirkning_paav_mtm_003():
        """PAAV-MTM-003: no match gives nulls; fallback only uses species rows."""
        obs = lag_testobservasjoner(
            [
                ("ukjent", 999999, 999999),
                # Calidris alpina subsp. alpina: foreldrearten 3732 har ingen artsrad,
                # bare underarten schinzii (3736), så den skal ikke få treff.
                ("myrsnipe", 3733, 3732),
                ("uten_artnavnid", 888888, None),
            ]
        )
        resultat = legg_til_påvirkning(obs, lag_testark())

        nye = [KOBLING_KOLONNE, KOBLET_ART_KOLONNE, "Trend", "de_grad_1_5"]
        assert resultat.select(nye).null_count().row(0) == (3, 3, 3, 3)

    test_legg_til_påvirkning_paav_mtm_003()
    return


@app.cell(hide_code=True)
def PAAV_MTM_004(lag_testark, lag_testobservasjoner):
    def test_legg_til_påvirkning_paav_mtm_004():
        """PAAV-MTM-004: rows, order and original columns are preserved."""
        obs = lag_testobservasjoner(
            [
                ("c", 4333, 4332),
                ("a", 999999, 999999),
                ("b", 296651, 185969),
                ("d", 4333, 4332),
            ]
        ).with_columns(pl.lit("beholdes").alias("Kommune"))
        resultat = legg_til_påvirkning(obs, lag_testark())

        assert resultat.height == obs.height
        assert resultat.select(obs.columns).equals(obs)
        assert resultat.columns == [
            *obs.columns,
            KOBLING_KOLONNE,
            KOBLET_ART_KOLONNE,
            "Trend",
            "de_grad_1_5",
        ]

    test_legg_til_påvirkning_paav_mtm_004()
    return


@app.cell(hide_code=True)
def PAAV_MTM_005(lag_testark, lag_testobservasjoner):
    def test_legg_til_påvirkning_paav_mtm_005():
        """PAAV-MTM-005: invalid input fails early with a helpful message."""
        obs = lag_testobservasjoner([("a", 4332, 4332)])
        ark = lag_testark()

        with pytest.raises(ValueError, match="ArtNavnId"):
            legg_til_påvirkning(obs.drop("ArtNavnId"), ark)

        with pytest.raises(ValueError, match="dupliserte Artens ID: \\[4332\\]"):
            legg_til_påvirkning(
                obs, pl.concat([ark, ark.filter(pl.col("Artens ID") == 4332)])
            )

        with pytest.raises(ValueError, match="Trend"):
            legg_til_påvirkning(obs.with_columns(pl.lit("x").alias("Trend")), ark)

    test_legg_til_påvirkning_paav_mtm_005()
    return


@app.cell(hide_code=True)
def PAAV_MTM_006(lag_testark, lag_testobservasjoner):
    def test_legg_til_påvirkning_paav_mtm_006():
        """PAAV-MTM-006: empty input gives empty output with the new columns."""
        resultat = legg_til_påvirkning(lag_testobservasjoner([]), lag_testark())

        assert resultat.height == 0
        assert resultat.columns[-4:] == [
            KOBLING_KOLONNE,
            KOBLET_ART_KOLONNE,
            "Trend",
            "de_grad_1_5",
        ]

    test_legg_til_påvirkning_paav_mtm_006()
    return


@app.cell(hide_code=True)
def PAAV_MTM_007():
    def test_påvirkningsdatabase_paav_mtm_007():
        """PAAV-MTM-007: the database has both typed tables with unique keys."""
        with duckdb.connect(str(DATABASE_STI), read_only=True) as database:
            tabeller = {rad[0] for rad in database.sql("SHOW TABLES").fetchall()}
            hekkebestand = database.table("hekkebestand").pl()
            attributter = les_påvirkningsattributter(database)
            synonymer = les_synonymer(database)

        assert tabeller == set(TABELLER)
        valider_påvirkningsattributter(hekkebestand, kilde="tabellen hekkebestand")
        assert hekkebestand.height == attributter.height == 259
        for kolonne in ["Artens ID", "ArtNavnId", "de_grad_1_5", "de_side"]:
            assert attributter.schema[kolonne] == pl.Int64, kolonne
        assert attributter.schema["Gjennomsnitt, par"] == pl.Float64
        valider_synonymer(synonymer, attributter)
        assert synonymer.filter(pl.col("Artens ID") == 4164)[
            "Gyldig Artens ID"
        ].to_list() == [4166]

    test_påvirkningsdatabase_paav_mtm_007()
    return


@app.cell(hide_code=True)
def paavirkning_synonym_testhjelper():
    def lag_testsynonymer(rader: list[tuple[int, int]]) -> pl.DataFrame:
        """Synonyms as (outdated Artens ID, valid Artens ID)."""
        return pl.DataFrame(
            rader,
            schema={"Artens ID": pl.Int64, "Gyldig Artens ID": pl.Int64},
            orient="row",
        )

    return (lag_testsynonymer,)


@app.cell(hide_code=True)
def PAAV_MTM_008(lag_testark, lag_testobservasjoner, lag_testsynonymer):
    def test_legg_til_påvirkning_paav_mtm_008():
        """PAAV-MTM-008: an outdated ID reaches the sheet row via the synonym table."""
        obs = lag_testobservasjoner([("kråke", 4164, 4164), ("direkte", 4166, 4166)])
        resultat = legg_til_påvirkning(
            obs, lag_testark(), lag_testsynonymer([(4164, 4166)])
        )

        assert resultat[KOBLING_KOLONNE].to_list() == ["synonym", "Artens ID"]
        assert resultat[KOBLET_ART_KOLONNE].to_list() == ["Corvus corone"] * 2
        assert resultat["de_grad_1_5"].to_list() == [4, 4]
        assert resultat["Artens ID"].to_list() == [4164, 4166], (
            "Artens ID skal ikke endres"
        )

        uten_synonymer = legg_til_påvirkning(obs, lag_testark())
        assert uten_synonymer[KOBLING_KOLONNE].to_list() == [None, "Artens ID"]

    test_legg_til_påvirkning_paav_mtm_008()
    return


@app.cell(hide_code=True)
def PAAV_MTM_009(lag_testark, lag_testobservasjoner, lag_testsynonymer):
    def test_legg_til_påvirkning_paav_mtm_009():
        """PAAV-MTM-009: an invalid synonym table fails early."""
        obs = lag_testobservasjoner([("kråke", 4164, 4164)])
        ark = lag_testark()

        with pytest.raises(ValueError, match="dupliserte Artens ID: \\[4164\\]"):
            legg_til_påvirkning(
                obs, ark, lag_testsynonymer([(4164, 4166), (4164, 4332)])
            )

        with pytest.raises(ValueError, match="finnes også i arket"):
            legg_til_påvirkning(obs, ark, lag_testsynonymer([(4332, 4166)]))

        with pytest.raises(ValueError, match="finnes ikke i arket"):
            legg_til_påvirkning(obs, ark, lag_testsynonymer([(4164, 999999)]))

    test_legg_til_påvirkning_paav_mtm_009()
    return


@app.cell(column=1, hide_code=True)
def md_kjoring():
    mo.md(r"""
    ## Kjøring

    ```
    uv run databehandling/påvirkning_fugl.py output.parquet --output output_påvirkning.parquet
    ```

    Hekkebestand og kollisjonsrisiko vedlikeholdes direkte i DuckDB-filen
    `påvirkning_fugl`; det finnes ingen CSV-kilde.

    Under vises koblingen mot `output.parquet` i prosjektroten, når filen finnes.
    """)
    return


@app.cell
def opprett_console():
    console = Console()
    return (console,)


@app.cell
def definer_paavirkning_cli(console):
    cli_app = typer.Typer()

    @cli_app.command()
    def koble_cli(
        input_fil_sti: str = typer.Argument(
            ..., help="Parquet-fil fra databehandling.py"
        ),
        output: str = typer.Option(
            "output_påvirkning.parquet", help="Sti til utfil (Parquet)"
        ),
    ):
        """Koble hekkebestand og kollisjonsrisiko til observasjonene."""
        console.print(Rule("[bold]Kobler påvirkning til observasjoner[/bold]"))
        with duckdb.connect(str(DATABASE_STI), read_only=True) as database:
            attributter = les_påvirkningsattributter(database)
            synonymer = les_synonymer(database)
        df = legg_til_påvirkning(pl.read_parquet(input_fil_sti), attributter, synonymer)
        df.write_parquet(output)

        antall = df[KOBLING_KOLONNE].value_counts()
        treff = dict(zip(antall[KOBLING_KOLONNE], antall["count"], strict=True))
        oppsummering = (
            f"[bold green]Ferdig![/bold green]\n\n"
            f"  Rader:                [cyan]{df.height}[/cyan]\n"
            f"  Treff på Artens ID:   [cyan]{treff.get('Artens ID', 0)}[/cyan]\n"
            f"  Treff via synonym:    [cyan]{treff.get('synonym', 0)}[/cyan]\n"
            f"  Treff via ArtNavnId:  [cyan]{treff.get('ArtNavnId', 0)}[/cyan]\n"
            f"  Uten treff:           [cyan]{treff.get(None, 0)}[/cyan]\n"
            f"  Skrevet til:          [cyan]{output}[/cyan]"
        )
        console.print(Panel(oppsummering, title="Oppsummering", border_style="green"))

    # Kjør Typer CLI kun i script-modus (uv run påvirkning_fugl.py ...)
    if mo.app_meta().mode == "script":
        cli_app()
    return


@app.cell
def les_paavirkningsdatabase():
    mo.stop(
        not DATABASE_STI.exists(),
        mo.md(f"Fant ikke påvirkningsdatabasen `{DATABASE_STI}`."),
    )
    with duckdb.connect(str(DATABASE_STI), read_only=True) as _database:
        attributter = les_påvirkningsattributter(_database)
        synonymer = les_synonymer(_database)
    attributter
    return attributter, synonymer


@app.cell
def koble_output_parquet(attributter, synonymer):
    _output_sti = DATAMAPPE.parent / "output.parquet"
    mo.stop(
        not _output_sti.exists(),
        mo.md("Fant ikke `output.parquet` i prosjektroten."),
    )
    _observasjoner = pl.read_parquet(_output_sti)
    mo.stop(
        "ArtNavnId" not in _observasjoner.columns,
        mo.md(
            "`output.parquet` mangler `ArtNavnId`. Kjør `databehandling.py` på nytt "
            "for å lage en ny eksport."
        ),
    )
    observasjoner_med_påvirkning = legg_til_påvirkning(
        _observasjoner, attributter, synonymer
    )
    observasjoner_med_påvirkning
    return (observasjoner_med_påvirkning,)


@app.cell
def oppsummer_kobling(observasjoner_med_påvirkning):
    (
        observasjoner_med_påvirkning.group_by(
            "Art", "Artens ID", KOBLING_KOLONNE, KOBLET_ART_KOLONNE
        )
        .len("Observasjoner")
        .sort(KOBLING_KOLONNE, "Art", nulls_last=True)
    )
    return


if __name__ == "__main__":
    app.run()
