import marimo

__generated_with = "0.23.16"
app = marimo.App(width="columns", layout_file="layouts/data_analyse.grid.json")

with app.setup(hide_code=True):
    import inspect
    import textwrap
    from datetime import date
    from html import escape

    import altair as alt
    import colorcet as cc
    import datashader as ds
    import geopandas as gpd
    import great_tables as gt
    import hvplot.polars
    import marimo as mo
    import plotly.express as px
    import polars as pl
    from holoviews.element.tiles import EsriImagery

    ARTSSTATISTIKK_INPUTKOLONNER = {
        "Artens ID",
        "Art",
        "Navn",
        "Kategori",
        "Verdi M1941",
        "Art av nasjonal forvaltningsinteresse (eks. rødlista)",
        "Antall",
        "Atferd",
        "Observert dato",
        "Familie",
        "Orden",
    }

    ARTSSTATISTIKK_OUTPUTKOLONNER = [
        "Artens ID",
        "Verdi M1941",
        "Kategori",
        "Forvaltningsinteresse",
        "Navn",
        "Art",
        "Observasjoner",
        "Individer",
        "Gj.snitt individer",
        "År-periode",
        "Måneder",
        "Månedsprofil",
        "Familie",
        "Orden",
        "Reproduksjon",
        "Mulig reproduksjon",
    ]

    ARTSSTATISTIKK_METADATAKOLONNER = [
        "Navn",
        "Kategori",
        "Verdi M1941",
        "Art av nasjonal forvaltningsinteresse (eks. rødlista)",
        "Familie",
        "Orden",
    ]

    ARTSSTATISTIKK_TEKSTKOLONNER = {
        "Art",
        "Navn",
        "Kategori",
        "Verdi M1941",
        "Art av nasjonal forvaltningsinteresse (eks. rødlista)",
        "Atferd",
        "Familie",
        "Orden",
    }

    ARTSSTATISTIKK_MAANEDSNAVN = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "Mai",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Okt",
        11: "Nov",
        12: "Des",
    }

    ARTSSTATISTIKK_KATEGORI_REKKEFOELGE = {
        kategori: indeks
        for indeks, kategori in enumerate(
            [
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
            ]
        )
    }

    ARTSSTATISTIKK_M1941_REKKEFOELGE = {
        verdi: indeks
        for indeks, verdi in enumerate(
            [
                "Svært stor verdi",
                "Stor verdi",
                "Middels verdi",
                "Noe verdi",
                "Uten betydning for KU",
            ]
        )
    }

    ARTSANTALL_ETIKETT = "Arter (inkl. underarter)"
    ARTSANTALL_MERKNAD = (
        "Tellingen følger kildens ID og navn; også høyere taksonomiske nivåer "
        "og navnevarianter kan inngå."
    )

    # Gjeldende risikokategorifarger fra Artsdatabanken.
    ARTSSTATISTIKK_KATEGORIFARGER = {
        "RE": "#262F31",
        "CR": "#D61900",
        "EN": "#F34F39",
        "VU": "#EB8107",
        "NT": "#E6C000",
        "LC": "#61A360",
        "DD": "#6C6C6C",
        "SE": "#4E1A53",
        "HI": "#17467C",
        "PH": "#286371",
        "LO": "#7CB1AC",
        "NK": "#D2CF84",
        "NA": "#FFFFFF",
        "NE": "#FFFFFF",
        "Unknown": "#D9D9D9",
    }

    # Offisielle HEX-koder fra M-1941, tabell 1-11.
    ARTSSTATISTIKK_M1941_FARGER = {
        "Svært stor verdi": "#AF0F0F",
        "Stor verdi": "#FD7032",
        "Middels verdi": "#FEC02D",
        "Noe verdi": "#FFFF00",
        "Uten betydning for KU": "#D9D9D9",
    }


@app.cell(hide_code=True)
def _():
    valgt_fil = mo.ui.file_browser(
        filetypes=[".parquet"],
        multiple=False,
        label="Velg ferdig behandlet Parquet-fil",
    )
    valgt_fil
    return (valgt_fil,)


@app.cell(hide_code=True)
def artsstatistikk_dokumentasjon(
    artsstatistikk_forventede_kolonner,
    hent_påkrevde_artsstatistikk_kolonner,
    lag_artsstatistikk,
    lag_artsstatistikk_tabell,
    lag_artsstatistikk_testinput,
    lag_tom_artsstatistikk_testinput,
    test_artsstatistikk_mtm_001,
    test_artsstatistikk_mtm_002,
    test_artsstatistikk_mtm_003,
    test_artsstatistikk_mtm_004,
    test_artsstatistikk_mtm_005,
    test_artsstatistikk_mtm_006,
    test_artsstatistikk_mtm_007,
    test_artsstatistikk_mtm_008,
    valider_artsstatistikk_input,
):
    def _vis_kildekode(_funksjon):
        _kildekode = textwrap.dedent(inspect.getsource(_funksjon)).strip()
        return mo.md(f"#### `{_funksjon.__name__}`\n\n```python\n{_kildekode}\n```")

    _funksjoner = [
        hent_påkrevde_artsstatistikk_kolonner,
        valider_artsstatistikk_input,
        lag_artsstatistikk,
        lag_artsstatistikk_tabell,
    ]
    _testhjelpere = [
        lag_artsstatistikk_testinput,
        lag_tom_artsstatistikk_testinput,
        artsstatistikk_forventede_kolonner,
    ]
    _tester = [
        test_artsstatistikk_mtm_001,
        test_artsstatistikk_mtm_002,
        test_artsstatistikk_mtm_003,
        test_artsstatistikk_mtm_004,
        test_artsstatistikk_mtm_005,
        test_artsstatistikk_mtm_006,
        test_artsstatistikk_mtm_007,
        test_artsstatistikk_mtm_008,
    ]
    _innhold = mo.vstack(
        [
            mo.md(r"""
    ### Funksjonsstruktur

    1. `hent_påkrevde_artsstatistikk_kolonner` beskriver inputkontrakten.
    2. `valider_artsstatistikk_input` validerer kolonner, typer, kategorier og metadata.
    3. `lag_artsstatistikk` aggregerer observasjoner til én rad per kildepar `(Artens ID, Art)`.
    4. `lag_artsstatistikk_tabell` formaterer resultatet som en Great Table.

    ### Funksjoner
    """),
            *[_vis_kildekode(_funksjon) for _funksjon in _funksjoner],
            mo.md(r"""
    ### Testbeskrivelse og testmatrise

    Testene kjøres reaktivt og dekker inputkontrakt, aggregering, sortering,
    rendering og månedsprofil. Alle tre aggregatorer krever ikke-negative heltall
    med eksakt totalsum innen Int64, heltalls-ID, utfylt vitenskapelig navn og
    kalenderdato (`Date`, aldri tidsstempel). Udaterte rader beholdes i antall,
    individtall og gjennomsnitt, men ikke i tidsaggregatene. Navnepar bevares
    uten normalisering; dette er ikke et rent mål på artsrikdom.

    | ID | Scenario | Forventet resultat |
    |---|---|---|
    | ARTSTABELL-MTM-001 | To observasjoner av samme art | Korrekte summer, gjennomsnitt, tidsrom, måneder og aktiviteter |
    | ARTSTABELL-MTM-002 | To taksa med samme norske navn | Taksa holdes atskilt med `Artens ID` og `Art` |
    | ARTSTABELL-MTM-003 | Blandet kategori og observasjonsmengde | Full kategoriorden og flest observasjoner først innen kategori |
    | ARTSTABELL-MTM-004 | Motstridende artsmetadata | Tydelig `ValueError` i stedet for vilkårlig `.first()` |
    | ARTSTABELL-MTM-005 | Tom input med riktig schema | Tom output med fast kolonnerekkefølge og riktige typer |
    | ARTSTABELL-MTM-006 | Manglende obligatorisk kolonne | Tidlig feil som nevner kolonnen |
    | ARTSTABELL-MTM-007 | Feil datatype eller ukjent kategori | Tidlig og forklarende feil |
    | ARTSTABELL-MTM-008 | Great Tables-rendering | Verdi M1941 vises først uten tom ekstrakolonne, Source Sans 3 og offisielle Artsdatabanken/M-1941-fargeprofiler brukes, og tabell, nanoplot, tittel og fotnoter renderes |

    ### Testgrunnlag
    """),
            *[_vis_kildekode(_funksjon) for _funksjon in _testhjelpere],
            mo.md("### Tester"),
            *[_vis_kildekode(_funksjon) for _funksjon in _tester],
        ],
        gap=1,
    )
    mo.accordion({"Artsstatistikk – funksjoner og tester": _innhold})
    return


@app.cell(hide_code=True)
def _(arter_df, lag_artsstatistikk):
    artsstatistikk_df = lag_artsstatistikk(arter_df)
    return (artsstatistikk_df,)




@app.function(hide_code=True)
def hent_påkrevde_artsstatistikk_kolonner() -> set[str]:
    """Returner kolonnene som kreves for å lage artsstatistikken."""
    return set(ARTSSTATISTIKK_INPUTKOLONNER)


@app.function(hide_code=True)
def valider_observasjonsgrunnlag(df: pl.DataFrame, kolonner: set[str]) -> None:
    """Valider felles beregningsgrunnlag før konvertering eller summering."""
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Analyse krever en Polars DataFrame")
    manglende = sorted(kolonner - set(df.columns))
    if manglende:
        raise ValueError("Mangler obligatoriske kolonner: " + ", ".join(manglende))
    if not df.schema["Artens ID"].is_integer():
        raise TypeError("Kolonnen `Artens ID` må ha heltallstype")
    if df.get_column("Artens ID").null_count():
        raise ValueError("Kolonnen `Artens ID` kan ikke inneholde nullverdier")
    if df.schema["Art"] != pl.String:
        raise TypeError("Kolonnen `Art` må ha teksttype")
    if df.filter(pl.col("Art").is_null() | (pl.col("Art").str.strip_chars() == "")).height:
        raise ValueError("Kolonnen `Art` må inneholde et vitenskapelig navn, ikke null eller blank tekst")
    if df.schema["Observert dato"] != pl.Date:
        raise TypeError("Kolonnen `Observert dato` må ha typen Date; tidsstempler støttes ikke")
    if not df.schema["Antall"].is_integer():
        raise TypeError("Kolonnen `Antall` må ha heltallstype")
    antall = df.get_column("Antall")
    if antall.null_count():
        raise ValueError("Kolonnen `Antall` kan ikke inneholde nullverdier")
    if (antall < 0).any():
        raise ValueError("Kolonnen `Antall` kan ikke inneholde negative verdier")
    # Python-heltall beskytter også totalsummen på tvers av grupper og måneder.
    if sum(antall) > 2**63 - 1:
        raise ValueError("Summen av `Antall` overskrider støttet Int64-område (0–2**63−1)")


@app.function(hide_code=True)
def valider_artsstatistikk_input(df: pl.DataFrame) -> None:
    """Valider inputkontrakten for artsstatistikk.

    Args:
        df: Ferdig behandlet observasjonsdata.

    Raises:
        TypeError: Når input eller en obligatorisk kolonne har feil datatype.
        ValueError: Når kolonner/nøkler mangler, individtall er null/negative
            eller samlet overstiger Int64, kategorier er ukjente eller samme
            kildepar har motstridende metadata. Dato må være Date, men kan være null.
    """
    valider_observasjonsgrunnlag(df, hent_påkrevde_artsstatistikk_kolonner())

    feil_teksttyper = sorted(kolonne for kolonne in ARTSSTATISTIKK_TEKSTKOLONNER if df.schema[kolonne] != pl.String)
    if feil_teksttyper:
        raise TypeError("Følgende artsstatistikk-kolonner må ha teksttype: " + ", ".join(feil_teksttyper))

    tillatte_kategorier = set(ARTSSTATISTIKK_KATEGORI_REKKEFOELGE)
    ukjente_kategorier = (
        df.filter(pl.col("Kategori").is_null() | ~pl.col("Kategori").is_in(tillatte_kategorier))
        .get_column("Kategori")
        .unique()
        .to_list()
    )
    if ukjente_kategorier:
        kategoritekst = ", ".join(sorted("<null>" if verdi is None else str(verdi) for verdi in ukjente_kategorier))
        raise ValueError(f"Ukjente Kategori-verdier: {kategoritekst}")

    metadata_konflikter = (
        df.group_by(["Artens ID", "Art"])
        .agg(
            [pl.col(kolonne).drop_nulls().n_unique().alias(kolonne) for kolonne in ARTSSTATISTIKK_METADATAKOLONNER]
        )
        .filter(pl.any_horizontal([pl.col(kolonne) > 1 for kolonne in ARTSSTATISTIKK_METADATAKOLONNER]))
    )
    if metadata_konflikter.height > 0:
        konfliktkolonner = [
            kolonne
            for kolonne in ARTSSTATISTIKK_METADATAKOLONNER
            if metadata_konflikter.filter(pl.col(kolonne) > 1).height > 0
        ]
        raise ValueError("Motstridende artsmetadata for samme Artens ID/Art: " + ", ".join(konfliktkolonner))


@app.function(hide_code=True)
def lag_artsstatistikk(df: pl.DataFrame) -> pl.DataFrame:
    """Aggreger observasjoner til én rad per eksakt kildepar (Artens ID, Art).

    Månedsprofilen inneholder tolv heltall i rekkefølgen januar–desember.
    Udaterte rader inngår i N, sum og snitt, ikke i tidsprofilen. Nøkkelparet
    er ikke norsk navn, foreldreart eller en ny biologisk klassifisering.
    """
    valider_artsstatistikk_input(df)

    maanedsaggregeringer = [
        (pl.col("Observert dato").dt.month() == maaned).cast(pl.Int64).sum().alias(f"__maaned_{maaned}")
        for maaned in range(1, 13)
    ]

    return (
        df.group_by(["Artens ID", "Art"], maintain_order=True)
        .agg(
            [
                pl.col("Navn").drop_nulls().first().alias("Navn"),
                pl.col("Kategori").drop_nulls().first().alias("Kategori"),
                pl.col("Verdi M1941").drop_nulls().first().alias("Verdi M1941"),
                pl.col("Art av nasjonal forvaltningsinteresse (eks. rødlista)")
                .drop_nulls()
                .first()
                .alias("Forvaltningsinteresse"),
                pl.len().cast(pl.Int64).alias("Observasjoner"),
                pl.col("Antall").cast(pl.Int64).sum().alias("Individer"),
                (pl.col("Antall").cast(pl.Int64).sum() / pl.len()).alias("Gj.snitt individer"),
                pl.col("Observert dato").dt.year().min().alias("__aar_fra"),
                pl.col("Observert dato").dt.year().max().alias("__aar_til"),
                pl.col("Observert dato").dt.month().drop_nulls().unique().sort().alias("__maaneder"),
                pl.col("Familie").drop_nulls().first().alias("Familie"),
                pl.col("Orden").drop_nulls().first().alias("Orden"),
                (pl.col("Atferd") == "reproductive").cast(pl.Int64).sum().alias("Reproduksjon"),
                (pl.col("Atferd") == "possiblereproductive").cast(pl.Int64).sum().alias("Mulig reproduksjon"),
                *maanedsaggregeringer,
            ]
        )
        .with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("Navn").str.strip_chars().replace("", None),
                        pl.col("Art"),
                        pl.lit("Ukjent art"),
                    ]
                ).alias("Navn"),
                pl.when(pl.col("__aar_fra").is_null())
                .then(pl.lit(None, dtype=pl.String))
                .when(pl.col("__aar_fra") == pl.col("__aar_til"))
                .then(pl.col("__aar_fra").cast(pl.String))
                .otherwise(pl.concat_str([pl.col("__aar_fra"), pl.lit("–"), pl.col("__aar_til")]))
                .alias("År-periode"),
                pl.col("__maaneder")
                .list.eval(
                    pl.element().replace_strict(
                        ARTSSTATISTIKK_MAANEDSNAVN,
                        return_dtype=pl.String,
                    )
                )
                .list.join(", ")
                .alias("Måneder"),
                pl.concat_list([pl.col(f"__maaned_{maaned}") for maaned in range(1, 13)]).alias("Månedsprofil"),
            ]
        )
        .with_columns(
            [
                pl.col("Verdi M1941")
                .replace_strict(
                    ARTSSTATISTIKK_M1941_REKKEFOELGE,
                    default=999,
                )
                .alias("__verdi_sortering"),
                pl.col("Kategori")
                .replace_strict(
                    ARTSSTATISTIKK_KATEGORI_REKKEFOELGE,
                    default=999,
                )
                .alias("__kategori_sortering"),
            ]
        )
        .sort(
            ["__verdi_sortering", "__kategori_sortering", "Observasjoner"],
            descending=[False, False, True],
            maintain_order=True,
        )
        .select(ARTSSTATISTIKK_OUTPUTKOLONNER)
    )


@app.function(hide_code=True)
def lag_artsstatistikk_tabell(artsstatistikk_df: pl.DataFrame) -> gt.GT:
    """Bygg sikker HTML fra rå aggregert statistikk; kalleren skal ikke escape.

    Heltall formateres uten flyttall. Dateringsandel vises som stripe/prosent,
    eller bare «Ingen daterte observasjoner» når hele kildeparet er udatert.
    """
    manglende_kolonner = sorted(set(ARTSSTATISTIKK_OUTPUTKOLONNER) - set(artsstatistikk_df.columns))
    if manglende_kolonner:
        raise ValueError("Mangler kolonner for Great Tables-rendering: " + ", ".join(manglende_kolonner))

    antall_arter = artsstatistikk_df.height
    antall_observasjoner = sum(artsstatistikk_df["Observasjoner"])
    antall_individer = sum(artsstatistikk_df["Individer"])

    def velg_tekstfarge(bakgrunn: str) -> str:
        """Velg svart eller hvit tekst med best kontrast mot bakgrunnen."""
        heks = bakgrunn.removeprefix("#")
        rgb = [int(heks[indeks : indeks + 2], 16) / 255 for indeks in (0, 2, 4)]
        lineær_rgb = [kanal / 12.92 if kanal <= 0.04045 else ((kanal + 0.055) / 1.055) ** 2.4 for kanal in rgb]
        luminans = 0.2126 * lineær_rgb[0] + 0.7152 * lineær_rgb[1] + 0.0722 * lineær_rgb[2]
        return "#FFFFFF" if luminans < 0.179 else "#172033"

    def lag_fargemerke(verdi: str | None, fargekart: dict[str, str], *, kompakt: bool = False) -> str:
        """Velg farge på rå verdi; escape etiketten først ved HTML-grensen."""
        uklassifisert = not kompakt and verdi not in fargekart
        bakgrunn = "#FFFFFF" if uklassifisert else fargekart.get(verdi, "#D9D9D9")
        tekstfarge = velg_tekstfarge(bakgrunn)
        kantfarge = "#768083" if bakgrunn.upper() == "#FFFFFF" else bakgrunn
        kantstil = "dashed" if uklassifisert else "solid"
        minstebredde = "min-width:2.75em;" if kompakt else ""
        status = '' if kompakt else f' data-m1941="{"uklassifisert" if uklassifisert else "vurdert"}"'
        etikett = "Uklassifisert" if verdi is None else verdi
        return (
            f'<span{status} style="display:inline-flex;align-items:center;justify-content:center;'
            f"{minstebredde}box-sizing:border-box;padding:0.24em 0.58em;"
            f"background-color:{bakgrunn};color:{tekstfarge};"
            f"border:1px {kantstil} {kantfarge};border-radius:999px;font-weight:600;"
            f'line-height:1.2;white-space:nowrap;">{escape(str(etikett))}</span>'
        )

    tekstkolonner = [navn for navn, dtype in artsstatistikk_df.schema.items()
                    if dtype == pl.String and navn not in {"Kategori", "Verdi M1941"}]
    visning = artsstatistikk_df.with_columns(
        pl.col(tekstkolonner).map_elements(escape, return_dtype=pl.String),
        pl.col("Verdi M1941").map_elements(
            lambda verdi: lag_fargemerke(verdi, ARTSSTATISTIKK_M1941_FARGER),
            return_dtype=pl.String, skip_nulls=False,
        ),
        pl.col("Kategori").map_elements(
            lambda verdi: lag_fargemerke(verdi, ARTSSTATISTIKK_KATEGORIFARGER, kompakt=True),
            return_dtype=pl.String, skip_nulls=False,
        ),
    )
    tabell = (
        gt.GT(visning, id="artsstatistikk", locale="nb")
        .opt_table_font(font=["Source Sans 3", "sans-serif"])
        .tab_header(
            title="Artsstatistikk for valgte observasjoner",
            subtitle=(
                f"{ARTSANTALL_ETIKETT}: {antall_arter} · {antall_observasjoner} observasjoner · "
                f"{antall_individer} summerte, behandlede individtall"
            ),
        )
        .cols_merge(
            columns=["Navn", "Art"],
            hide_columns=["Art"],
            pattern="<strong>{0}</strong><br><em>{1}</em>",
        )
        .cols_hide(columns="Artens ID")
        .cols_label(
            cases={
                "Navn": "Art",
                "Forvaltningsinteresse": gt.html("Art av nasjonal<br>forvaltningsinteresse"),
                "Gj.snitt individer": "Gj.snitt",
                "År-periode": "År",
            }
        )
        .tab_style(
            style=gt.style.text(align="center"),
            locations=gt.loc.column_labels(),
        )
        .cols_align(
            align="center",
            columns=[
                "Verdi M1941",
                "Kategori",
                "Observasjoner",
                "Individer",
                "Gj.snitt individer",
                "Reproduksjon",
                "Mulig reproduksjon",
            ],
        )
        .fmt(
            fns=lambda verdi: format(verdi, ",").replace(",", "\u00a0"),
            columns=[
                "Observasjoner",
                "Individer",
                "Reproduksjon",
                "Mulig reproduksjon",
            ],
        )
        .fmt_number(
            columns="Gj.snitt individer",
            decimals=1,
            locale="nb",
        )
        .fmt_nanoplot(
            columns="Månedsprofil",
            rows=[indeks for indeks, profil in enumerate(artsstatistikk_df["Månedsprofil"]) if sum(profil)],
            plot_type="bar",
            plot_height="2.2em",
            autoscale=False,
            options=gt.nanoplot_options(
                data_bar_fill_color="#3B82F6",
                data_bar_stroke_color="#1D4ED8",
                data_bar_stroke_width=1,
                interactive_data_values=True,
            ),
        )
        .sub_missing(missing_text="–")
        .tab_spanner(
            label="Art og forvaltning",
            columns=["Verdi M1941", "Kategori", "Forvaltningsinteresse", "Navn"],
        )
        .tab_spanner(
            label="Omfang",
            columns=["Observasjoner", "Individer", "Gj.snitt individer"],
        )
        .tab_spanner(
            label="Tidsrom",
            columns=["År-periode", "Måneder", "Månedsprofil"],
        )
        .tab_spanner(label="Taksonomi", columns=["Familie", "Orden"])
        .tab_spanner(
            label="Aktivitet",
            columns=["Reproduksjon", "Mulig reproduksjon"],
        )
        .tab_footnote(
            footnote=(
                "Tolv søyler viser antall observasjoner fra januar til desember. "
                "Skalaen tilpasses hver rad for å fremheve sesongmønsteret. "
                "Stripen og prosenten viser andelen observasjoner med dato. "
                "Udaterte observasjoner inngår fortsatt i antall, individtall og gjennomsnitt."
            ),
            locations=gt.loc.column_labels(columns="Månedsprofil"),
        )
        .tab_footnote(
            footnote="Antall observasjoner registrert med atferden «reproductive».",
            locations=gt.loc.column_labels(columns="Reproduksjon"),
        )
        .tab_footnote(
            footnote="Antall observasjoner registrert med atferden «possiblereproductive».",
            locations=gt.loc.column_labels(columns="Mulig reproduksjon"),
        )
        .tab_footnote(
            footnote=(
                "Oppsummering av nasjonale forvaltningskriterier; «Nei» betyr "
                "ingen treff utover eventuell rødlistestatus."
            ),
            locations=gt.loc.column_labels(columns="Forvaltningsinteresse"),
        )
        .tab_source_note(source_note="Datagrunnlag: valgte rader i observasjonstabellen.")
        .tab_source_note(source_note=ARTSANTALL_MERKNAD)
        .tab_source_note(source_note="Summerte, behandlede individtall er ikke nødvendigvis forskjellige individer.")
        .opt_row_striping()
        .cols_width(
            cases={
                "Kategori": "70px",
                "Verdi M1941": "120px",
                "Forvaltningsinteresse": "190px",
                "Navn": "180px",
                "Observasjoner": "85px",
                "Individer": "80px",
                "Gj.snitt individer": "75px",
                "År-periode": "90px",
                "Måneder": "150px",
                "Månedsprofil": "170px",
                "Familie": "130px",
                "Orden": "150px",
                "Reproduksjon": "95px",
                "Mulig reproduksjon": "115px",
            }
        )
        .tab_options(
            container_width="1800px",
            container_height="1200px",
            container_overflow_x="auto",
            container_overflow_y="auto",
            table_width="1800px",
            table_layout="fixed",
            table_font_size="12px",
            heading_title_font_size="18px",
            heading_subtitle_font_size="12px",
            column_labels_font_size="12px",
            data_row_padding="5px",
            row_striping_background_color="#F7F9FC",
            grand_summary_row_background_color="#EAF2F8",
            footnotes_marks="letters",
        )
    )

    for indeks, rad in enumerate(artsstatistikk_df.iter_rows(named=True)):
        daterte = sum(rad["Månedsprofil"])
        if not daterte:
            tabell = tabell.text_transform(
                locations=gt.loc.body(columns="Månedsprofil", rows=[indeks]),
                fn=lambda _: "Ingen daterte observasjoner",
            )
            continue
        totalt = rad["Observasjoner"]
        tiendeler = daterte * 1000 // totalt
        if daterte == totalt:
            prosent = "100 %"
        elif daterte * 1000 < totalt:
            prosent = "<0,1 %"
        elif daterte * 1000 > totalt * 999:
            prosent = ">99,9 %"
        else:
            prosent = (str(tiendeler // 10) if tiendeler % 10 == 0
                       else f"{tiendeler // 10},{tiendeler % 10}") + " %"
        bredde = 100 if daterte == totalt else max(0.1, min(99.9, tiendeler / 10))
        dekning = (
            f'<div class="datodekning" aria-label="Datodekning: {escape(prosent)}" '
            'style="display:flex;align-items:center;gap:6px;margin-top:4px;">'
            '<span aria-hidden="true" style="display:block;flex:1;height:6px;background:#E2E8F0;">'
            f'<span style="display:block;height:100%;width:{bredde}%;background:#475569;"></span>'
            f'</span><span style="white-space:nowrap;">{escape(prosent)}</span></div>'
        )
        tabell = tabell.text_transform(
            locations=gt.loc.body(columns="Månedsprofil", rows=[indeks]),
            fn=lambda figur, stripe=dekning: figur + stripe,
        )
    return tabell


@app.cell(hide_code=True)
def _(ARTSSTATISTIKK_OUTPUTKOLONNER, ARTSSTATISTIKK_TEKSTKOLONNER):
    def lag_artsstatistikk_testinput(
        rad_overrides: list[dict[str, object]] | None = None,
    ) -> pl.DataFrame:
        """Lag en liten, komplett fixture for artsstatistikk-testene."""
        if rad_overrides is None:
            rad_overrides = [{}]

        grunnrad = {
            "Artens ID": 1001,
            "Art": "Species standardus",
            "Navn": "standardart",
            "Kategori": "LC",
            "Verdi M1941": "Noe verdi",
            "Art av nasjonal forvaltningsinteresse (eks. rødlista)": "Nei",
            "Antall": 1,
            "Atferd": None,
            "Observert dato": date(2020, 1, 1),
            "Familie": "standardfamilien",
            "Orden": "standardorden",
        }
        return pl.DataFrame(
            [{**grunnrad, **overrides} for overrides in rad_overrides],
            schema_overrides={kolonne: pl.String for kolonne in ARTSSTATISTIKK_TEKSTKOLONNER},
        )

    def lag_tom_artsstatistikk_testinput() -> pl.DataFrame:
        """Lag tom artsstatistikk-input med riktig schema."""
        tekstkolonner = ARTSSTATISTIKK_TEKSTKOLONNER
        kolonner = {kolonne: pl.Series(kolonne, [], dtype=pl.String) for kolonne in tekstkolonner}
        kolonner["Artens ID"] = pl.Series("Artens ID", [], dtype=pl.Int64)
        kolonner["Antall"] = pl.Series("Antall", [], dtype=pl.Int64)
        kolonner["Observert dato"] = pl.Series("Observert dato", [], dtype=pl.Date)
        return pl.DataFrame(kolonner)

    def artsstatistikk_forventede_kolonner() -> list[str]:
        """Returner godkjent kolonnerekkefølge for artsstatistikken."""
        return list(ARTSSTATISTIKK_OUTPUTKOLONNER)

    return (
        artsstatistikk_forventede_kolonner,
        lag_artsstatistikk_testinput,
        lag_tom_artsstatistikk_testinput,
    )


@app.cell(hide_code=True)
def _(lag_artsstatistikk, lag_artsstatistikk_testinput):
    def test_artsstatistikk_mtm_001():
        test_df = lag_artsstatistikk_testinput(
            [
                {
                    "Artens ID": 1,
                    "Art": "Avis exemplaris",
                    "Navn": "eksempelfugl",
                    "Antall": 2,
                    "Atferd": "reproductive",
                    "Observert dato": date(2021, 1, 2),
                },
                {
                    "Artens ID": 1,
                    "Art": "Avis exemplaris",
                    "Navn": "eksempelfugl",
                    "Antall": 4,
                    "Atferd": "possiblereproductive",
                    "Observert dato": date(2023, 3, 4),
                },
            ]
        )
        result = lag_artsstatistikk(test_df)

        assert result.height == 1
        assert result["Observasjoner"].to_list() == [2]
        assert result["Individer"].to_list() == [6]
        assert result["Gj.snitt individer"].to_list() == [3.0]
        assert result["År-periode"].to_list() == ["2021–2023"]
        assert result["Måneder"].to_list() == ["Jan, Mar"]
        assert result["Reproduksjon"].to_list() == [1]
        assert result["Mulig reproduksjon"].to_list() == [1]
        assert result["Månedsprofil"].to_list() == [[1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]]

    test_artsstatistikk_mtm_001()
    return (test_artsstatistikk_mtm_001,)


@app.cell(hide_code=True)
def _(lag_artsstatistikk, lag_artsstatistikk_testinput):
    def test_artsstatistikk_mtm_002():
        test_df = lag_artsstatistikk_testinput(
            [
                {"Artens ID": 1, "Art": "Species alpha", "Navn": "samme navn"},
                {"Artens ID": 2, "Art": "Species beta", "Navn": "samme navn"},
            ]
        )
        result = lag_artsstatistikk(test_df)

        assert result.height == 2, "Taksa med samme norske navn skal ikke slås sammen"
        assert result["Artens ID"].to_list() == [1, 2]
        assert result["Art"].to_list() == ["Species alpha", "Species beta"]

    test_artsstatistikk_mtm_002()
    return (test_artsstatistikk_mtm_002,)


@app.cell(hide_code=True)
def _(lag_artsstatistikk, lag_artsstatistikk_testinput):
    def test_artsstatistikk_mtm_003():
        test_df = lag_artsstatistikk_testinput(
            [
                {
                    "Artens ID": 1,
                    "Art": "middels-cr",
                    "Kategori": "CR",
                    "Verdi M1941": "Middels verdi",
                },
                {
                    "Artens ID": 2,
                    "Art": "svært-lc",
                    "Kategori": "LC",
                    "Verdi M1941": "Svært stor verdi",
                },
                {
                    "Artens ID": 3,
                    "Art": "stor-lc",
                    "Kategori": "LC",
                    "Verdi M1941": "Stor verdi",
                },
                {
                    "Artens ID": 4,
                    "Art": "stor-cr-liten",
                    "Kategori": "CR",
                    "Verdi M1941": "Stor verdi",
                },
                {
                    "Artens ID": 5,
                    "Art": "stor-cr-stor",
                    "Kategori": "CR",
                    "Verdi M1941": "Stor verdi",
                },
                {
                    "Artens ID": 5,
                    "Art": "stor-cr-stor",
                    "Kategori": "CR",
                    "Verdi M1941": "Stor verdi",
                },
            ]
        )
        result = lag_artsstatistikk(test_df)

        assert result.columns[:3] == ["Artens ID", "Verdi M1941", "Kategori"]
        assert result["Art"].to_list() == [
            "svært-lc",
            "stor-cr-stor",
            "stor-cr-liten",
            "stor-lc",
            "middels-cr",
        ]

    test_artsstatistikk_mtm_003()
    return (test_artsstatistikk_mtm_003,)


@app.cell(hide_code=True)
def _(lag_artsstatistikk, lag_artsstatistikk_testinput):
    def test_artsstatistikk_mtm_004():
        test_df = lag_artsstatistikk_testinput(
            [
                {"Artens ID": 1, "Art": "Species alpha", "Kategori": "LC"},
                {"Artens ID": 1, "Art": "Species alpha", "Kategori": "NT"},
            ]
        )
        try:
            lag_artsstatistikk(test_df)
        except ValueError as exc:
            assert "Kategori" in str(exc)
            assert "Motstridende artsmetadata" in str(exc)
        else:
            raise AssertionError("Motstridende kategori skulle gitt ValueError")

    test_artsstatistikk_mtm_004()
    return (test_artsstatistikk_mtm_004,)


@app.cell(hide_code=True)
def _(
    artsstatistikk_forventede_kolonner,
    lag_artsstatistikk,
    lag_tom_artsstatistikk_testinput,
):
    def test_artsstatistikk_mtm_005():
        result = lag_artsstatistikk(lag_tom_artsstatistikk_testinput())

        assert result.height == 0
        assert result.columns == artsstatistikk_forventede_kolonner()
        assert result.schema["Observasjoner"] == pl.Int64
        assert result.schema["Individer"] == pl.Int64
        assert result.schema["Gj.snitt individer"] == pl.Float64
        assert result.schema["Månedsprofil"] == pl.List(pl.Int64)

    test_artsstatistikk_mtm_005()
    return (test_artsstatistikk_mtm_005,)


@app.cell(hide_code=True)
def _(lag_artsstatistikk, lag_artsstatistikk_testinput):
    def test_artsstatistikk_mtm_006():
        test_df = lag_artsstatistikk_testinput().drop("Atferd")
        try:
            lag_artsstatistikk(test_df)
        except ValueError as exc:
            assert "Atferd" in str(exc)
        else:
            raise AssertionError("Manglende Atferd skulle gitt ValueError")

    test_artsstatistikk_mtm_006()
    return (test_artsstatistikk_mtm_006,)


@app.cell(hide_code=True)
def _(lag_artsstatistikk, lag_artsstatistikk_testinput):
    def test_artsstatistikk_mtm_007():
        feil_antall = lag_artsstatistikk_testinput([{"Antall": "to"}])
        try:
            lag_artsstatistikk(feil_antall)
        except TypeError as exc:
            assert "Antall" in str(exc)
        else:
            raise AssertionError("Antall med teksttype skulle gitt TypeError")

        ukjent_kategori = lag_artsstatistikk_testinput([{"Kategori": "XX"}])
        try:
            lag_artsstatistikk(ukjent_kategori)
        except ValueError as exc:
            assert "XX" in str(exc)
        else:
            raise AssertionError("Ukjent kategori skulle gitt ValueError")

    test_artsstatistikk_mtm_007()
    return (test_artsstatistikk_mtm_007,)


@app.cell(hide_code=True)
def _(
    ARTSSTATISTIKK_KATEGORIFARGER,
    ARTSSTATISTIKK_M1941_FARGER,
    lag_artsstatistikk,
    lag_artsstatistikk_tabell,
    lag_artsstatistikk_testinput,
):
    def test_artsstatistikk_mtm_008():
        forventede_kategorifarger = {
            "RE": "#262F31",
            "CR": "#D61900",
            "EN": "#F34F39",
            "VU": "#EB8107",
            "NT": "#E6C000",
            "LC": "#61A360",
            "DD": "#6C6C6C",
            "SE": "#4E1A53",
            "HI": "#17467C",
            "PH": "#286371",
            "LO": "#7CB1AC",
            "NK": "#D2CF84",
            "NA": "#FFFFFF",
            "NE": "#FFFFFF",
            "Unknown": "#D9D9D9",
        }
        forventede_m1941_farger = {
            "Svært stor verdi": "#AF0F0F",
            "Stor verdi": "#FD7032",
            "Middels verdi": "#FEC02D",
            "Noe verdi": "#FFFF00",
            "Uten betydning for KU": "#D9D9D9",
        }
        statistikk = lag_artsstatistikk(lag_artsstatistikk_testinput())
        tabell = lag_artsstatistikk_tabell(statistikk)
        html = tabell.as_raw_html()

        assert ARTSSTATISTIKK_KATEGORIFARGER == forventede_kategorifarger
        assert ARTSSTATISTIKK_M1941_FARGER == forventede_m1941_farger
        assert isinstance(tabell, gt.GT)
        assert "Artsstatistikk for valgte observasjoner" in html
        assert "Månedsprofil" in html
        assert "Art av nasjonal<br>forvaltningsinteresse" in html
        assert "Source Sans 3" in html
        assert "background-color:#61A360" in html
        assert "background-color:#FFFF00" in html
        assert html.index(">Verdi M1941<") < html.index(">Kategori<")
        assert 'gt_stub">&nbsp;</th>' not in html, "Tabellen skal ikke ha en tom ekstrakolonne"
        assert "border-radius:999px" in html, "Verdi og kategori skal renderes som fargemerker"
        assert "Noe verdi</span>" in html, "Hele verditeksten skal finnes i fargemerket"
        assert "<svg" in html, "Månedsprofilen skal renderes som nanoplot"
        assert "reproductive" in html

    test_artsstatistikk_mtm_008()
    return (test_artsstatistikk_mtm_008,)


@app.cell(hide_code=True)
def _(valgt_fil):
    mo.stop(
        not valgt_fil.value,
        mo.md("Velg en ferdig behandlet Parquet-fil for å starte analysen."),
    )

    file_info = valgt_fil.value[0]
    arter_df_lest_inn = pl.read_parquet(file_info.path)
    artsdata_df = mo.ui.table(arter_df_lest_inn, page_size=20)
    return (artsdata_df,)


@app.cell(hide_code=True)
def _(artsdata_df):
    arter_df = artsdata_df.value
    return (arter_df,)


@app.cell(hide_code=True)
def dekningsmatrise_funksjonsvisning(
    lag_dekningsmatrise,
    lag_dekningsmatrise_testinput,
    lag_dekningsmatrisefigur,
    test_dekningsmatrise_mtm_001,
    test_dekningsmatrise_mtm_002,
    valider_dekningsmatrise_input,
):
    def _vis_kildekode(_funksjon):
        _kildekode = textwrap.dedent(inspect.getsource(_funksjon)).strip()
        return mo.md(f"#### `{_funksjon.__name__}`\n\n```python\n{_kildekode}\n```")

    _funksjoner = [
        valider_dekningsmatrise_input,
        lag_dekningsmatrise,
        lag_dekningsmatrisefigur,
    ]
    _testhjelpere = [lag_dekningsmatrise_testinput]
    _tester = [
        test_dekningsmatrise_mtm_001,
        test_dekningsmatrise_mtm_002,
    ]
    _innhold = mo.vstack(
        [
            mo.md(r"""
    ### Funksjonsstruktur

    1. `valider_dekningsmatrise_input` validerer inputkontrakten.
    2. `lag_dekningsmatrise` lager et komplett år–måned-rutenett med absolutte datamål.
    3. `lag_dekningsmatrisefigur` bygger Altair-matrisen med månedsnavn på øvre akse.

    ### Funksjoner
    """),
            *[_vis_kildekode(_funksjon) for _funksjon in _funksjoner],
            mo.md(r"""
    ### Testbeskrivelse og testmatrise

    Testene kjøres reaktivt og kontrollerer både aggregeringen og figurens struktur.

    | ID | Scenario | Forventet resultat |
    |---|---|---|
    | DEKNINGSMATRISE-MTM-001 | Data fra flere år og måneder | Korrekte summer, unike verdier og tomme ruter |
    | DEKNINGSMATRISE-MTM-002 | Rendering av matrisen | Rektangelmerker og månedsakse øverst |

    ### Testgrunnlag
    """),
            *[_vis_kildekode(_funksjon) for _funksjon in _testhjelpere],
            mo.md("### Tester"),
            *[_vis_kildekode(_funksjon) for _funksjon in _tester],
        ],
        gap=1,
    )
    mo.accordion({"Datadekningsmatrise – funksjoner og tester": _innhold})
    return


@app.cell(hide_code=True)
def dekningsmatrise_konstanter_validering(valider_observasjonsgrunnlag):
    DEKNINGSMATRISE_MAANEDSNAVN = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "Mai",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Okt",
        11: "Nov",
        12: "Des",
    }

    DEKNINGSMATRISE_MAANEDSREKKEFOELGE = list(DEKNINGSMATRISE_MAANEDSNAVN.values())

    DEKNINGSMATRISE_INPUTKOLONNER = {
        "Observert dato",
        "Antall",
        "Artens ID",
        "Art",
        "Observatør",
        "Lokalitet",
    }

    DEKNINGSMATRISE_OUTPUTKOLONNER = [
        "År",
        "Måned",
        "Månedsnavn",
        "Registreringer",
        "Individer",
        "Aktive datoer",
        "Arter",
        "Observatører",
        "Lokaliteter",
    ]

    DEKNINGSMATRISE_OUTPUTTYPER = {
        "År": pl.Int32,
        "Måned": pl.Int8,
        "Månedsnavn": pl.String,
        "Registreringer": pl.Int64,
        "Individer": pl.Int64,
        "Aktive datoer": pl.Int64,
        "Arter": pl.Int64,
        "Observatører": pl.Int64,
        "Lokaliteter": pl.Int64,
    }

    DEKNINGSMATRISE_MAAL = [
        "Registreringer",
        "Individer",
        "Aktive datoer",
        "Arter",
        "Observatører",
        "Lokaliteter",
    ]

    def valider_dekningsmatrise_input(df: pl.DataFrame) -> None:
        """Valider ferdig behandlet observasjonsdata for dekningsmatrisen."""
        valider_observasjonsgrunnlag(df, DEKNINGSMATRISE_INPUTKOLONNER)

    return (
        DEKNINGSMATRISE_MAAL,
        DEKNINGSMATRISE_MAANEDSNAVN,
        DEKNINGSMATRISE_MAANEDSREKKEFOELGE,
        DEKNINGSMATRISE_OUTPUTKOLONNER,
        DEKNINGSMATRISE_OUTPUTTYPER,
        valider_dekningsmatrise_input,
    )


@app.cell(hide_code=True)
def dekningsmatrise_aggregering(
    DEKNINGSMATRISE_MAAL,
    DEKNINGSMATRISE_MAANEDSNAVN,
    DEKNINGSMATRISE_OUTPUTKOLONNER,
    DEKNINGSMATRISE_OUTPUTTYPER,
    valider_dekningsmatrise_input,
):
    def lag_dekningsmatrise(df: pl.DataFrame) -> pl.DataFrame:
        """Aggreger daterte rader til år–måned; helt udatert gir tom matrise."""
        valider_dekningsmatrise_input(df)
        datert = df.filter(pl.col("Observert dato").is_not_null())
        if datert.is_empty():
            return pl.DataFrame(schema=DEKNINGSMATRISE_OUTPUTTYPER)

        datert = datert.with_columns(
            pl.col("Observert dato").dt.year().alias("År"),
            pl.col("Observert dato").dt.month().cast(pl.Int8).alias("Måned"),
        )
        aggregert = datert.group_by("År", "Måned").agg(
            pl.len().cast(pl.Int64).alias("Registreringer"),
            pl.col("Antall").cast(pl.Int64).sum().alias("Individer"),
            pl.col("Observert dato").n_unique().cast(pl.Int64).alias("Aktive datoer"),
            pl.struct("Artens ID", "Art").n_unique().cast(pl.Int64).alias("Arter"),
            pl.col("Observatør").drop_nulls().n_unique().cast(pl.Int64).alias("Observatører"),
            pl.col("Lokalitet").drop_nulls().n_unique().cast(pl.Int64).alias("Lokaliteter"),
        )
        min_aar = int(datert.get_column("År").min())
        maks_aar = int(datert.get_column("År").max())
        aar = pl.DataFrame({"År": pl.Series(range(min_aar, maks_aar + 1), dtype=pl.Int32)})
        maaneder = pl.DataFrame({"Måned": pl.Series(range(1, 13), dtype=pl.Int8)})

        return (
            aar.join(maaneder, how="cross")
            .join(aggregert, on=["År", "Måned"], how="left")
            .with_columns(pl.col(DEKNINGSMATRISE_MAAL).fill_null(0))
            .with_columns(
                pl.col("Måned")
                .replace_strict(
                    DEKNINGSMATRISE_MAANEDSNAVN,
                    return_dtype=pl.String,
                )
                .alias("Månedsnavn")
            )
            .sort("År", "Måned")
            .select(DEKNINGSMATRISE_OUTPUTKOLONNER)
        )

    return (lag_dekningsmatrise,)


@app.cell(hide_code=True)
def dekningsmatrise_figurfunksjon(
    DEKNINGSMATRISE_MAAL,
    DEKNINGSMATRISE_MAANEDSREKKEFOELGE,
    DEKNINGSMATRISE_OUTPUTKOLONNER,
):
    def lag_dekningsmatrisefigur(
        dekningsmatrise_df: pl.DataFrame,
        maal: str,
    ):
        """Bygg en år–måned-varmematrise med månedsnavn på øvre akse."""
        if not isinstance(dekningsmatrise_df, pl.DataFrame):
            raise TypeError("Dekningsmatrisefiguren krever en Polars DataFrame")
        manglende = sorted(set(DEKNINGSMATRISE_OUTPUTKOLONNER) - set(dekningsmatrise_df.columns))
        if manglende:
            raise ValueError("Mangler kolonner for dekningsmatrisefiguren: " + ", ".join(manglende))
        if maal not in DEKNINGSMATRISE_MAAL:
            raise ValueError(f"Ukjent mål for dekningsmatrisen: {maal}")

        if dekningsmatrise_df.is_empty():
            return alt.Chart(pl.DataFrame({"Melding": ["Ingen daterte observasjoner"]})).mark_text(
                fontSize=14,
            ).encode(text="Melding:N").properties(width=900, height=80)
        etikett = ARTSANTALL_ETIKETT if maal == "Arter" else maal
        antall_aar = dekningsmatrise_df.get_column("År").n_unique()
        # Nettlesertall er flyttall; tooltip-tekst bevarer også store heltall eksakt.
        visning = dekningsmatrise_df.with_columns([
            pl.col(kolonne).map_elements(
                lambda verdi: format(verdi, ",").replace(",", "\u00a0"), return_dtype=pl.String,
            ).alias(f"__tekst_{kolonne}") for kolonne in DEKNINGSMATRISE_MAAL
        ])
        return (
            alt.Chart(visning)
            .mark_rect(stroke="white", strokeWidth=1)
            .encode(
                x=alt.X(
                    "Månedsnavn:N",
                    title=None,
                    sort=DEKNINGSMATRISE_MAANEDSREKKEFOELGE,
                    axis=alt.Axis(
                        orient="top",
                        title=None,
                        labelAngle=0,
                    ),
                ),
                y=alt.Y("År:O", title="År", sort="descending"),
                color=alt.condition(
                    f"datum['{maal}'] === 0",
                    alt.value("#F3F4F6"),
                    alt.Color(
                        field=maal,
                        type="quantitative",
                        title=etikett,
                        scale=alt.Scale(scheme="blues"),
                    ),
                ),
                tooltip=[
                    alt.Tooltip("År:O", title="År"),
                    alt.Tooltip("Månedsnavn:N", title="Måned"),
                    *[alt.Tooltip(f"__tekst_{kolonne}:N", title=(
                        ARTSANTALL_ETIKETT if kolonne == "Arter" else kolonne
                    )) for kolonne in DEKNINGSMATRISE_MAAL],
                ],
            )
            .properties(
                width=900,
                height=max(260, antall_aar * 18),
                title=alt.Title(f"År × måned: {etikett}", subtitle=ARTSANTALL_MERKNAD),
            )
            .configure_view(stroke=None)
        )

    return (lag_dekningsmatrisefigur,)


@app.cell(hide_code=True)
def dekningsmatrise_tester(lag_dekningsmatrise, lag_dekningsmatrisefigur):
    def lag_dekningsmatrise_testinput(
        rader: list[dict[str, object]] | None = None,
    ) -> pl.DataFrame:
        """Lag et lite, komplett testgrunnlag for dekningsmatrisen."""
        if rader is None:
            rader = []
        grunnrad = {
            "Observert dato": date(2020, 1, 1),
            "Antall": 1,
            "Artens ID": 1,
            "Art": "Species standardus",
            "Observatør": "Ola",
            "Lokalitet": "A",
        }
        return pl.DataFrame(
            [{**grunnrad, **rad} for rad in rader],
            schema={
                "Observert dato": pl.Date,
                "Antall": pl.Int64,
                "Artens ID": pl.Int64,
                "Art": pl.String,
                "Observatør": pl.String,
                "Lokalitet": pl.String,
            },
        )

    def test_dekningsmatrise_mtm_001():
        test_df = lag_dekningsmatrise_testinput(
            [
                {"Observert dato": date(2020, 1, 1), "Antall": 1},
                {
                    "Observert dato": date(2020, 1, 1),
                    "Antall": 3,
                    "Artens ID": 2,
                    "Observatør": "Kari",
                },
                {"Observert dato": date(2021, 2, 2), "Antall": 2},
            ]
        )
        result = lag_dekningsmatrise(test_df)
        januar = result.filter((pl.col("År") == 2020) & (pl.col("Måned") == 1)).row(0, named=True)

        assert result.height == 24
        assert januar["Registreringer"] == 2
        assert januar["Individer"] == 4
        assert januar["Aktive datoer"] == 1
        assert januar["Arter"] == 2
        assert januar["Observatører"] == 2
        assert result.filter((pl.col("År") == 2020) & (pl.col("Måned") == 2))["Registreringer"].item() == 0

    def test_dekningsmatrise_mtm_002():
        test_df = lag_dekningsmatrise_testinput([{"Observert dato": date(2020, 1, 1), "Antall": 1}])
        result = lag_dekningsmatrise(test_df)
        figur = lag_dekningsmatrisefigur(result, "Aktive datoer")
        spesifikasjon = figur.to_dict()

        assert result["Individer"].sum() == 1
        assert spesifikasjon["mark"]["type"] == "rect"
        assert spesifikasjon["encoding"]["x"]["axis"]["orient"] == "top"

    test_dekningsmatrise_mtm_001()
    test_dekningsmatrise_mtm_002()
    return (
        lag_dekningsmatrise_testinput,
        test_dekningsmatrise_mtm_001,
        test_dekningsmatrise_mtm_002,
    )


@app.cell(hide_code=True)
def maanedsgrunnlag_funksjonsvisning(
    lag_maanedsgrunnlag,
    lag_maanedsgrunnlag_testinput,
    lag_maanedsgrunnlagstabell,
    test_maanedsgrunnlag_mtm_001,
    test_maanedsgrunnlag_mtm_002,
    valider_maanedsgrunnlag_input,
):
    def _vis_kildekode(_funksjon):
        _kildekode = textwrap.dedent(inspect.getsource(_funksjon)).strip()
        return mo.md(f"#### `{_funksjon.__name__}`\n\n```python\n{_kildekode}\n```")

    _funksjoner = [
        valider_maanedsgrunnlag_input,
        lag_maanedsgrunnlag,
        lag_maanedsgrunnlagstabell,
    ]
    _testhjelpere = [lag_maanedsgrunnlag_testinput]
    _tester = [
        test_maanedsgrunnlag_mtm_001,
        test_maanedsgrunnlag_mtm_002,
    ]
    _innhold = mo.vstack(
        [
            mo.md(r"""
    ### Funksjonsstruktur

    1. `valider_maanedsgrunnlag_input` validerer inputkontrakten.
    2. `lag_maanedsgrunnlag` aggregerer datagrunnlaget til tolv kalendermåneder.
    3. `lag_maanedsgrunnlagstabell` bygger en formatert Great Table.

    ### Funksjoner
    """),
            *[_vis_kildekode(_funksjon) for _funksjon in _funksjoner],
            mo.md(r"""
    ### Testbeskrivelse og testmatrise

    Testene kjøres reaktivt og kontrollerer månedsaggregering, individtall og
    Great Tables-rendering.

    | ID | Scenario | Forventet resultat |
    |---|---|---|
    | MAANEDSGRUNNLAG-MTM-001 | Data fra flere år og måneder | Tolv rader med korrekte summer, aktive datoer og år med data |
    | MAANEDSGRUNNLAG-MTM-002 | Great Tables-rendering | Korrekte individtall, tittel og norske kolonnenavn |

    ### Testgrunnlag
    """),
            *[_vis_kildekode(_funksjon) for _funksjon in _testhjelpere],
            mo.md("### Tester"),
            *[_vis_kildekode(_funksjon) for _funksjon in _tester],
        ],
        gap=1,
    )
    mo.accordion({"Månedsgrunnlag – funksjoner og tester": _innhold})
    return


@app.cell(hide_code=True)
def maanedsgrunnlag_konstanter_validering(valider_observasjonsgrunnlag):
    DATAGRUNNLAG_MAANEDSNAVN = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "Mai",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Okt",
        11: "Nov",
        12: "Des",
    }

    MAANEDSGRUNNLAG_INPUTKOLONNER = {
        "Observert dato",
        "Antall",
        "Artens ID",
        "Art",
        "Observatør",
        "Lokalitet",
    }

    MAANEDSGRUNNLAG_OUTPUTKOLONNER = [
        "Måned",
        "Månedsnavn",
        "Registreringer",
        "Individer",
        "Aktive datoer",
        "År med data",
        "Arter",
        "Observatører",
        "Lokaliteter",
    ]

    MAANEDSGRUNNLAG_OUTPUTTYPER = {
        "Måned": pl.Int8,
        "Månedsnavn": pl.String,
        "Registreringer": pl.Int64,
        "Individer": pl.Int64,
        "Aktive datoer": pl.Int64,
        "År med data": pl.Int64,
        "Arter": pl.Int64,
        "Observatører": pl.Int64,
        "Lokaliteter": pl.Int64,
    }

    def valider_maanedsgrunnlag_input(df: pl.DataFrame) -> None:
        """Valider ferdig behandlet observasjonsdata for månedsgrunnlaget."""
        valider_observasjonsgrunnlag(df, MAANEDSGRUNNLAG_INPUTKOLONNER)

    return (
        DATAGRUNNLAG_MAANEDSNAVN,
        MAANEDSGRUNNLAG_OUTPUTKOLONNER,
        MAANEDSGRUNNLAG_OUTPUTTYPER,
        valider_maanedsgrunnlag_input,
    )


@app.cell(hide_code=True)
def maanedsgrunnlag_aggregering(
    DATAGRUNNLAG_MAANEDSNAVN,
    MAANEDSGRUNNLAG_OUTPUTKOLONNER,
    MAANEDSGRUNNLAG_OUTPUTTYPER,
    valider_maanedsgrunnlag_input,
):
    def lag_maanedsgrunnlag(df: pl.DataFrame) -> pl.DataFrame:
        """Aggreger daterte rader til tolv måneder; tell eksakte ID/navnepar."""
        valider_maanedsgrunnlag_input(df)
        df = df.filter(pl.col("Observert dato").is_not_null())
        maaneder = pl.DataFrame({"Måned": pl.Series(range(1, 13), dtype=pl.Int8)})
        if df.is_empty():
            aggregert = pl.DataFrame(
                schema={
                    kolonne: dtype for kolonne, dtype in MAANEDSGRUNNLAG_OUTPUTTYPER.items() if kolonne != "Månedsnavn"
                }
            )
        else:
            aggregert = (
                df.with_columns(pl.col("Observert dato").dt.month().cast(pl.Int8).alias("Måned"))
                .group_by("Måned")
                .agg(
                    pl.len().cast(pl.Int64).alias("Registreringer"),
                    pl.col("Antall").cast(pl.Int64).sum().alias("Individer"),
                    pl.col("Observert dato").n_unique().cast(pl.Int64).alias("Aktive datoer"),
                    pl.col("Observert dato").dt.year().n_unique().cast(pl.Int64).alias("År med data"),
                    pl.struct("Artens ID", "Art").n_unique().cast(pl.Int64).alias("Arter"),
                    pl.col("Observatør").drop_nulls().n_unique().cast(pl.Int64).alias("Observatører"),
                    pl.col("Lokalitet").drop_nulls().n_unique().cast(pl.Int64).alias("Lokaliteter"),
                )
            )
        tallkolonner = [kolonne for kolonne in MAANEDSGRUNNLAG_OUTPUTKOLONNER if kolonne not in {"Måned", "Månedsnavn"}]
        return (
            maaneder.join(aggregert, on="Måned", how="left")
            .with_columns(pl.col(tallkolonner).fill_null(0))
            .with_columns(
                pl.col("Måned")
                .replace_strict(
                    DATAGRUNNLAG_MAANEDSNAVN,
                    return_dtype=pl.String,
                )
                .alias("Månedsnavn")
            )
            .sort("Måned")
            .select(MAANEDSGRUNNLAG_OUTPUTKOLONNER)
        )

    return (lag_maanedsgrunnlag,)


@app.cell(hide_code=True)
def maanedsgrunnlag_tabellfunksjon(MAANEDSGRUNNLAG_OUTPUTKOLONNER):
    def lag_maanedsgrunnlagstabell(
        maanedsgrunnlag_df: pl.DataFrame,
    ) -> gt.GT:
        """Bygg en Great Table med absolutt datagrunnlag per måned."""
        if not isinstance(maanedsgrunnlag_df, pl.DataFrame):
            raise TypeError("Månedsgrunnlagstabellen krever en Polars DataFrame")
        manglende = sorted(set(MAANEDSGRUNNLAG_OUTPUTKOLONNER) - set(maanedsgrunnlag_df.columns))
        if manglende:
            raise ValueError("Mangler kolonner for månedsgrunnlagstabellen: " + ", ".join(manglende))

        antall_registreringer = sum(maanedsgrunnlag_df["Registreringer"])
        if not antall_registreringer:
            return gt.GT(maanedsgrunnlag_df.head(0)).tab_header(
                title="Ingen daterte observasjoner",
            ).tab_options(column_labels_hidden=True)
        antall_individer = sum(maanedsgrunnlag_df["Individer"])
        antall_aktive_datoer = sum(maanedsgrunnlag_df["Aktive datoer"])
        tallkolonner = [
            "Registreringer",
            "Individer",
            "Aktive datoer",
            "År med data",
            "Arter",
            "Observatører",
            "Lokaliteter",
        ]

        return (
            gt.GT(
                maanedsgrunnlag_df.drop("Måned"),
                id="maanedsgrunnlag",
                locale="nb",
            )
            .tab_header(
                title="Datagrunnlag gjennom året",
                subtitle=(
                    f"{antall_registreringer} registreringer · "
                    f"{antall_individer} summerte, behandlede individtall · "
                    f"{antall_aktive_datoer} aktive datoer"
                ),
            )
            .cols_label(cases={"Månedsnavn": "Måned", "Arter": ARTSANTALL_ETIKETT})
            .fmt(columns=tallkolonner, fns=lambda verdi: format(verdi, ",").replace(",", "\u00a0"))
            .data_color(
                columns=tallkolonner,
                palette=["#EFF6FF", "#1D4ED8"],
                autocolor_text=True,
            )
            .tab_spanner(
                label="Omfang",
                columns=["Registreringer", "Individer"],
            )
            .tab_spanner(
                label="Tidsdekning",
                columns=["Aktive datoer", "År med data"],
            )
            .tab_spanner(
                label="Bredde",
                columns=["Arter", "Observatører", "Lokaliteter"],
            )
            .tab_footnote(
                footnote=("Antall observasjonsrader; radene er ikke nødvendigvis uavhengige observasjoner."),
                locations=gt.loc.column_labels(columns="Registreringer"),
            )
            .tab_footnote(
                footnote="Unike datoer med minst én registreringsrad i måneden.",
                locations=gt.loc.column_labels(columns="Aktive datoer"),
            )
            .tab_footnote(
                footnote="Kalenderår med minst én registreringsrad i måneden.",
                locations=gt.loc.column_labels(columns="År med data"),
            )
            .tab_source_note(source_note="Datagrunnlag: aktivt utvalg i observasjonstabellen.")
            .tab_source_note(source_note=ARTSANTALL_MERKNAD)
            .tab_source_note(source_note="Summerte, behandlede individtall er ikke nødvendigvis forskjellige individer.")
            .opt_row_striping()
            .cols_width(
                cases={
                    "Månedsnavn": "90px",
                    "Registreringer": "115px",
                    "Individer": "100px",
                    "Aktive datoer": "110px",
                    "År med data": "100px",
                    "Arter": "85px",
                    "Observatører": "105px",
                    "Lokaliteter": "100px",
                }
            )
            .tab_options(
                table_width="900px",
                table_font_size="13px",
                data_row_padding="6px",
            )
        )

    return (lag_maanedsgrunnlagstabell,)


@app.cell(hide_code=True)
def maanedsgrunnlag_tester(lag_maanedsgrunnlag, lag_maanedsgrunnlagstabell):
    def lag_maanedsgrunnlag_testinput(
        rader: list[dict[str, object]] | None = None,
    ) -> pl.DataFrame:
        """Lag et lite, komplett testgrunnlag for månedstabellen."""
        if rader is None:
            rader = []
        grunnrad = {
            "Observert dato": date(2020, 1, 1),
            "Antall": 1,
            "Artens ID": 1,
            "Art": "Species standardus",
            "Observatør": "Ola",
            "Lokalitet": "A",
        }
        return pl.DataFrame(
            [{**grunnrad, **rad} for rad in rader],
            schema={
                "Observert dato": pl.Date,
                "Antall": pl.Int64,
                "Artens ID": pl.Int64,
                "Art": pl.String,
                "Observatør": pl.String,
                "Lokalitet": pl.String,
            },
        )

    def test_maanedsgrunnlag_mtm_001():
        test_df = lag_maanedsgrunnlag_testinput(
            [
                {"Observert dato": date(2020, 1, 1), "Antall": 1},
                {
                    "Observert dato": date(2021, 1, 2),
                    "Antall": 3,
                    "Artens ID": 2,
                },
                {"Observert dato": date(2021, 2, 1), "Antall": 2},
            ]
        )
        result = lag_maanedsgrunnlag(test_df)
        januar = result.filter(pl.col("Måned") == 1).row(0, named=True)

        assert result.height == 12
        assert januar["Registreringer"] == 2
        assert januar["Individer"] == 4
        assert januar["Aktive datoer"] == 2
        assert januar["År med data"] == 2
        assert januar["Arter"] == 2
        assert result.filter(pl.col("Måned") == 3)["Registreringer"].item() == 0

    def test_maanedsgrunnlag_mtm_002():
        test_df = lag_maanedsgrunnlag_testinput([{"Observert dato": date(2020, 1, 1), "Antall": 1}])
        result = lag_maanedsgrunnlag(test_df)
        tabell = lag_maanedsgrunnlagstabell(result)
        html = tabell.as_raw_html()

        assert result["Individer"].sum() == 1
        assert isinstance(tabell, gt.GT)
        assert "Datagrunnlag gjennom året" in html
        assert "Individer" in html
        assert "individualCount" not in html

    test_maanedsgrunnlag_mtm_001()
    test_maanedsgrunnlag_mtm_002()
    return (
        lag_maanedsgrunnlag_testinput,
        test_maanedsgrunnlag_mtm_001,
        test_maanedsgrunnlag_mtm_002,
    )


@app.cell(column=1, hide_code=True)
def _(artsdata_df):
    artsdata_df
    return


@app.cell(hide_code=True)
def _(artsstatistikk_df, lag_artsstatistikk_tabell):
    artsstatistikk_tabell = lag_artsstatistikk_tabell(artsstatistikk_df)
    artsstatistikk_tabell
    return


@app.cell(hide_code=True)
def maanedsgrunnlag_visning(
    arter_df,
    lag_maanedsgrunnlag,
    lag_maanedsgrunnlagstabell,
    valgt_fil,
):
    mo.stop(not valgt_fil.value)
    mo.stop(arter_df.is_empty(), mo.md("Ingen observasjoner i dette utvalget."))

    maanedsgrunnlag_df = lag_maanedsgrunnlag(arter_df)
    maanedsgrunnlag_tabell = lag_maanedsgrunnlagstabell(maanedsgrunnlag_df)
    maanedsgrunnlag_tabell
    return


@app.cell(hide_code=True)
def dekningsmatrise_kontroll(DEKNINGSMATRISE_MAAL, valgt_fil):
    mo.stop(not valgt_fil.value)

    dekningsmatrise_maal = mo.ui.dropdown(
        options={ARTSANTALL_ETIKETT if maal == "Arter" else maal: maal for maal in DEKNINGSMATRISE_MAAL},
        value="Aktive datoer",
        label="Fargelegg rutene etter",
    )
    dekningsmatrise_maal
    return (dekningsmatrise_maal,)


@app.cell(hide_code=True)
def dekningsmatrise_visning(
    arter_df,
    dekningsmatrise_maal,
    lag_dekningsmatrise,
    lag_dekningsmatrisefigur,
    valgt_fil,
):
    mo.stop(not valgt_fil.value)
    mo.stop(arter_df.is_empty(), mo.md("Ingen observasjoner i dette utvalget."))

    dekningsmatrise_df = lag_dekningsmatrise(arter_df)
    dekningsmatrise_figur = mo.ui.altair_chart(
        lag_dekningsmatrisefigur(
            dekningsmatrise_df,
            dekningsmatrise_maal.value,
        ),
        chart_selection=False,
        legend_selection=False,
    )
    dekningsmatrise_figur
    return


@app.cell(column=2, hide_code=True)
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


@app.cell(hide_code=True)
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

    # Eldre Parquet-filer kan fortsatt åpnes, men radnummer gjelder bare denne visningen.
    plotly_id_felt = "obs_id" if "obs_id" in arter_df.columns else "__row_nr"
    plotly_arter_df = arter_df if plotly_id_felt == "obs_id" else arter_df.with_row_index("__row_nr")
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
            "Art av nasjonal forvaltningsinteresse (eks. rødlista)",
            "Atferd",
            "Observert dato",
            "Verdi M1941",
            "Art",
        ],
        custom_data=[plotly_id_felt, "Artens ID", "Navn"],
        zoom=8,
        height=650,
        map_style="white-bg",
        color=farge_kart_arter.value,
        **plotly_color_kwargs,
    )
    plotly_map_fig.update_traces(
        marker={"size": 8, "opacity": 0.9},
        selected={"marker": {"size": 11, "opacity": 1.0}},
        unselected={"marker": {"opacity": 0.35}},
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
    return plotly_arter_df, plotly_id_felt, plotly_map, plotly_map_fig


@app.cell(hide_code=True)
def _(plotly_arter_df, plotly_id_felt, plotly_map, plotly_map_fig):
    selected_obs_ids = [
        plotly_map_fig.data[point["curveNumber"]].customdata[point["pointIndex"]][0]
        for point in plotly_map.points
    ]
    selected_arter_df = plotly_arter_df.filter(pl.col(plotly_id_felt).is_in(selected_obs_ids))
    if plotly_id_felt == "__row_nr":
        selected_arter_df = selected_arter_df.drop("__row_nr")

    mo.vstack(
        [
            mo.md(f"**Valgte observasjoner fra heatmap:** {selected_arter_df.height}"),
            mo.ui.table(selected_arter_df, page_size=10),
        ]
    )
    return (selected_arter_df,)


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


@app.cell(hide_code=True)
def _():
    max_px_value = mo.ui.slider(
        start=1,
        stop=10,
        step=1,
        value=10,
        show_value=True,
        label="Maks spredning",
    )
    threshold_value = mo.ui.slider(
        start=0.0,
        stop=1.0,
        step=0.05,
        value=0.95,
        show_value=True,
        label="Terskel",
    )
    return max_px_value, threshold_value


@app.cell(hide_code=True)
def _(max_px_value, threshold_value):
    stack = mo.vstack(
        [
            max_px_value,
            mo.md("*Største antall piksler punktene kan utvides på hver side.*"),
            threshold_value,
            mo.md("*Tettheten som må nås før spredningen stopper. En høyere terskel gir mer spredning.*"),
        ]
    )

    stack
    return


@app.cell(hide_code=True)
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


if __name__ == "__main__":
    app.run()
