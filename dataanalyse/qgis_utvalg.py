import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")

with app.setup:
    import sys
    import json
    from datetime import date
    from pathlib import Path
    import marimo as mo
    import polars as pl
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from dataanalyse.data_analyse import (
        lag_artsstatistikk, lag_artsstatistikk_tabell, valider_artsstatistikk_input,
    )

    # Lokal plugin: samme filkontrakt i QGIS og marimo, uten PyQGIS i notatboken.
    sys.path.insert(0, str(Path.home() / 'qgis/qgis-naturmangfold/qt6/plugins'))
    from marimo_qgis.selection_session import SelectionClient, load_analysis, make_analysis


@app.function
def les_kalenderdato(datoer: pl.Series) -> pl.Series:
    """Accept Date or strict YYYY-MM-DD JSON transport; never truncate timestamps."""
    if datoer.dtype == pl.Date:
        return datoer
    if datoer.dtype == pl.Null:
        return datoer.cast(pl.Date)
    if datoer.dtype != pl.String:
        raise TypeError('Observasjonsdato må være Date eller YYYY-MM-DD; tidsstempler støttes ikke.')
    if (~datoer.drop_nulls().str.contains(r'^\d{4}-\d{2}-\d{2}$')).any():
        raise ValueError('Observasjonsdato må være YYYY-MM-DD; tidsstempler støttes ikke.')
    try:
        return datoer.str.to_date(format='%Y-%m-%d', strict=True)
    except pl.exceptions.InvalidOperationError as feil:
        raise ValueError('Ugyldig kalenderdato; forventet YYYY-MM-DD.') from feil


@app.function
def til_analysegrunnlag(rader: pl.DataFrame) -> pl.DataFrame:
    """Convert the GeoPackage attributes in a QGIS snapshot to the notebook schema."""
    kolonner = {
        'obs_id': 'obs_id', 'art_id': 'Artens ID', 'vitenskapelig_navn': 'Art',
        'navn': 'Navn', 'rodlistekategori': 'Kategori', 'verdi_m1941': 'Verdi M1941',
        'forvaltningsinteresse': 'Art av nasjonal forvaltningsinteresse (eks. rødlista)',
        'antall': 'Antall', 'atferd_kode': 'Atferd', 'observert_dato': 'Observert dato',
        'familie': 'Familie', 'orden': 'Orden',
    }
    mangler = sorted(set(kolonner) - set(rader.columns))
    if mangler:
        raise ValueError('Laget mangler analysefelt: ' + ', '.join(mangler) +
                         '. Bruk fugleeksporten med kilde-ID, familie og orden.')
    resultat = rader.select(*kolonner).rename(kolonner)
    ids = resultat['obs_id'].cast(pl.String)
    if ids.null_count() or ids.str.strip_chars().eq('').any() or ids.n_unique() != len(ids):
        raise ValueError('obs_id må være utfylt og unik.')
    # Bevar heltallstyper og vitenskapelig navn før den felles valideringen.
    resultat = resultat.with_columns(
        les_kalenderdato(resultat['Observert dato']),
        *[pl.col(navn).cast(pl.String) for navn in resultat.columns
          if navn not in ('Artens ID', 'Antall', 'Art', 'Observert dato')],
    )
    valider_artsstatistikk_input(resultat)
    return resultat


@app.function
def filtrer_observasjoner(
    rader: pl.DataFrame, kriterier: dict[str, list],
    fra_dato: date | None = None, til_dato: date | None = None,
) -> pl.DataFrame:
    """Intersect category filters and inclusive date limits without collapsing records."""
    if fra_dato and til_dato and fra_dato > til_dato:
        raise ValueError('Fra-dato må være før eller lik til-dato.')
    resultat = rader
    for felt, verdier in kriterier.items():
        if verdier:
            resultat = resultat.filter(pl.col(felt).is_in(verdier))
    if fra_dato or til_dato:
        dato = les_kalenderdato(resultat['observert_dato'])
        innenfor = dato.is_not_null()
        if fra_dato:
            innenfor &= dato >= fra_dato
        if til_dato:
            innenfor &= dato <= til_dato
        resultat = resultat.filter(innenfor)
    return resultat


@app.function
def hent_analyserader(snapshot: dict, valgte_ids: list) -> pl.DataFrame:
    """Validate selected JSON integers before Polars can merge bool/int types."""
    ids = set(valgte_ids)
    rader = [rad for rad in snapshot['rows'] if rad[snapshot['id_field']] in ids]
    for rad in rader:
        for felt, etikett in [('antall', 'Antall'), ('art_id', 'Artens ID')]:
            if type(rad.get(felt)) is not int:
                raise TypeError(f'Kolonnen `{etikett}` må inneholde heltall, ikke null, bool, flyttall eller tekst.')
        if not isinstance(rad.get('vitenskapelig_navn'), str):
            raise TypeError('Kolonnen `Art` må inneholde et vitenskapelig navn som tekst.')
    if not rader:
        return pl.from_dicts(snapshot['rows'], infer_schema_length=None).head(0).with_columns(
            pl.col('antall', 'art_id').cast(pl.Int64),
            pl.col('vitenskapelig_navn').cast(pl.String),
        )
    return pl.from_dicts(rader, infer_schema_length=None)


@app.function
def gjenskap_analyse(arkiv: dict) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Recompute saved filters, selection and statistics without QGIS or source files."""
    data = pl.from_dicts(arkiv['snapshot']['rows'], infer_schema_length=None)
    parametere = arkiv['filters']
    filtrert = filtrer_observasjoner(
        data, parametere['criteria'],
        date.fromisoformat(parametere['date_from']) if parametere['date_from'] else None,
        date.fromisoformat(parametere['date_to']) if parametere['date_to'] else None,
    )
    id_felt = arkiv['snapshot']['id_field']
    if set(filtrert[id_felt].to_list()) != set(arkiv['visible_ids']):
        raise ValueError('Lagrede filtre gir et annet datagrunnlag enn det lagrede resultatet.')
    valgte_ids = filtrert.filter(pl.col(id_felt).is_in(arkiv['selected_ids']))[id_felt].to_list()
    valgt = hent_analyserader(arkiv['snapshot'], valgte_ids)
    statistikk = lag_artsstatistikk(til_analysegrunnlag(valgt))
    if statistikk.to_dicts() != arkiv['statistics']:
        raise ValueError('Beregnet artsstatistikk avviker fra den lagrede. Kontroller analysefilen og beregningskoden.')
    return filtrert, valgt, statistikk


@app.cell
def _():
    mo.md("""
    # Fugleobservasjoner: QGIS og marimo

    **Velg rader i tabellen** for å markere observasjonene i QGIS.
    **Velg objekter i QGIS** med polygon-/frihåndsutvalg for å vise alle
    tilhørende observasjoner i resultattabellen nedenfor, også ved overlapp.

    **Filtrene nedenfor styrer både tabellen og det synlige QGIS-laget.**
    Ingen valg i et kategorifilter betyr alle. Datogrenser er inklusive;
    registreringer uten dato tas med når datofilteret er av. Kalenderdato kreves;
    eldre analyser med tidsstempler avvises uten automatisk konvertering.
    Et smalere filter beholder bare kartutvalg som fortsatt passer.

    Artsstatistikken beregnes for kartutvalget. Tabellens eget søk gjelder bare
    tabellen. «Tøm kartutvalget» beholder filtrene; «Nullstill filtre» viser hele
    det tilkoblede datagrunnlaget igjen.

    **Lagre analyse** laster ned datagrunnlag, filtre, utvalg og kontrollresultat
    som én JSON-fil. **Åpne / gjenoppta** gjenoppretter den i QGIS når datagrunnlaget
    stemmer. Uten QGIS vises en skrivebeskyttet gjenskaping av det lagrede resultatet.
    """)
    return


@app.cell
def _():
    tilkoblingsmappe = mo.ui.text(
        value=mo.cli_args().get('qgis-session') or '',
        label='Tilkoblingsmappe fra QGIS', full_width=True, debounce=True,
    )
    tilkoblingsmappe
    return (tilkoblingsmappe,)


@app.cell
def _():
    analysefil = mo.ui.file(filetypes=['.json'], max_size=50_000_000,
        label='Lagret fugleanalyse').form(submit_button_label='Åpne / gjenoppta',
        show_clear_button=True, clear_button_label='Lukk lagret analyse')
    analysefil
    return (analysefil,)


@app.cell
def _(analysefil, tilkoblingsmappe):
    lagret_analyse = None
    forbindelse = None
    snapshot = None
    lastefeil = None
    frakoblet = not bool(tilkoblingsmappe.value)
    try:
        if analysefil.value:
            lagret_analyse = load_analysis(analysefil.value[0].contents)
        elif mo.cli_args().get('analyse'):
            lagret_analyse = load_analysis(Path(mo.cli_args()['analyse']).read_bytes())
        if lagret_analyse is not None:
            gjenskap_analyse(lagret_analyse)
        if not frakoblet:
            forbindelse = SelectionClient(tilkoblingsmappe.value, saved_analysis=lagret_analyse)
            snapshot = forbindelse.snapshot
        elif lagret_analyse is not None:
            snapshot = lagret_analyse['snapshot']
    except (OSError, TypeError, ValueError, pl.exceptions.PolarsError) as feil:
        lastefeil = str(feil)
    mo.stop(lastefeil is not None, mo.callout(lastefeil or '', kind='warn'))
    mo.stop(snapshot is None, mo.md('Start tilkoblingen i QGIS, eller åpne en lagret analyse ovenfor.'))
    observasjoner = pl.from_dicts(snapshot['rows'], infer_schema_length=None)
    return forbindelse, frakoblet, lagret_analyse, observasjoner, snapshot


@app.cell
def _(frakoblet, lagret_analyse):
    lagret_analyse
    nullstill_filtre = mo.ui.button(value=0, on_click=lambda verdi: verdi + 1,
        label='Nullstill filtre', disabled=frakoblet)
    nullstill_filtre
    return (nullstill_filtre,)


@app.cell
def _(frakoblet, lagret_analyse, nullstill_filtre, observasjoner):
    startfilter = lagret_analyse['filters'] if lagret_analyse and nullstill_filtre.value == 0 else {'criteria': {}, 'date_from': None, 'date_to': None}
    startvalg = startfilter['criteria']
    def filtervalg(felt):
        return sorted(observasjoner[felt].drop_nulls().unique().to_list()) if felt in observasjoner.columns else []

    artsvalg = {
        f"{rad['navn'] or rad['vitenskapelig_navn']} ({rad['art_id']})": rad['art_id']
        for rad in observasjoner.select('art_id', 'navn', 'vitenskapelig_navn').unique().sort('navn').to_dicts()
    } if {'art_id', 'navn', 'vitenskapelig_navn'} <= set(observasjoner.columns) else {}
    art_filter = mo.ui.multiselect(artsvalg, value=[k for k, v in artsvalg.items() if v in startvalg.get('art_id', [])], label='Art', full_width=True, disabled=frakoblet)
    sesong_filter = mo.ui.multiselect(filtervalg('sesong'), value=startvalg.get('sesong', []), label='Sesong', full_width=True, disabled=frakoblet)
    aktivitet_filter = mo.ui.multiselect(filtervalg('funksjon'), value=startvalg.get('funksjon', []), label='Aktivitet', full_width=True, disabled=frakoblet)
    verdi_filter = mo.ui.multiselect(filtervalg('verdi_m1941'), value=startvalg.get('verdi_m1941', []), label='Verdi M1941', full_width=True, disabled=frakoblet)
    datoer = observasjoner['observert_dato'].drop_nulls().sort() if 'observert_dato' in observasjoner.columns else []
    bruk_dato = mo.ui.checkbox(value=bool(startfilter['date_from'] or startfilter['date_to']), label='Avgrens dato', disabled=frakoblet or not len(datoer))
    fra_dato = mo.ui.date(value=startfilter['date_from'] or (str(datoer[0])[:10] if len(datoer) else date.today()), label='Fra og med', disabled=frakoblet)
    til_dato = mo.ui.date(value=startfilter['date_to'] or (str(datoer[-1])[:10] if len(datoer) else date.today()), label='Til og med', disabled=frakoblet)
    mo.vstack([
        mo.hstack([art_filter, sesong_filter], widths='equal'),
        mo.hstack([aktivitet_filter, verdi_filter], widths='equal'),
        mo.hstack([bruk_dato, fra_dato, til_dato]),
    ])
    return aktivitet_filter, art_filter, bruk_dato, fra_dato, sesong_filter, til_dato, verdi_filter


@app.cell
def _(aktivitet_filter, art_filter, bruk_dato, forbindelse, fra_dato, frakoblet, observasjoner, sesong_filter, snapshot, til_dato, verdi_filter):
    mo.stop(bruk_dato.value and fra_dato.value > til_dato.value,
            mo.callout('Fra-dato må være før eller lik til-dato. Forrige kartfilter er fortsatt aktivt.', kind='warn'))
    filterparametere = {
        'criteria': {'art_id': art_filter.value, 'sesong': sesong_filter.value,
                     'funksjon': aktivitet_filter.value, 'verdi_m1941': verdi_filter.value},
        'date_from': fra_dato.value.isoformat() if bruk_dato.value else None,
        'date_to': til_dato.value.isoformat() if bruk_dato.value else None,
    }
    filtrerte_observasjoner = filtrer_observasjoner(
        observasjoner, filterparametere['criteria'],
        fra_dato.value if bruk_dato.value else None,
        til_dato.value if bruk_dato.value else None,
    )
    filterforesporsel = 'saved' if frakoblet else forbindelse.filter(filtrerte_observasjoner[snapshot['id_field']].to_list())
    return filterforesporsel, filterparametere, filtrerte_observasjoner


@app.cell
def _(filterforesporsel, filtrerte_observasjoner, forbindelse, frakoblet, hent_filterstatus, snapshot):
    mo.stop(not frakoblet and hent_filterstatus() != ('active', filterforesporsel), mo.md('Venter på at QGIS bekrefter filteret.'))
    def send_tabellutvalg(rader):
        forbindelse.select([] if rader is None else rader[forbindelse.id_field].to_list(),
                           filter_request_id=filterforesporsel)

    observasjonstabell = mo.ui.table(
        filtrerte_observasjoner, page_size=15, selection=None if frakoblet else 'multi',
        on_change=None if frakoblet else send_tabellutvalg, label='Lagret filter' if frakoblet else 'Velg observasjoner → QGIS',
        freeze_columns_left=[snapshot['id_field']],
    )
    tom_utvalg = mo.ui.button(label='Tøm kartutvalget', disabled=frakoblet,
        on_click=lambda _: forbindelse.select([], filter_request_id=filterforesporsel))
    mo.vstack([mo.md(f'**{filtrerte_observasjoner.height} observasjoner i filteret**'), observasjonstabell, tom_utvalg])
    return


@app.cell
def _():
    hent_kartstatus, sett_kartstatus = mo.state(None)
    hent_filterstatus, sett_filterstatus = mo.state(None)
    oppdatering = mo.ui.refresh(options=[1, 2, 5], default_interval=1, label='Hent utvalg fra QGIS')
    oppdatering
    return hent_filterstatus, hent_kartstatus, oppdatering, sett_filterstatus, sett_kartstatus


@app.cell
def _(forbindelse, frakoblet, hent_filterstatus, hent_kartstatus, lagret_analyse, oppdatering, sett_filterstatus, sett_kartstatus):
    oppdatering.value
    kartstatus = {'status': 'saved', 'filter_request_id': 'saved', 'selected_ids': lagret_analyse['selected_ids'], 'error': ''} if frakoblet else forbindelse.read_state()
    filterstatus = (kartstatus['status'], kartstatus.get('filter_request_id'))
    if filterstatus != hent_filterstatus():
        sett_filterstatus(filterstatus)
    if kartstatus != hent_kartstatus():
        sett_kartstatus(kartstatus)
    return


@app.cell
def _(frakoblet, hent_kartstatus, observasjoner, snapshot):
    status = hent_kartstatus()
    mo.stop(status is None)
    try:
        valgte_observasjoner = hent_analyserader(snapshot, status['selected_ids'])
        utvalgsfeil = None
    except (TypeError, ValueError, pl.exceptions.PolarsError) as feil:
        valgte_observasjoner = observasjoner.head(0)
        utvalgsfeil = str(feil)
    mo.stop(utvalgsfeil is not None, mo.callout(utvalgsfeil or '', kind='warn'))
    statusmelding = status['error'] or ('Lagret analyse — uten QGIS' if frakoblet else 'Tilkoblet QGIS')
    mo.vstack([
        mo.md(f"**{statusmelding}** — {valgte_observasjoner.height} valgte observasjoner"),
        mo.ui.table(valgte_observasjoner, selection=None, page_size=15, label='Lagret utvalg' if frakoblet else 'Gjeldende utvalg i QGIS'),
    ])
    return (valgte_observasjoner,)


@app.cell
def _(valgte_observasjoner):
    try:
        analysegrunnlag = til_analysegrunnlag(valgte_observasjoner)
        analysefeil = None
    except (TypeError, ValueError, pl.exceptions.PolarsError) as feil:
        analysegrunnlag = None
        analysefeil = str(feil)
    mo.stop(analysefeil is not None, mo.callout(analysefeil or '', kind='warn'))
    artsstatistikk_utvalg = lag_artsstatistikk(analysegrunnlag)
    mo.vstack([
        mo.md(f'## Artsstatistikk for kartutvalget — Arter (inkl. underarter): {artsstatistikk_utvalg.height}'),
        lag_artsstatistikk_tabell(artsstatistikk_utvalg)
        if artsstatistikk_utvalg.height else mo.md('Velg observasjoner i QGIS eller tabellen for å beregne artsstatistikk.'),
    ])
    return (artsstatistikk_utvalg,)


@app.cell
def _(filterforesporsel, filterparametere, filtrerte_observasjoner, forbindelse, frakoblet, hent_kartstatus, lagret_analyse, snapshot):
    lagringsstatus = hent_kartstatus()
    mo.stop(lagringsstatus is None)
    mo.stop(not frakoblet and (lagringsstatus['status'] != 'active' or lagringsstatus.get('filter_request_id') != filterforesporsel))
    def last_ned_analyse():
        if frakoblet:
            arkiv = lagret_analyse
        else:
            # Les ved klikk, slik at det nyeste QGIS-utvalget lagres.
            siste = forbindelse.read_state()
            if siste['status'] != 'active' or siste['filter_request_id'] != filterforesporsel:
                raise ValueError('QGIS-filteret er endret eller tilkoblingen avsluttet. Vent på oppdatert visning.')
            valgte_ids = filtrerte_observasjoner.filter(
                pl.col(snapshot['id_field']).is_in(siste['selected_ids'])
            )[snapshot['id_field']].to_list()
            valgt = hent_analyserader(snapshot, valgte_ids)
            statistikk = lag_artsstatistikk(til_analysegrunnlag(valgt))
            arkiv = make_analysis(snapshot, filterparametere,
                filtrerte_observasjoner[snapshot['id_field']].to_list(), siste['selected_ids'], statistikk.to_dicts())
        return json.dumps(arkiv, ensure_ascii=False, indent=2, allow_nan=False).encode()

    mo.download(last_ned_analyse, filename='qgis_fugleanalyse.json', mimetype='application/json', label='Lagre analyse')
    return


if __name__ == "__main__":
    app.run()
