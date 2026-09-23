"""Trinn 2: bevar kildeidentitet og bruk den eksisterende artsstatistikken."""

from datetime import date
from html.parser import HTMLParser
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from dataanalyse.data_analyse import lag_artsstatistikk
from dataanalyse.qgis_utvalg import til_analysegrunnlag
from databehandling.databehandling import legg_til_observasjons_id
from tests_KI.notebook_cells import load_notebook_cells


NOTEBOOK = load_notebook_cells()
AGGREGATORER = [NOTEBOOK[name] for name in (
    'lag_artsstatistikk', 'lag_maanedsgrunnlag', 'lag_dekningsmatrise',
)]


def analysefixture() -> pl.DataFrame:
    """Planens håndregnede åtte rader, uten produksjonshjelper som tallfasit."""
    return pl.DataFrame({
        'obs_id': [f'obs-{i}' for i in range(8)],
        'Artens ID': [7, 7, 7, 7, 7, 8, 9, 9],
        'Art': ['A', 'A', 'A', 'A', 'B', 'B', 'Genus', 'Genus'],
        'Antall': [2, 4, 0, 10, 3, 5, 6, 0],
        'Observert dato': [date(2020, 1, 1), date(2021, 3, 1), date(2021, 3, 31),
                           None, date(2020, 1, 1), date(2020, 1, 2), None, None],
        'Navn': ['fellesnavn'] * 8, 'Kategori': ['LC'] * 8,
        'Verdi M1941': ['Noe verdi'] * 8,
        'Art av nasjonal forvaltningsinteresse (eks. rødlista)': ['Nei'] * 8,
        'Atferd': pl.Series([None] * 8, dtype=pl.String),
        'Familie': ['Familie'] * 8, 'Orden': ['Orden'] * 8,
        'Observatør': ['Ola'] * 8, 'Lokalitet': ['A'] * 8,
    })


@pytest.mark.parametrize('aggregator', AGGREGATORER, ids=lambda f: f.__name__)
@pytest.mark.parametrize('antall', [
    pl.Series('Antall', [-1]), pl.Series('Antall', [None], dtype=pl.Int64),
    pl.Series('Antall', [2.9]), pl.Series('Antall', [2.0]),
    pl.Series('Antall', [True]), pl.Series('Antall', ['2']),
    pl.Series('Antall', [2**63], dtype=pl.UInt64),
])
def test_ugyldige_individtall_stopper_alle_aggregatorer(aggregator, antall):
    with pytest.raises((TypeError, ValueError), match='Antall|Int64'):
        aggregator(analysefixture().head(1).with_columns(antall))


@pytest.mark.parametrize('aggregator', AGGREGATORER, ids=lambda f: f.__name__)
def test_heltallssummer_er_eksakte_og_global_overflyt_avvises(aggregator):
    smale = analysefixture().head(2).with_columns(
        pl.Series('Antall', [2**31 - 1, 1], dtype=pl.Int32),
        pl.lit(date(2020, 1, 1)).alias('Observert dato'),
    )
    assert sum(aggregator(smale)['Individer']) == 2147483648
    stor = smale.head(1).with_columns(pl.lit(2**63 - 1).alias('Antall'))
    assert sum(aggregator(stor)['Individer']) == 9223372036854775807
    for ids, datoer in [([7, 7], [date(2020, 1, 1)] * 2),
                        ([7, 8], [date(2020, 1, 1), date(2021, 3, 1)])]:
        over = smale.with_columns(pl.Series('Antall', [2**63 - 1, 1]), pl.Series('Artens ID', ids),
                                 pl.Series('Observert dato', datoer))
        with pytest.raises(ValueError, match='Int64'):
            aggregator(over)


def test_handfasit_kildepar_datoer_og_udatert_nevner():
    frame = analysefixture()
    stats = lag_artsstatistikk(frame)
    assert (stats.height, sum(stats['Observasjoner']), sum(stats['Individer'])) == (4, 8, 30)
    groups = {(r['Artens ID'], r['Art']): r for r in stats.to_dicts()}
    assert set(groups) == {(7, 'A'), (7, 'B'), (8, 'B'), (9, 'Genus')}
    for key, expected in {(7, 'A'): (4, 16, 4.0), (7, 'B'): (1, 3, 3.0),
                          (8, 'B'): (1, 5, 5.0), (9, 'Genus'): (2, 6, 3.0)}.items():
        row = groups[key]
        assert (row['Observasjoner'], row['Individer'], row['Gj.snitt individer']) == expected
    assert groups[7, 'A']['Månedsprofil'] == [1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert groups[9, 'Genus']['Månedsprofil'] == [0] * 12
    for name, height in [('lag_maanedsgrunnlag', 12), ('lag_dekningsmatrise', 24)]:
        result = NOTEBOOK[name](frame)
        assert result.height == height
        assert (sum(result['Registreringer']), sum(result['Individer'])) == (5, 14)
        dated = result.filter(pl.col('Registreringer') > 0)
        assert dated.select('Måned', 'Registreringer', 'Individer', 'Aktive datoer', 'Arter').rows() == [
            (1, 3, 10, 2, 3), (3, 2, 4, 2, 1),
        ]
    undated = frame.with_columns(pl.lit(None, dtype=pl.Date).alias('Observert dato'))
    assert_frame_equal(lag_artsstatistikk(undated).select('Artens ID', 'Art', 'Observasjoner', 'Individer', 'Gj.snitt individer'),
                       stats.select('Artens ID', 'Art', 'Observasjoner', 'Individer', 'Gj.snitt individer'))
    assert NOTEBOOK['lag_dekningsmatrise'](undated).is_empty()
    assert sum(NOTEBOOK['lag_maanedsgrunnlag'](undated)['Registreringer']) == 0
    for aggregator in AGGREGATORER:
        aggregator(frame.head(0))


@pytest.mark.parametrize('aggregator', AGGREGATORER, ids=lambda f: f.__name__)
@pytest.mark.parametrize('endring', [
    pl.Series('Artens ID', [None], dtype=pl.Int64), pl.Series('Artens ID', [1.9]),
    pl.Series('Artens ID', [True]), pl.Series('Artens ID', ['7']),
    pl.Series('Art', [None], dtype=pl.String), pl.Series('Art', ['']), pl.Series('Art', [' \t\n']),
])
def test_ufullstendige_nokler_avvises_ogsa_uten_dato(aggregator, endring):
    frame = analysefixture().head(1).with_columns(endring, pl.lit(None, dtype=pl.Date).alias('Observert dato'))
    with pytest.raises((TypeError, ValueError), match='Art'):
        aggregator(frame)


@pytest.mark.parametrize('aggregator', AGGREGATORER, ids=lambda f: f.__name__)
@pytest.mark.parametrize('dtype', [pl.Datetime, pl.Datetime('us', 'Europe/Oslo')])
def test_tidsstempler_avvises_ogsa_ved_midnatt(aggregator, dtype):
    with pytest.raises(TypeError, match='Date'):
        aggregator(analysefixture().with_columns(pl.col('Observert dato').cast(dtype)))


def test_nokler_bevares_uten_positivitetskrav_eller_normalisering():
    frame = analysefixture().head(2).with_columns(pl.Series('Artens ID', [-1, 0]),
                                                 pl.Series('Art', [' A ', 'A']))
    assert lag_artsstatistikk(frame).select('Artens ID', 'Art').rows() == [(-1, ' A '), (0, 'A')]
    large_id = frame.head(1).with_columns(pl.Series('Artens ID', [2**64 - 1], dtype=pl.UInt64))
    assert lag_artsstatistikk(large_id)['Artens ID'].item() == 18446744073709551615


@pytest.mark.parametrize('navn', sorted(name for name in NOTEBOOK if name.startswith((
    'test_artsstatistikk_mtm_', 'test_dekningsmatrise_mtm_', 'test_maanedsgrunnlag_mtm_',
))))
def test_innebygde_notebooktester(navn):
    NOTEBOOK[navn]()


def qgisfixture() -> pl.DataFrame:
    return analysefixture().rename({
        'Artens ID': 'art_id', 'Art': 'vitenskapelig_navn', 'Navn': 'navn',
        'Kategori': 'rodlistekategori', 'Verdi M1941': 'verdi_m1941',
        'Art av nasjonal forvaltningsinteresse (eks. rødlista)': 'forvaltningsinteresse',
        'Antall': 'antall', 'Atferd': 'atferd_kode', 'Observert dato': 'observert_dato',
        'Familie': 'familie', 'Orden': 'orden',
    }).with_columns(pl.col('observert_dato').cast(pl.String))


@pytest.mark.parametrize('felt', ['antall', 'art_id'])
@pytest.mark.parametrize('value', [2.9, 2.0, True, '2', None])
def test_adapter_avviser_ugyldige_tall_for_cast(felt, value):
    with pytest.raises((TypeError, ValueError), match='Antall|Artens ID'):
        til_analysegrunnlag(qgisfixture().head(1).with_columns(pl.lit(value).alias(felt)))


@pytest.mark.parametrize('dato', ['2020-01-01T00:00:00', '2020-01-01 08:00:00',
                                 '2020-01-01T00:30:00+02:00', '2020-1-1', '2020-02-30'])
def test_adapter_og_datofilter_avviser_annet_enn_kalenderdato(dato):
    from dataanalyse.qgis_utvalg import filtrer_observasjoner
    rader = qgisfixture().head(1).with_columns(pl.lit(dato).alias('observert_dato'))
    with pytest.raises((TypeError, ValueError), match='dato|Date|YYYY-MM-DD'):
        til_analysegrunnlag(rader)
    with pytest.raises((TypeError, ValueError), match='dato|Date|YYYY-MM-DD'):
        filtrer_observasjoner(rader, {}, date(2020, 1, 1), date(2020, 12, 31))


def test_adapter_bevarer_date_null_og_store_id_er():
    from dataanalyse.qgis_utvalg import filtrer_observasjoner
    rader = qgisfixture().head(1).with_columns(
        pl.Series('art_id', [2**64 - 1], dtype=pl.UInt64),
        pl.lit(None).alias('observert_dato'),
    )
    resultat = til_analysegrunnlag(rader)
    assert resultat['Artens ID'].item() == 18446744073709551615
    assert resultat.schema['Observert dato'] == pl.Date
    assert resultat['Observert dato'].item() is None
    assert filtrer_observasjoner(rader, {}, date(2020, 1, 1)).is_empty()
    datert = qgisfixture().head(1).with_columns(pl.lit(date(2020, 1, 1)).alias('observert_dato'))
    assert til_analysegrunnlag(datert)['Observert dato'].item() == date(2020, 1, 1)
    assert_frame_equal(filtrer_observasjoner(datert, {}, date(2020, 1, 1)), datert)
    assert_frame_equal(filtrer_observasjoner(qgisfixture(), {'art_id': [8]}), qgisfixture().filter(pl.col('art_id') == 8))


class TabellHTML(HTMLParser):
    """Les faktisk formatter-HTML uten nettleser eller wrapper-CSP."""
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.tekst = []
        self.elementer = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.elementer.append((tag, dict(attrs)))

    def handle_data(self, data):
        self.tekst.append(data)


def test_delt_renderer_escaper_kildetekst_en_gang_uten_csp():
    payload = '<img src="x" onerror="alert(1)"><script>alert(2)</script> Æ & "ø"'
    tekstfelt = ['Art', 'Navn', 'Familie', 'Orden', 'Art av nasjonal forvaltningsinteresse (eks. rødlista)', 'Verdi M1941']
    stats = lag_artsstatistikk(analysefixture().head(1).with_columns(
        *[pl.lit(payload).alias(felt) for felt in tekstfelt],
    ))
    html = NOTEBOOK['lag_artsstatistikk_tabell'](stats).as_raw_html()
    dom = TabellHTML(html)
    assert not any(tag in ('img', 'script') for tag, _ in dom.elementer)
    assert ''.join(dom.tekst).count(payload) == 6
    assert '@import' not in html


def test_m1941_uklassifisert_er_ikke_vurdert_uten_betydning():
    values = ['Uten betydning for KU', 'Ingen', None, 'Ukjent & verdi']
    frame = analysefixture().head(4).with_columns(pl.Series('Artens ID', [1, 2, 3, 4]),
                                                 pl.Series('Verdi M1941', values))
    dom = TabellHTML(NOTEBOOK['lag_artsstatistikk_tabell'](lag_artsstatistikk(frame)).as_raw_html())
    badges = [attrs for tag, attrs in dom.elementer if tag == 'span' and 'data-m1941' in attrs]
    assert len(badges) == 4
    assessed = [attrs for attrs in badges if attrs['data-m1941'] == 'vurdert']
    assert len(assessed) == 1 and 'background-color:#D9D9D9' in assessed[0]['style']
    assert all('background-color:#D9D9D9' not in attrs['style'] for attrs in badges if attrs['data-m1941'] == 'uklassifisert')
    assert all(text in ''.join(dom.tekst) for text in ['Ingen', 'Uklassifisert', 'Ukjent & verdi'])


def test_tabellviser_eksakte_int64_heltall_uten_flyttallsavrunding():
    stats = lag_artsstatistikk(analysefixture().head(1).with_columns(pl.lit(2**63 - 1).alias('Antall')))
    dom = TabellHTML(NOTEBOOK['lag_artsstatistikk_tabell'](stats).as_raw_html())
    assert '9 223 372 036 854 775 807' in [text.replace('\u00a0', ' ') for text in dom.tekst]
    assert '9223372036854775807 summerte, behandlede individtall' in ''.join(dom.tekst)
    month = NOTEBOOK['lag_maanedsgrunnlag'](analysefixture().head(1).with_columns(pl.lit(2**63 - 1).alias('Antall')))
    assert '9 223 372 036 854 775 807' in [text.replace('\u00a0', ' ') for text in TabellHTML(NOTEBOOK['lag_maanedsgrunnlagstabell'](month).as_raw_html()).tekst]


def test_datodekning_stripe_prosent_og_eksakt_udatert_tekst():
    stats = lag_artsstatistikk(analysefixture())
    html = NOTEBOOK['lag_artsstatistikk_tabell'](stats).as_raw_html()
    dom = TabellHTML(html)
    assert '75 %' in dom.tekst and dom.tekst.count('100 %') == 2
    assert dom.tekst.count('Ingen daterte observasjoner') == 1
    assert sum(tag == 'svg' for tag, _ in dom.elementer) == 3
    assert sum(attrs.get('class') == 'datodekning' for _, attrs in dom.elementer) == 3
    assert 'Arter (inkl. underarter)' in ''.join(dom.tekst)
    assert 'høyere taksonomiske nivåer og navnevarianter' in ''.join(dom.tekst)
    # Syntetiske aggregerte rader tester avrundingsgrenser uten millioner av kilderader.
    for dated, total, text in [(9999, 10000, '>99,9 %'), (1, 10000, '<0,1 %')]:
        edge = stats.head(1).with_columns(pl.lit(total).alias('Observasjoner'),
                                         pl.Series('Månedsprofil', [[dated] + [0] * 11]))
        rendered = TabellHTML(NOTEBOOK['lag_artsstatistikk_tabell'](edge).as_raw_html())
        assert text in rendered.tekst
        assert '100 %' not in rendered.tekst and '0 %' not in rendered.tekst
    undated = analysefixture().with_columns(pl.lit(None, dtype=pl.Date).alias('Observert dato'))
    rendered = TabellHTML(NOTEBOOK['lag_artsstatistikk_tabell'](lag_artsstatistikk(undated)).as_raw_html())
    assert rendered.tekst.count('Ingen daterte observasjoner') == 4
    assert not any(tag == 'svg' or attrs.get('class') == 'datodekning' for tag, attrs in rendered.elementer)
    month_html = NOTEBOOK['lag_maanedsgrunnlagstabell'](NOTEBOOK['lag_maanedsgrunnlag'](undated)).as_raw_html()
    assert 'Ingen daterte observasjoner' in ''.join(TabellHTML(month_html).tekst)
    assert not any(tag == 'td' and 'gt_row' in attrs.get('class', '').split()
                   for tag, attrs in TabellHTML(month_html).elementer)
    figure = NOTEBOOK['lag_dekningsmatrisefigur'](NOTEBOOK['lag_dekningsmatrise'](undated), 'Arter').to_dict()
    assert figure['mark']['type'] == 'text' and 'Ingen daterte observasjoner' in str(figure)


def test_maanedspresentasjoner_bruker_kvalifisert_artsordlyd():
    month = NOTEBOOK['lag_maanedsgrunnlag'](analysefixture())
    text = ''.join(TabellHTML(NOTEBOOK['lag_maanedsgrunnlagstabell'](month).as_raw_html()).tekst)
    assert 'Arter (inkl. underarter)' in text
    figure = NOTEBOOK['lag_dekningsmatrisefigur'](NOTEBOOK['lag_dekningsmatrise'](analysefixture()), 'Arter').to_dict()
    assert figure['encoding']['color']['title'] == 'Arter (inkl. underarter)'
    assert 'Arter (inkl. underarter)' in [tip['title'] for tip in figure['encoding']['tooltip']]


@pytest.mark.parametrize('felt,verdi', [
    (felt, verdi) for felt in ['antall', 'art_id'] for verdi in [True, 1.0, '1']
] + [('vitenskapelig_navn', 1)])
def test_json_replay_validerer_ra_valgte_tall_uten_at_utelatte_rader_forgifter_typen(felt, verdi):
    from dataanalyse.qgis_utvalg import gjenskap_analyse
    rader = qgisfixture().head(2).with_columns(pl.lit(1).alias('antall'), pl.lit(1).alias('art_id'))
    stats = lag_artsstatistikk(til_analysegrunnlag(rader))
    arkiv = {'snapshot': {'rows': rader.to_dicts(), 'id_field': 'obs_id'},
             'filters': {'criteria': {}, 'date_from': None, 'date_to': None},
             'visible_ids': ['obs-0', 'obs-1'], 'selected_ids': ['obs-0', 'obs-1'],
             'statistics': stats.to_dicts()}
    arkiv['snapshot']['rows'][1][felt] = verdi
    with pytest.raises((TypeError, ValueError), match='Antall|Art'):
        gjenskap_analyse(arkiv)
    arkiv['selected_ids'] = ['obs-0']
    arkiv['statistics'] = lag_artsstatistikk(til_analysegrunnlag(rader.head(1))).to_dicts()
    assert gjenskap_analyse(arkiv)[2]['Individer'].item() == 1


@pytest.mark.parametrize('dtype', [pl.Datetime, pl.Datetime('us', 'Europe/Oslo')])
def test_native_tidsstempler_avvises_i_adapter_og_filter(dtype):
    from dataanalyse.qgis_utvalg import filtrer_observasjoner
    rader = qgisfixture().with_columns(pl.col('observert_dato').str.to_date().cast(dtype))
    with pytest.raises(TypeError, match='Date'):
        til_analysegrunnlag(rader)
    with pytest.raises(TypeError, match='Date'):
        filtrer_observasjoner(rader, {}, date(2020, 1, 1))


def test_kilde_id_bevares_ved_sortering_og_filter():
    kilde = pl.DataFrame({'proxyId': ['kilde/a', 'kilde/b'], 'art': ['samme', 'samme']})
    resultat = legg_til_observasjons_id(kilde.reverse())
    assert resultat['obs_id'].to_list() == ['kilde/b', 'kilde/a']
    assert resultat.filter(pl.col('proxyId') == 'kilde/a')['obs_id'].to_list() == ['kilde/a']


@pytest.mark.parametrize('ids', [[None], [''], [' '], ['lik', 'lik']])
def test_ugyldig_kilde_id_avvises(ids):
    with pytest.raises(ValueError):
        legg_til_observasjons_id(pl.DataFrame({'proxyId': ids}))


def test_manglende_kilde_id_avvises():
    with pytest.raises(ValueError, match='proxyId'):
        legg_til_observasjons_id(pl.DataFrame({'art': ['art']}))


def test_qgis_utvalg_bruker_samme_statistikk_og_taler_tomt_utvalg():
    # To kilderegistreringer kan ha samme art, dato og koordinat uten å være én observasjon.
    rader = pl.DataFrame({
        'obs_id': ['kilde/a', 'kilde/b'], 'art_id': [1, 1],
        'vitenskapelig_navn': ['Testus', 'Testus'], 'navn': ['Testart', 'Testart'],
        'rodlistekategori': ['LC', 'LC'], 'verdi_m1941': ['Noe verdi', 'Noe verdi'],
        'forvaltningsinteresse': [None, None], 'antall': [2, 3],
        'atferd_kode': [None, None], 'observert_dato': ['2020-06-01'] * 2,
        'familie': ['Familie'] * 2, 'orden': ['Orden'] * 2,
    })
    forventet = pl.DataFrame({
        'obs_id': ['kilde/a', 'kilde/b'], 'Artens ID': [1, 1],
        'Art': ['Testus', 'Testus'], 'Navn': ['Testart', 'Testart'], 'Kategori': ['LC', 'LC'],
        'Verdi M1941': ['Noe verdi', 'Noe verdi'],
        'Art av nasjonal forvaltningsinteresse (eks. rødlista)': pl.Series([None, None], dtype=pl.String),
        'Antall': [2, 3], 'Atferd': pl.Series([None, None], dtype=pl.String),
        'Observert dato': [date(2020, 6, 1)] * 2, 'Familie': ['Familie'] * 2, 'Orden': ['Orden'] * 2,
    })
    assert_frame_equal(til_analysegrunnlag(rader), forventet)
    for ids in ([], ['kilde/a'], ['kilde/b'], ['kilde/b', 'kilde/a']):
        qgis = til_analysegrunnlag(rader.filter(pl.col('obs_id').is_in(ids)))
        vanlig = forventet.filter(pl.col('obs_id').is_in(ids))
        assert_frame_equal(lag_artsstatistikk(qgis), lag_artsstatistikk(vanlig))
    assert lag_artsstatistikk(til_analysegrunnlag(rader.head(0))).height == 0
    with pytest.raises(ValueError, match='familie'):
        til_analysegrunnlag(rader.drop('familie'))
    with pytest.raises(ValueError, match='YYYY-MM-DD'):
        til_analysegrunnlag(rader.with_columns(pl.lit('ikke en dato').alias('observert_dato')))


def test_filtre_kombineres_uten_tap_av_overlappende_observasjoner():
    from dataanalyse.qgis_utvalg import filtrer_observasjoner
    rader = pl.DataFrame({
        'obs_id': ['A', 'B', 'C', 'D'], 'art_id': [1, 2, 1, 1],
        'sesong': ['Sommer', 'Sommer', 'Vinter', 'Sommer'],
        'funksjon': ['Næringssøk', 'Næringssøk', 'Rasting', 'Næringssøk'],
        'verdi_m1941': ['Stor', 'Middels', 'Stor', 'Stor'],
        'observert_dato': ['2020-06-01', '2020-06-30', '2021-01-01', None],
        'x': [0, 0, 0, 0], 'y': [0, 0, 0, 0],
    })
    assert_frame_equal(filtrer_observasjoner(rader, {}), rader)
    assert_frame_equal(filtrer_observasjoner(rader, {'art_id': []}), rader)
    assert filtrer_observasjoner(rader, {'sesong': ['Sommer']})['obs_id'].to_list() == ['A', 'B', 'D']
    kriterier = {'art_id': [1], 'sesong': ['Sommer'], 'funksjon': ['Næringssøk'], 'verdi_m1941': ['Stor']}
    assert filtrer_observasjoner(rader, kriterier)['obs_id'].to_list() == ['A', 'D']
    assert filtrer_observasjoner(rader, kriterier, date(2020, 6, 1), date(2020, 6, 30))['obs_id'].to_list() == ['A']
    assert filtrer_observasjoner(rader, {}, date(2020, 6, 1), date(2020, 6, 30))['obs_id'].to_list() == ['A', 'B']
    assert filtrer_observasjoner(rader, {'art_id': [2], 'sesong': ['Vinter']}).is_empty()
    with pytest.raises(ValueError, match='Fra-dato'):
        filtrer_observasjoner(rader, {}, date(2021, 1, 1), date(2020, 1, 1))


def test_lagret_analyse_gjenskapes_og_avviser_endret_datagrunnlag():
    import copy
    import json
    from dataanalyse.qgis_utvalg import gjenskap_analyse
    from marimo_qgis.selection_session import make_analysis, load_analysis, snapshot_fingerprint

    rad = {
        'obs_id': 'kilde/a', 'fid': 1, 'art_id': 1, 'vitenskapelig_navn': 'Testus',
        'navn': 'Testart', 'rodlistekategori': 'LC', 'verdi_m1941': 'Noe verdi',
        'forvaltningsinteresse': None, 'antall': 2, 'atferd_kode': None,
        'observert_dato': '2020-06-01', 'familie': 'Familie', 'orden': 'Orden',
        'sesong': 'Sommer', 'funksjon': 'Ikke angitt',
    }
    snapshot = {'id_field': 'obs_id', 'crs': 'EPSG:25833',
        'rows': [rad, {**rad, 'obs_id': 'kilde/b', 'fid': 2, 'antall': 3}],
        'geometry_wkb': {'kilde/a': '', 'kilde/b': ''}, 'source': '/does/not/exist.gpkg'}
    data = pl.from_dicts(snapshot['rows'], infer_schema_length=None)
    filters = {'criteria': {'sesong': ['Sommer']}, 'date_from': '2020-06-01', 'date_to': '2020-06-01'}
    for ids in ([], ['kilde/a'], ['kilde/a', 'kilde/b']):
        selected = data.filter(pl.col('obs_id').is_in(ids))
        stats = lag_artsstatistikk(til_analysegrunnlag(selected))
        archive = make_analysis(snapshot, filters, ['kilde/a', 'kilde/b'], ids, stats.to_dicts())
        loaded = load_analysis(json.dumps(archive).encode())
        visible, replayed, result = gjenskap_analyse(loaded)
        assert visible.height == 2
        assert_frame_equal(replayed, selected)
        assert_frame_equal(result, stats)
    reordered = copy.deepcopy(snapshot)
    reordered['rows'].reverse()
    reordered['rows'][0]['fid'] = 987
    reordered['source'] = '/moved/data.gpkg'
    assert snapshot_fingerprint(reordered) == snapshot_fingerprint(snapshot)
    for key, value in [('antall', 99), ('obs_id', 'ny'), ('navn', 'endret')]:
        damaged = copy.deepcopy(archive)
        damaged['snapshot']['rows'][0][key] = value
        with pytest.raises(ValueError):
            load_analysis(json.dumps(damaged).encode())
    changed_geometry = copy.deepcopy(snapshot)
    changed_geometry['geometry_wkb']['kilde/a'] = '01'
    assert snapshot_fingerprint(changed_geometry) != snapshot_fingerprint(snapshot)
    for key, value in [('selected_ids', ['ukjent']), ('version', True), ('version', 2), ('visible_ids', []), ('filters', {})]:
        damaged = {**archive, key: value}
        with pytest.raises(ValueError):
            load_analysis(json.dumps(damaged).encode())
    changed = copy.deepcopy(archive)
    changed['filters']['criteria']['sesong'] = ['Vinter']
    with pytest.raises(ValueError, match='filtre gir'):
        gjenskap_analyse(changed)
    changed = copy.deepcopy(archive)
    changed['statistics'][0]['Observasjoner'] = 999
    with pytest.raises(ValueError, match='artsstatistikk avviker'):
        gjenskap_analyse(changed)
    empty = make_analysis(snapshot, {**filters, 'criteria': {'sesong': ['Vinter']}}, [], [], [])
    assert gjenskap_analyse(empty)[2].is_empty()
