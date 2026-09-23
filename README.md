# Fugl Artsdataanalyser

`databehandling/databehandling.py` lager behandlet Parquet. `dataanalyse/` er
separat analyse/presentasjon. GIS-flyten starter etter databehandlingen og
bruker eksisterende Fuglefilter i `~/qgis`.

- [Begreper](CONTEXT.md)
- [Parquet → QGIS og ArcGIS: dataflyt, kodeeierskap og beslutninger](docs/qgis-pipeline-plan.md)

## GIS-inngang

Velg **Utvidelser → Fuglefilter → Importer behandlet Parquet …** i et lagret
QGIS-prosjekt. Kanonisk CLI er
`~/qgis/qgis-naturmangfold/qt6/environment/scripts/prepare_bird_data.py`
med `--input`, `--output` og `--crs`. Begge innganger lager GeoPackage og
ArcGIS GeoParquet; Polygon/MultiPolygon og én geometrirad per observasjon
bevares i ArcGIS-filene.

`databehandling/arcgis_geoparquet.py` er **avviklet**, ikke en støttet inngang.
Testgarantiene for den avviklede inngangen ligger nå i GIS-repoets
`scripts/test_prepare_bird_data.py`.
Gjeldende driftsbeskrivelse finnes i
`~/qgis/qgis-naturmangfold/qt6/environment/references/bird-data-workflow.md`;
planen ovenfor inneholder også historiske beslutninger.

## Beregningskontrakt

- `Observert dato` skal være kalenderdato (`Date`), eventuelt null.
  Tidsstempler avvises, også midnatt og tidsstempler med tidssone; de trunkeres
  ikke. Parquet-datostrenger er heller ikke Date. Ren `YYYY-MM-DD` i
  analyseadapterens JSON-grensesnitt er datotransport, ikke timestamp-støtte.
- `Antall` skal være et ikke-negativt heltall, inkludert **0**. Både hver verdi
  og eksakt totalsum må ligge i `0`–`2**63 - 1`. Null, flyttall, boolske verdier
  og numerisk tekst stopper rapporten før cast; ingen rader repareres eller
  fjernes stilltiende. Oppstrøms imputering i `databehandling.py` er uendret.
- Grupperingsnøkkelen er eksakt **(`Artens ID`, `Art`)**, der `Art` er
  vitenskapelig navn. ID må være heltall og utfylt; null/tomt/blankt navn gir
  rapportfeil, også på udaterte rader. Navnepar normaliseres ikke, og synonymer
  eller underarter slås ikke automatisk sammen. Norsk navn er visningstekst.
- **Arter (inkl. underarter)** er visningsetiketten. Presiseringen er:
  «Tellingen følger kildens ID og navn; også høyere taksonomiske nivåer og
  navnevarianter kan inngå.» Tallet er ikke et rent mål på artsrikdom.
- Gjennomsnitt er sum av behandlede individtall delt på **alle inkluderte
  observasjonsrader**, også antall 0 og udaterte rader. Summer er ikke
  nødvendigvis forskjellige individer. Udaterte inngår ikke i tidsstatistikk;
  et eksplisitt datointervall utelukker dem også fra selve utvalget.
- Ved månedsprofilen vises datodekning som **stripe og prosent** av
  observasjonsradene. En helt udatert gruppe viser bare
  **«Ingen daterte observasjoner»**, ikke tolv nullsøyler eller ekstra antall.
- Den delte tabellformatereren eier HTML-escaping; kallere sender rå tekst.
  Uklassifisert M1941 vises med hvitt, stiplet omriss, ikke den vurderte grå
  kategorien «Uten betydning for KU».

Valideringen gjelder de inkluderte radene etter eksplisitte filtre og utvalg.
Fuglefilter bruker analysefunksjonene i en **separat worker** i dette prosjektets
miljø. QGIS importerer ikke analysemodulet eller dets Polars-/Marimo-/GT-miljø,
og starter ingen Marimo-server. Kildeendring krever ny åpning av Fuglefilter;
rapportens avgrensning, PNG-kontrakt og manuelle etterprøvbarhet er beskrevet i
GIS-arbeidsflyten. Ingen automatisk provenienssidefil eller nytt analysearkiv bygges.

## Kontroll

I eksisterende miljø, uten installasjon:

```sh
PYTHONDONTWRITEBYTECODE=1 UV_OFFLINE=1 UV_NO_SYNC=1 \
  PYTHONPATH="$PWD:$HOME/qgis/qgis-naturmangfold/qt6/plugins" \
  .venv/bin/python -B -m pytest -q -p no:cacheprovider tests_KI/test_qgis_analyse.py
```

Testen inkluderer de tolv innebygde notebooktestene og uavhengige tallfasiter.
Automatiserte fil-/helpertester er ikke en interaktiv Marimo-UI-test, faktisk
ArcGIS-åpning, Qt5-verifikasjon eller Word-innsetting. Disse er ikke utført som
del av rettingskontrollene; teknisk kontroll er heller ikke publiseringsgodkjenning.
