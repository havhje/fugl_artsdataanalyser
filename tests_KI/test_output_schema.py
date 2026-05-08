from __future__ import annotations

from datetime import datetime

import polars as pl

from databehandling.databehandling import rydd_navn_og_datatyper

EXPECTED_OUTPUT_COLUMNS = [
    "Verdi M1941",
    "Kategori",
    "Art av nasjonal forvaltningsinteresse",
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


def test_rydd_navn_og_datatyper_output_schema_and_dtypes() -> None:
    source_df = pl.DataFrame(
        {
            "Verdi M1941": ["Noe verdi"],
            "category": ["LC"],
            "Art av nasjonal forvaltningsinteresse": ["Nei"],
            "preferredPopularName": ["dompap"],
            "validScientificName": ["Pyrrhula pyrrhula"],
            "individualCount": ["2"],
            "behavior": ["syngende"],
            "dateTimeCollected": [datetime(2024, 5, 1, 12, 0, 0)],
            "coordinateUncertaintyInMeters": [25],
            "FamilieNavn": ["meisefamilien"],
            "OrdenNavn": ["spurvefugler"],
            "taxonGroupName": ["Fugler"],
            "collector": ["Ola Nordmann"],
            "locality": ["Testlokalitet"],
            "municipality": ["Øksnes"],
            "county": ["Nordland"],
            "scientificNameRank": ["species"],
            "Ansvarsarter": ["No"],
            "Andre spesielt hensynskrevende arter": ["No"],
            "Spesielle okologiske former": ["No"],
            "Prioriterte arter": ["No"],
            "Fredete arter": ["No"],
            "Fremmede arter": ["No"],
            "latitude": ["68,904168"],
            "longitude": ["15.066918"],
            "geometry": ["POINT (502688 7643678)"],
            "validScientificNameId": [4263],
        }
    )

    result = rydd_navn_og_datatyper(source_df)

    assert result.columns == EXPECTED_OUTPUT_COLUMNS
    assert result.schema["Antall"] == pl.Int64
    assert result.schema["Observert dato"] == pl.Date
    assert result.schema["Usikkerhet meter"] == pl.Int64
    assert result.schema["latitude"] == pl.Float64
    assert result.schema["longitude"] == pl.Float64
    assert result.schema["Artens ID"].is_integer()
