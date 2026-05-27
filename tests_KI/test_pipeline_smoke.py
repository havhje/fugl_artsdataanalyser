from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from databehandling.databehandling import validate_artskart_input_contract
from tests_KI.helpers import FakeNorTaxa, run_databehandling_app

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CSV = ROOT / "data_csv_gammel" / "test_fugledata.csv"
EXPECTED_FILTERED_HEIGHT_1990 = 16_534


def expected_filtered_height(filter_year: int) -> int:
    raw_df = duckdb.sql(f"SELECT * FROM read_csv('{FIXTURE_CSV.as_posix()}')").pl()
    return (
        raw_df.with_columns(pl.col("dateTimeCollected").dt.date())
        .filter(pl.col("dateTimeCollected") >= date(filter_year, 1, 1))
        .height
    )


def test_pipeline_components_smoke_on_fixture_with_mocked_nortaxa_without_m1941_step() -> None:
    """Kjør nåværende berikingsløp uten den slettede legg_til_verdi_m1941-funksjonen."""
    fake_fetch = FakeNorTaxa()
    definitions = run_databehandling_app(fake_fetch)

    input_df = duckdb.sql(f"SELECT * FROM read_csv('{FIXTURE_CSV.as_posix()}')").pl()
    validate_artskart_input_contract(input_df)
    filtered_df = input_df.with_columns(pl.col("dateTimeCollected").dt.date()).filter(
        pl.col("dateTimeCollected") >= date(1990, 1, 1)
    )

    enriched_df = definitions["process_and_enrich_data"](filtered_df)
    criteria_df = definitions["add_national_interest_criteria"](enriched_df)
    result = definitions["legg_til_kolonne_arteravnasjonal"](criteria_df)

    assert isinstance(result, pl.DataFrame)
    assert expected_filtered_height(1990) == EXPECTED_FILTERED_HEIGHT_1990
    assert result.height == EXPECTED_FILTERED_HEIGHT_1990
    assert result.height == expected_filtered_height(1990)
    assert result.height > 0
    assert fake_fetch.calls

    required_columns = {
        "validScientificNameId",
        "validScientificName",
        "preferredPopularName",
        "category",
        "Verdi M1941",
        "Art av nasjonal forvaltningsinteresse",
        "latitude",
        "longitude",
    }
    assert required_columns <= set(result.columns)

    expected_dtypes = {
        "validScientificNameId": pl.Int64,
        "validScientificName": pl.Utf8,
        "preferredPopularName": pl.Utf8,
        "category": pl.Utf8,
        "Verdi M1941": pl.Utf8,
        "Art av nasjonal forvaltningsinteresse": pl.Utf8,
    }
    for column, dtype in expected_dtypes.items():
        assert result.schema[column] == dtype

    for column in required_columns:
        assert result.get_column(column).drop_nulls().len() > 0, f"{column} har bare manglende verdier"
