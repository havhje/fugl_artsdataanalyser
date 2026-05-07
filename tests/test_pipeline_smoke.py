from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from tests.helpers import FakeNorTaxa, run_databehandling_app

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


def test_full_pipeline_smoke_on_fixture_with_mocked_nortaxa() -> None:
    fake_fetch = FakeNorTaxa()
    pipeline = run_databehandling_app(fake_fetch)["les_data_og_kjør_alle_funksjoner"]

    result = pipeline(FIXTURE_CSV.as_posix(), filter_year=1990)

    assert isinstance(result, pl.DataFrame)
    assert expected_filtered_height(1990) == EXPECTED_FILTERED_HEIGHT_1990
    assert result.height == EXPECTED_FILTERED_HEIGHT_1990
    assert result.height == expected_filtered_height(1990)
    assert result.height > 0
    assert fake_fetch.calls

    required_columns = {
        "Artens ID",
        "Art",
        "Navn",
        "Kategori",
        "Verdi M1941",
        "latitude",
        "longitude",
    }
    assert required_columns <= set(result.columns)

    expected_dtypes = {
        "Artens ID": pl.Int64,
        "Art": pl.Utf8,
        "Navn": pl.Utf8,
        "Kategori": pl.Utf8,
        "Verdi M1941": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }
    for column, dtype in expected_dtypes.items():
        assert result.schema[column] == dtype

    for column in required_columns:
        assert result.get_column(column).drop_nulls().len() > 0, f"{column} har bare manglende verdier"
