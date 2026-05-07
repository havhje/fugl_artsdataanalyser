from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from databehandling.databehandling import (
    get_allowed_categories,
    get_required_artskart_columns,
    validate_artskart_input_contract,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CSV = ROOT / "data_csv_gammel" / "test_fugledata.csv"


def read_fixture() -> pl.DataFrame:
    return duckdb.sql(f"SELECT * FROM read_csv('{FIXTURE_CSV.as_posix()}')").pl()


def test_artskart_fixture_satisfies_required_input_contract() -> None:
    df = read_fixture()

    required_columns = get_required_artskart_columns()
    assert required_columns <= set(df.columns)
    validate_artskart_input_contract(df)


def test_artskart_contract_reports_missing_required_column() -> None:
    df = read_fixture()
    missing_column = "validScientificNameId"

    with pytest.raises(ValueError, match=missing_column):
        validate_artskart_input_contract(df.drop(missing_column))


def test_fixture_categories_are_allowed_domain_values() -> None:
    df = read_fixture()

    observed_categories = set(df.get_column("category").drop_nulls().unique().to_list())
    assert observed_categories <= get_allowed_categories()


def test_artskart_contract_reports_unknown_category_code() -> None:
    df = read_fixture().head(1).with_columns(pl.lit("BAD_CODE").alias("category"))

    with pytest.raises(ValueError, match="BAD_CODE"):
        validate_artskart_input_contract(df)
