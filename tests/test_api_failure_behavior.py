from __future__ import annotations

import polars as pl
import pytest

from tests.helpers import FAKE_DESIRED_RANKS, FakeNorTaxa, run_databehandling_app


def test_one_api_call_failure_raises_runtime_error_with_id() -> None:
    fake_fetch = FakeNorTaxa(fail_ids={102})
    process_and_enrich_data = run_databehandling_app(fake_fetch)["process_and_enrich_data"]

    with pytest.raises(RuntimeError, match="102"):
        process_and_enrich_data(pl.DataFrame({"validScientificNameId": [101, 102]}))


def test_all_api_calls_fail_raise_clear_runtime_error() -> None:
    fake_fetch = FakeNorTaxa(fail_ids={101, 102})
    process_and_enrich_data = run_databehandling_app(fake_fetch)["process_and_enrich_data"]

    with pytest.raises(RuntimeError) as exc_info:
        process_and_enrich_data(pl.DataFrame({"validScientificNameId": [101, 102]}))

    message = str(exc_info.value)
    assert "2 ID" in message
    assert "101" in message
    assert "102" in message


def test_invalid_and_null_species_ids_are_not_fetched_and_rows_are_preserved() -> None:
    fake_fetch = FakeNorTaxa()
    process_and_enrich_data = run_databehandling_app(fake_fetch)["process_and_enrich_data"]
    source_df = pl.DataFrame({"validScientificNameId": [101, None], "row_label": ["gyldig", "mangler"]})

    result = process_and_enrich_data(source_df)

    assert result.height == source_df.height
    assert None not in fake_fetch.calls
    assert 101 in fake_fetch.calls

    null_id_row = result.filter(pl.col("validScientificNameId").is_null())
    assert null_id_row.height == 1
    for column in [*FAKE_DESIRED_RANKS, "FamilieNavn", "OrdenNavn"]:
        assert null_id_row.get_column(column).to_list() == [None]


def test_empty_api_result_raises_runtime_error_not_column_not_found() -> None:
    fake_fetch = FakeNorTaxa(empty_ids={101})
    process_and_enrich_data = run_databehandling_app(fake_fetch)["process_and_enrich_data"]

    with pytest.raises(RuntimeError, match="101"):
        process_and_enrich_data(pl.DataFrame({"validScientificNameId": [101]}))


def test_all_invalid_species_ids_raise_value_error() -> None:
    process_and_enrich_data = run_databehandling_app(FakeNorTaxa())["process_and_enrich_data"]
    source_df = pl.DataFrame(
        {"validScientificNameId": ["ikke-en-id", None]},
        schema={"validScientificNameId": pl.Utf8},
    )

    with pytest.raises(ValueError, match="Ingen gyldige validScientificNameId"):
        process_and_enrich_data(source_df)
