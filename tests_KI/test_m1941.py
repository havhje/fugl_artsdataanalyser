from __future__ import annotations

import polars as pl

from tests_KI.helpers import FakeNorTaxa, run_databehandling_app


def test_m1941_value_is_read_from_national_interest_lookup() -> None:
    """M1941-verdi kommer nå fra kriterietabellen, ikke fra egen regel-funksjon."""
    add_national_interest_criteria = run_databehandling_app(FakeNorTaxa())["add_national_interest_criteria"]
    source_df = pl.DataFrame(
        {
            "validScientificNameId": [3506, 3478, 3495, 999999],
            "validScientificName": [
                "Clangula hyemalis",
                "Anser erythropus",
                "Branta canadensis",
                "Nonexistent species",
            ],
        }
    )

    result = add_national_interest_criteria(source_df)

    assert result.get_column("Verdi M1941").to_list() == [
        "Stor verdi",
        "Svært stor verdi",
        "-",
        None,
    ]
