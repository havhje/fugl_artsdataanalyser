from __future__ import annotations

import polars as pl

from databehandling.databehandling import legg_til_verdi_m1941
from tests_KI.helpers import FakeNorTaxa, run_databehandling_app


def test_legg_til_verdi_m1941_maps_remaining_allowed_categories_to_ingen() -> None:
    source_df = pl.DataFrame(
        {
            "category": ["RE", "DD", "SE", "HI", "PH", "LO", "NK", "NA", "NE", "Unknown", None],
        }
    )

    result = legg_til_verdi_m1941(source_df)

    assert result.get_column("verdi_rodliste_artskart").to_list() == [
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        "Ingen",
        None,
    ]


def test_m1941_value_is_read_from_national_interest_lookup() -> None:
    """M1941-verdi fra kriterietabellen brukes for arter som finnes i ANF."""
    definitions = run_databehandling_app(FakeNorTaxa())
    legg_til_verdi_m1941 = definitions["legg_til_verdi_m1941"]
    add_national_interest_criteria = definitions["add_national_interest_criteria"]
    source_df = pl.DataFrame(
        {
            "validScientificNameId": [3506, 3478, 3495, 999999],
            "validScientificName": [
                "Clangula hyemalis",
                "Anser erythropus",
                "Branta canadensis",
                "Nonexistent species",
            ],
            "category": ["LC", "LC", "HI", None],
        }
    )

    result = source_df.pipe(legg_til_verdi_m1941).pipe(add_national_interest_criteria)

    assert result.get_column("Verdi M1941").to_list() == [
        "Stor verdi",
        "Svært stor verdi",
        "-",
        None,
    ]


def test_m1941_national_value_overrides_red_list_value() -> None:
    """ANF-verdi skal overstyre rødlisteverdi, med rødliste som fallback."""
    definitions = run_databehandling_app(FakeNorTaxa())
    legg_til_verdi_m1941 = definitions["legg_til_verdi_m1941"]
    add_national_interest_criteria = definitions["add_national_interest_criteria"]
    source_df = pl.DataFrame(
        {
            "validScientificNameId": [3506, 999999],
            "validScientificName": ["Clangula hyemalis", "Nonexistent species"],
            "category": ["LC", "EN"],
        }
    )

    result = source_df.pipe(legg_til_verdi_m1941).pipe(add_national_interest_criteria)

    assert result.get_column("verdi_rodliste_artskart").to_list() == ["Noe verdi", "Svært stor verdi"]
    assert result.get_column("verdi_m1941_nasjonal").to_list() == ["Stor verdi", None]
    assert result.get_column("Verdi M1941").to_list() == ["Stor verdi", "Svært stor verdi"]
