from __future__ import annotations

import polars as pl

from databehandling.databehandling import legg_til_verdi_m1941


def test_m1941_classification_precedence_and_domain_mapping() -> None:
    cases = [
        ("EN", "No", "No", "Svært stor verdi"),
        ("CR", "No", "No", "Svært stor verdi"),
        ("VU", "No", "No", "Stor verdi"),
        ("NT", "No", "No", "Middels verdi"),
        ("LC", "No", "No", "Noe verdi"),
        ("LC", "Yes", "No", "Svært stor verdi"),
        ("LC", "No", "Yes", "Stor verdi"),
        ("HI", "No", "No", "Ikke definert"),
        ("LO", "No", "No", "Ikke definert"),
        ("NA", "No", "No", "Ikke definert"),
        ("NE", "No", "No", "Ikke definert"),
        ("Unknown", "No", "No", "Ikke definert"),
    ]
    source_df = pl.DataFrame(
        {
            "category": [case[0] for case in cases],
            "Prioriterte arter": [case[1] for case in cases],
            "Andre spesielt hensynskrevende arter": [case[2] for case in cases],
            "expected": [case[3] for case in cases],
        }
    )

    result = legg_til_verdi_m1941(source_df)

    assert result.get_column("Verdi M1941").to_list() == result.get_column("expected").to_list()
