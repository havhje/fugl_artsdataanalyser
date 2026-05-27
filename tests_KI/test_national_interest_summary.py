from __future__ import annotations

import polars as pl

from databehandling.databehandling import legg_til_kolonne_arteravnasjonal

CATEGORY_COLUMNS = [
    "Ansvarsarter",
    "Andre spesielt hensynskrevende arter",
    "Hensynskrevende arter",
    "Spesielle økologiske former",
    "Datamangel",
    "Prioriterte arter",
    "Fredete arter",
    "Fremmede arter",
]


def test_national_interest_summary_preserves_missing_lookup_marker() -> None:
    source_df = pl.DataFrame(
        {
            "species": ["ingen treff", "ingen kriterier", "positive kriterier"],
            **{column: ["Treff ikke funnet", "Nei", "Nei"] for column in CATEGORY_COLUMNS},
        }
    ).with_columns(
        pl.Series("Ansvarsarter", ["Treff ikke funnet", "Nei", "Ja"]),
        pl.Series("Fremmede arter", ["Treff ikke funnet", "Nei", "Ja"]),
    )

    result = legg_til_kolonne_arteravnasjonal(source_df)

    assert result.get_column("Art av nasjonal forvaltningsinteresse").to_list() == [
        "Treff ikke funnet",
        "Nei",
        "Ansvarsarter, Fremmede arter",
    ]
