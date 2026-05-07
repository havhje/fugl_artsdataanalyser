from __future__ import annotations

import io
from collections.abc import Callable
from typing import Any

from rich.console import Console

from databehandling.databehandling import app

FAKE_DESIRED_RANKS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus"]
FAMILY_OFFSET = 10_000_000
ORDER_OFFSET = 20_000_000


class FakeNorTaxa:
    """Deterministic NorTaxa API fake used by pytest tests."""

    def __init__(self, fail_ids: set[int] | None = None, empty_ids: set[int] | None = None) -> None:
        self.fail_ids = fail_ids or set()
        self.empty_ids = empty_ids or set()
        self.calls: list[int] = []

    def __call__(self, taxon_id: int) -> dict[str, Any] | None:
        taxon_id = int(taxon_id)
        self.calls.append(taxon_id)

        if taxon_id in self.fail_ids:
            return None
        if taxon_id in self.empty_ids:
            return {}

        if taxon_id >= ORDER_OFFSET:
            species_id = taxon_id - ORDER_OFFSET
            return {"vernacularNames": [{"languageIsoCode": "nb", "vernacularName": f"orden {species_id}"}]}

        if taxon_id >= FAMILY_OFFSET:
            species_id = taxon_id - FAMILY_OFFSET
            return {"vernacularNames": [{"languageIsoCode": "nb", "vernacularName": f"familie {species_id}"}]}

        return {
            "higherClassification": [
                {"taxonRank": "Kingdom", "scientificName": "Animalia", "scientificNameId": 1},
                {"taxonRank": "Phylum", "scientificName": "Chordata", "scientificNameId": 2},
                {"taxonRank": "Class", "scientificName": "Aves", "scientificNameId": 3},
                {
                    "taxonRank": "Order",
                    "scientificName": "MockOrder",
                    "scientificNameId": ORDER_OFFSET + taxon_id,
                },
                {
                    "taxonRank": "Family",
                    "scientificName": "MockFamily",
                    "scientificNameId": FAMILY_OFFSET + taxon_id,
                },
                {"taxonRank": "Genus", "scientificName": "MockGenus", "scientificNameId": 4},
            ]
        }


def fake_extract_hierarchy_and_ids(api_data: dict[str, Any] | None) -> tuple[dict[str, str | None], int | None, int | None]:
    hierarchy: dict[str, str | None] = {}
    family_id = order_id = None

    if api_data and "higherClassification" in api_data:
        for level in api_data["higherClassification"]:
            rank = level.get("taxonRank")
            if rank in FAKE_DESIRED_RANKS:
                name = level.get("scientificName")
                hierarchy[rank] = name.strip() if name else None
            if rank == "Family":
                family_id = level.get("scientificNameId")
            elif rank == "Order":
                order_id = level.get("scientificNameId")

    return hierarchy, family_id, order_id


def fake_get_norwegian_name(api_data: dict[str, Any] | None) -> str | None:
    if not api_data or "vernacularNames" not in api_data:
        return None

    names = api_data["vernacularNames"]
    for name in names:
        if name.get("languageIsoCode") == "nb":
            return name.get("vernacularName")
    for name in names:
        if name.get("languageIsoCode") == "nn":
            return name.get("vernacularName")
    return None


def run_databehandling_app(fetch_taxon_data: Callable[[int], dict[str, Any] | None] | None = None) -> dict[str, Any]:
    fake_fetch = fetch_taxon_data or FakeNorTaxa()
    _, definitions = app.run(
        defs={
            "DESIRED_RANKS": FAKE_DESIRED_RANKS,
            "NORTAXA_API_BASE_URL": "mock://nortaxa",
            "RATE_LIMIT_DELAY": 0.0,
            "fetch_taxon_data": fake_fetch,
            "extract_hierarchy_and_ids": fake_extract_hierarchy_and_ids,
            "get_norwegian_name": fake_get_norwegian_name,
            "cli_app": None,
            "les_data_cli": None,
            "console": Console(file=io.StringIO(), force_terminal=False),
        }
    )
    return dict(definitions)
