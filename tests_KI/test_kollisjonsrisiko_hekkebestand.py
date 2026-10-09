"""Checks for the collision lookup based on the 259-entry breeding roster."""

import csv
from collections import Counter
from pathlib import Path

import pytest

DATA = Path(__file__).resolve().parents[1] / "databehandling"


def les_csv(navn: str) -> list[dict[str, str]]:
    with (DATA / navn).open(encoding="utf-8-sig", newline="") as fil:
        return list(csv.DictReader(fil))


@pytest.fixture
def resultat() -> list[dict[str, str]]:
    return les_csv("kollisjonsrisiko-bernotat-hekkebestand.csv")


def test_bevarer_akkurat_artslisten_og_alle_inndata(resultat):
    kilde = les_csv("hekkebestand_nortaxa.csv")
    assert len(resultat) == len(kilde) == 259
    assert len({rad["Artens ID"] for rad in resultat}) == 259
    assert [{felt: rad[felt] for felt in kilde[0]} for rad in resultat] == kilde
    assert Counter(rad["Taksonomisk nivå"] for rad in resultat) == {
        "species": 252,
        "subspecies": 7,
    }


def test_grader_og_kildehenvisninger_er_konsistente(resultat):
    etiketter = {"1": "1 (sh)", "2": "2 (h)", "3": "3 (m)", "4": "4 (g)", "5": "5 (sg)"}
    kildefelt = (
        "de_grad_tekst",
        "de_kilderad",
        "de_navn_tysk",
        "de_navn_vitenskapelig",
        "de_side",
        "de_kildeavsnitt",
    )
    assert Counter(rad["de_grad_1_5"] for rad in resultat) == {
        "1": 22,
        "2": 62,
        "3": 24,
        "4": 17,
        "5": 113,
        "": 21,
    }
    for rad in resultat:
        grad = rad["de_grad_1_5"]
        if grad:
            assert rad["koblingstype"] != "ingen_treff"
            assert rad["de_grad_tekst"] == etiketter[grad]
            assert all(rad[felt] for felt in kildefelt)
            assert 64 <= int(rad["de_side"]) <= 79
            assert int(rad["de_kilderad"]) != 242
        else:
            assert rad["koblingstype"] == "ingen_treff"
            assert all(not rad[felt] for felt in kildefelt)
            assert "Manglende grad er ikke lav grad" in rad["merknad"]


@pytest.mark.parametrize(
    ("art", "kilderad", "grad", "koblingstype"),
    [
        ("Lagopus lagopus", "5", "1", "samlet_takson"),
        ("Lagopus muta", "5", "1", "samlet_takson"),
        ("Corvus corone", "169", "4", "samlet_takson"),
        ("Anser fabalis", "64", "2", "samlet_takson"),
        ("Anser serrirostris", "64", "2", "samlet_takson"),
        ("Saxicola rubicola", "234", "5", "avgrenset_takson"),
        ("Eudromias morinellus", "34", "2", "navnevariant"),
        ("Thinornis dubius", "45", "2", "navnevariant"),
        ("Sternula albifrons", "164", "4", "navnevariant"),
        ("Astur gentilis", "141", "5", "navnevariant"),
        ("Hydrobates leucorhous", "183", "5", "navnevariant"),
        ("Columba livia subsp. domestica", "336", "3", "avgrenset_takson"),
        ("Acanthis flammea subsp. cabaret", "292", "5", "overordnet_art"),
        ("Acanthis flammea subsp. exilipes", "292", "5", "overordnet_art"),
        ("Calidris alpina subsp. schinzii", "36", "2", "overordnet_art"),
        ("Larus fuscus subsp. fuscus", "109", "3", "overordnet_art"),
        ("Larus fuscus subsp. intermedius", "109", "3", "overordnet_art"),
        ("Limosa limosa subsp. islandica", "13", "1", "overordnet_art"),
        ("Turdus philomelos", "124", "3", "eksakt"),
    ],
)
def test_kontrollerte_koblinger(resultat, art, kilderad, grad, koblingstype):
    rad = next(rad for rad in resultat if rad["Art"] == art)
    assert (rad["de_kilderad"], rad["de_grad_1_5"], rad["koblingstype"]) == (
        kilderad,
        grad,
        koblingstype,
    )
    if koblingstype != "eksakt":
        assert rad["merknad"]


def test_ukjente_arter_faar_ikke_slektningsgrad(resultat):
    forventet = {
        "Tarsiger cyanurus",
        "Acrocephalus dumetorum",
        "Loxia leucoptera",
        "Emberiza pusilla",
        "Loxia pytyopsittacus",
        "Surnia ulula",
        "Hydrobates pelagicus",
        "Pagophila eburnea",
        "Pinicola enucleator",
        "Poecile cinctus",
        "Phylloscopus borealis",
        "Strix nebulosa",
        "Perisoreus infaustus",
        "Uria lomvia",
        "Larus hyperboreus",
        "Phalaropus fulicarius",
        "Somateria spectabilis",
        "Xema sabini",
        "Bubo scandiacus",
        "Gulosus aristotelis",
        "Emberiza rustica",
    }
    assert {rad["Art"] for rad in resultat if not rad["de_grad_1_5"]} == forventet
