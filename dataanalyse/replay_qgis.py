"""Gjenskap artsstatistikk fra en lagret QGIS–marimo-analyse, uten QGIS."""

import argparse
from pathlib import Path
import tempfile

from dataanalyse.qgis_utvalg import gjenskap_analyse
from marimo_qgis.selection_session import load_analysis


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('analyse', type=Path)
    parser.add_argument('--ut', type=Path, required=True, help='Parquet-fil for gjenskapt artsstatistikk')
    args = parser.parse_args()
    if args.analyse.resolve() == args.ut.resolve():
        parser.error('Utfilen må være forskjellig fra analysefilen.')
    arkiv = load_analysis(args.analyse.read_bytes())
    _, valgt, statistikk = gjenskap_analyse(arkiv)
    with tempfile.TemporaryDirectory(dir=args.ut.resolve().parent) as mappe:
        fil = Path(mappe) / 'statistikk.parquet'
        statistikk.write_parquet(fil)
        fil.replace(args.ut)
    print(f'Gjenskapt Arter (inkl. underarter): {statistikk.height} fra {valgt.height} observasjoner: {args.ut}')


if __name__ == '__main__':
    main()
