# ----------------------------------------------------------------------
# Copyright (c) 2024 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import os

from argparse import ArgumentParser

# ---------------------------
# Third-party library imports
# ----------------------------

from lica.validators import vdir, vdate
from lica.asyncio.photometer import Model as PhotModel, Sensor

# --------------
# local imports
# -------------


def idir() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-i",
        "--input-dir",
        type=vdir,
        default=os.getcwd(),
        metavar="<Dir>",
        help="Input CSV directory (default %(default)s)",
    )
    return parser



def buf() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-b",
        "--buffer",
        type=int,
        default=None,
        help="Circular buffer size (default %(default)s)",
    )
    return parser


def info() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "--info",
        default=False,
        action="store_true",
        help="Query photometer info and exit (default %(default)s)",
    )
    return parser



def author() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-a",
        "--author",
        nargs="+",
        default=None,
        help="Calibration author (default %(default)s)",
    )
    return parser

