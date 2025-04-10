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

from lica.validators import vdir


# --------------
# local imports
# -------------

from ..constants import ObserverType


def name(obs_type: str) -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-n",
        "--name",
        nargs="+",
        required=True,
        help=f"{obs_type} " + "(default %(default)s)",
    )
    return parser


def optname(obs_type: str) -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-n",
        "--name",
        nargs="+",
        default=None,
        help=f"{obs_type} " + "(default %(default)s)",
    )
    return parser


def aff() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-a",
        "--affiliation",
        nargs="+",
        default=None,
        help=f"{ObserverType.PERSON} affiliation" + "(default %(default)s)",
    )
    return parser


def nym() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-y",
        "--acronym",
        type=str,
        default=None,
        help="Acronym (default %(default)s)",
    )
    return parser


def web() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-w",
        "--website-url",
        type=str,
        default=None,
        help="Website URL (default %(default)s)",
    )
    return parser


def email() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-e",
        "--email",
        type=str,
        default=None,
        help="Email (default %(default)s)",
    )
    return parser


def history() -> ArgumentParser:
    parser = ArgumentParser(add_help=False)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-a", "--all", action="store_true", help="Delete all observer history")
    group.add_argument("-c", "--current", action="store_true", help="Delete current observer")
    return parser
