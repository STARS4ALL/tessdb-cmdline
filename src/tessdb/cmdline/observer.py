# ----------------------------------------------------------------------
# Copyright (c) 2024 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import logging
from argparse import Namespace, ArgumentParser

# -------------------
# Third party imports
# -------------------

from lica.cli import execute
from lica.sqlite import open_database

# --------------
# local imports
# -------------

from . import __version__
from .utils import parser as prs

# ----------------
# Global variables
# ----------------


log = logging.getLogger(__name__)

# ------------------
# CLI Work functions
# ------------------

def cli_observer_create_person(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)

def cli_observer_create_organization(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    

def cli_observer_list(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    

def cli_observer_delete(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    

# ======
# PARSER
# ======

def add_args(parser: ArgumentParser):
    subparser = parser.add_subparsers(dest="command")
    parser_create = subparser.add_parser(
        "create", help="Create new observer"
    )
    subsubparser = parser_create.add_subparsers(dest="subcommand")
    parser_person = subsubparser.add_parser(
        "person", parents=[], help="Create new observer [person]"
    )
    parser_person.set_defaults(func=cli_observer_create_person)

    parser_organization = subsubparser.add_parser(
        "organization", parents=[], help="Create new observer [organization]"
    )
    parser_organization.set_defaults(func=cli_observer_create_organization)

    parser_list = subparser.add_parser(
        "list", parents=[], help="List observers"
    )
    parser_list.set_defaults(func=cli_observer_list)

    parser_delete = subparser.add_parser(
        "delete",
        parents=[],
        help="Delete observer",
    )
    parser_delete.set_defaults(func=cli_observer_delete)

  


def cli_main(args: Namespace) -> None:
    args.func(args)


def main():
    """The main entry point specified by pyproject.toml"""
    execute(
        main_func=cli_main,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="TESSDB Observers management",
    )


if __name__ == "__main__":
    main()
