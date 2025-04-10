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
from typing import Optional

# -------------------
# Third party imports
# -------------------

from lica.cli import execute
from lica.sqlite import open_database
from lica.tabulate import paging

# --------------
# local imports
# -------------

from . import __version__
from .utils import utcnow, parser as prs
from .constants import ObserverType, DEFAULT_END_DATE, ValidState

# ----------------
# Global variables
# ----------------

HEADERS = (
    "Id.",
    "Name",
    "Type",
    "State",
    "Since",
    "Until",
    "Affil.",
    "Acronym",
    "Email",
    "Website",
)

log = logging.getLogger(__name__)

# ---------------------
# API Library functions
# ---------------------


def observer_create(
    connection,
    obs_type: ObserverType,
    name: str,
    affiliation: str,
    acronym: str,
    website_url: str,
    email: str,
):
    now = utcnow()
    cursor = connection.cursor()
    params = {
        "name": name,
        "type": obs_type,
        "state": ValidState.CURRENT,
    }
    cursor.execute(
        """
        SELECT observer_id FROM observer_t 
        WHERE type = :type AND name = :name AND valid_state = :state
        """,
        params,
    )
    result = cursor.fetchone()
    if result is not None and len(result) > 0:
        log.info("Previous record found for %s => %s", obs_type, name)
        params = {"id": result[0], "state": ValidState.EXPIRED, "until": now}
        cursor.execute(
            """
            UPDATE observer_t
            SET valid_state = :state, 
                valid_until = :until
            WHERE observer_id = :id
            """,
            params,
        )
        log.info("Expired latest record for %s => %s", obs_type, name)
    params = {
        "name": name,
        "type": obs_type,
        "affiliation": affiliation,
        "acronym": acronym,
        "website_url": website_url,
        "email": email,
        "since": now,
        "until": DEFAULT_END_DATE,
        "state": ValidState.CURRENT,
    }
    result = cursor.execute(
        """
        INSERT INTO observer_t(type,name,affiliation,acronym,website_url,email,valid_since,valid_until,valid_state)
        VALUES(:type, :name, :affiliation, :acronym, :website_url, :email, :since, :until, :state)
        RETURNING observer_id
        
        """,
        params,
    )
    new_id = cursor.fetchone()
    if new_id is not None:
        new_id = new_id[0]
        log.info("Created new observer %s => %s, whose id. is = %d", obs_type, name, new_id)
    connection.commit()


def observer_list(connection, name: Optional[str]):
    cursor = connection.cursor()
    sql = """
            SELECT observer_id, name, type, valid_state, valid_since, valid_until, affiliation, acronym, email, website_url
            FROM observer_t
        """
    if name is not None:
        sql += " WHERE name = :name"
        params = {"name": name}
    else:
        params = {}
    cursor.execute(sql, params)
    paging(cursor, HEADERS)


def observer_delete(connection, obs_type: ObserverType, name: str, all_flag: bool):
    cursor = connection.cursor()
    params = {"type": obs_type, "name": name}
    if all_flag:
        log.info("Deleting all entries for %s => %s", obs_type, name)
        sql = "DELETE FROM observer_t WHERE type = :type AND name = :name"
        cursor.execute(sql, params)
    else:
        log.info("Deleting last entry for %s => %s", obs_type, name)
        params["state"] = ValidState.CURRENT
        params["until"] = DEFAULT_END_DATE
        sql = "DELETE FROM observer_t WHERE type = :type AND name = :name AND valid_state = :state"
        cursor.execute(sql, params)
        sql = """
            UPDATE observer_t SET valid_state = :state, valid_until = :until
            WHERE type = :type AND name = :name AND 
            valid_until = (SELECT MAX(valid_until) FROM observer_t WHERE type = :type AND name = :name)
        """
        cursor.execute(sql, params)
    log.info("Done")
    connection.commit()


# ------------------
# CLI Work functions
# ------------------


def cli_observer_create_person(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    observer_create(
        connection,
        ObserverType.PERSON,
        " ".join(args.name) if args.name is not None else None,
        " ".join(args.affiliation) if args.affiliation is not None else None,
        args.acronym,
        args.website_url,
        args.email,
    )


def cli_observer_create_organization(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    observer_create(
        connection,
        ObserverType.ORGANIZATION,
        " ".join(args.name) if args.name is not None else None,
        None,
        args.acronym,
        args.website_url,
        args.email,
    )


def cli_observer_list(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    observer_list(connection, " ".join(args.name) if args.name is not None else None)


def cli_observer_delete_person(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    observer_delete(
        connection,
        ObserverType.PERSON,
        " ".join(args.name) if args.name is not None else None,
        True if args.all else False,
    )


def cli_observer_delete_organization(args: Namespace) -> None:
    connection, url = open_database(env_var="DATABASE_URL")
    log.info("Opening database: %s", url)
    observer_delete(
        connection,
        ObserverType.ORGANIZATION,
        " ".join(args.name) if args.name is not None else None,
        True if args.all else False,
    )


# ======
# PARSER
# ======


def add_args(parser: ArgumentParser):
    subparser = parser.add_subparsers(dest="command")

    # ----------------
    # CREATE OBSERVERS
    # ----------------
    parser_create = subparser.add_parser("create", help="Create new observer")
    subsubparser = parser_create.add_subparsers(dest="subcommand")
    parser_person = subsubparser.add_parser(
        "person",
        parents=[prs.name(ObserverType.PERSON), prs.aff(), prs.nym(), prs.web(), prs.email()],
        help="Create new observer [person]",
    )
    parser_person.set_defaults(func=cli_observer_create_person)

    parser_organization = subsubparser.add_parser(
        "organization",
        parents=[prs.name(ObserverType.ORGANIZATION), prs.nym(), prs.web(), prs.email()],
        help="Create new observer [organization]",
    )
    parser_organization.set_defaults(func=cli_observer_create_organization)

    # --------------
    # LIST OBSERVERS
    # --------------
    parser_list = subparser.add_parser(
        "list",
        parents=[prs.optname(f"{ObserverType.PERSON} or {ObserverType.ORGANIZATION} name")],
        help="List observers",
    )
    parser_list.set_defaults(func=cli_observer_list)

    # ----------------
    # DELETE OBSERVERS
    # ----------------
    parser_delete = subparser.add_parser(
        "delete",
        help="Delete observer",
    )
    subsubparser = parser_delete.add_subparsers(dest="subcommand")
    parser_person = subsubparser.add_parser(
        "person",
        parents=[prs.name(ObserverType.PERSON), prs.history()],
        help="Delete observer [person]",
    )
    parser_person.set_defaults(func=cli_observer_delete_person)
    parser_organization = subsubparser.add_parser(
        "organization",
        parents=[prs.name(ObserverType.ORGANIZATION), prs.history()],
        help="Delete observer [organization]",
    )
    parser_organization.set_defaults(func=cli_observer_delete_organization)


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
