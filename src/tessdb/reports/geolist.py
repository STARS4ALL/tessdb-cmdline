# --------------------
# System wide imports
# -------------------

import os
import csv
import logging
from typing import Sequence
from argparse import Namespace, ArgumentParser
from sqlite3 import Connection
# --------------
# other imports
# -------------

import decouple

from lica.cli import execute
from lica.sqlite import open_database
from lica.validators import vfile, vdir


# --------------
# local imports
# -------------

from .._version import __version__

# ----------------
# Module constants
# ----------------

HEADER = ("name", "model", "longitude", "latitude")
FILENAME = "geolist.csv"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__.split(".")[-1])


def locations(connection: Connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT name, model, longitude, latitude
        FROM tess_v WHERE name like 'stars%' 
        AND longitude IS NOT NULL 
        ORDER BY CAST(SUBSTR(name,6) AS int) ASC
        """
    )
    return cursor.fetchall()


def exporter(sequence: Sequence, dir_path: str) -> None:
    csv_path = os.path.join(dir_path, FILENAME)
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(HEADER)
        for item in sequence:
            writer.writerow(item)


def geolist(args: Namespace) -> None:
    """
    Main entry point
    """
    output_base_dir = decouple.config("IDA_BASE_DIR") if args.out_dir is None else args.out_dir
    connection, db_path = open_database(args.dbase, env_var="TESSDB_URL")
    log.info("database opened on %s", db_path)
    geolist = locations(connection)
    log.info("Got %d photometers with known coordinates", len(geolist))
    exporter(geolist, output_base_dir)
    log.info(
        "Exported geographical distribution of TESS-W on to %s/%s",
        output_base_dir,
        FILENAME,
    )


# ===================================
# MAIN ENTRY POINT SPECIFIC ARGUMENTS
# ===================================


def add_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "-d", "--dbase", type=vfile, default=None, help="SQLite database full file path"
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        type=vdir,
        default=None,
        help="Output directory to dump record",
    )


# ================
# MAIN ENTRY POINT
# ================


def main() -> None:
    execute(
        main_func=geolist,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="Export TESS network geographical data",
    )
