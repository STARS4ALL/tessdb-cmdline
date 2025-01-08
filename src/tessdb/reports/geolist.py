# --------------------
# System wide imports
# -------------------

import os
import csv
import logging

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

HEADER = ("name", "longitude", "latitude")
FILENAME = "geolist.csv"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)
package = __name__.split(".")[0]


def locations(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT name, lomgitude, latitude
        FROM tess_v WHERE name like 'stars%' 
        AND longitude IS NOT NULL 
        ORDER BY CAST(SUBSTR(name,6) AS int) ASC
        """
    )
    return cursor.fetchall()


def exporter(iterable, dir_path):
    csv_path = os.path.join(dir_path, FILENAME)
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(HEADER)
        for item in iterable:
            writer.writerow(item)


def geolist(args):
    """
    Main entry point
    """
    output_base_dir = (
        decouple.config("IDA_BASE_DIR") if args.out_dir is None else args.out_dir
    )
    connection, db_path = open_database(args.dbase, env_var="TESSDB_URL")
    log.info("database opened on %s", db_path)
    geolist = locations(connection)
    exporter(geolist, output_base_dir)
    log.info(
        "Exported geographical distribution of TESS-W on to %s/%s",
        output_base_dir,
        FILENAME,
    )


# ===================================
# MAIN ENTRY POINT SPECIFIC ARGUMENTS
# ===================================


def add_args(parser):
    parser.add_argument(
        "-d", "--dbase", type=vfile, default=None, help="SQLite database full file path"
    )
    parser.add_argument(
        "-o",
        "--out_dir",
        type=vdir,
        default=None,
        help="Output directory to dump record",
    )


# ================
# MAIN ENTRY POINT
# ================


def main():
    execute(
        main_func=geolist,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="Export TESS data to monthly IDA files",
    )
