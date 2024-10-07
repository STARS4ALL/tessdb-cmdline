# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import os
import csv
import logging
import functools

# -------------------
# Third party imports
# -------------------

from lica.cli import execute
from lica.validators import vfile, vdir
from lica.sqlite import open_database
from lica.jinja2 import render_from

# --------------
# local imports
# -------------

from .._version import __version__


from .dbutils import get_tessdb_connection_string, get_zptess_connection_string, group_by_mac, common_A_B_items, in_A_not_in_B

# ----------------
# Module constants
# ----------------

SQL_ABSURD_ZP_TEMPLATE = 'sql-fix-absurd-zp.j2'

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)
package = __name__.split('.')[0]
render = functools.partial(render_from, package)

# -------------------------
# Module auxiliar functions
# -------------------------


def _easy_wrong_zp_photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT mac_address, zp1
        FROM tess_t
        WHERE zp1 < 10 and valid_state = 'Current'
        GROUP BY mac_address
        HAVING count(mac_address) = 1
        ORDER BY mac_address
        ''')
    result = [dict(zip(['mac', 'zero_point'], row)) for row in cursor]
    return result


def _zp_photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT mac, zero_point
        FROM summary_v
        WHERE zero_point IS NOT NULL
        ORDER BY mac, session desc -- get the latest session first
        ''')
    result = [dict(zip(['mac', 'zero_point'], row)) for row in cursor]
    return result


def _names_from_mac(connection, mac):
    params = {'mac_address': mac}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address = :mac_address
        ''', params)
    result = [dict(zip(['name', 'valid_since',
                        'valid_until', 'valid_state'], row)) for row in cursor]
    return result

# ===================
# Module entry points
# ===================


def easy(options):
    log.info(
        " ====================== FIXING WRONG ZP FOR EASY PHOTOMETERS ======================")
    tessdb = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", tessdb)
    conn_tessdb, _ = open_database(tessdb)

    zptess = get_zptess_connection_string()
    log.info("connecting to SQLite database %s", zptess)
    conn_zptess, _ = open_database(zptess)

    tessdb_input_list = _easy_wrong_zp_photometers_from_tessdb(conn_tessdb)
    tessdb_dict = group_by_mac(tessdb_input_list)

    zptess_input_list = _zp_photometers_from_tessdb(conn_zptess)
    zptess_dict = group_by_mac(zptess_input_list)

    common_mac_keys = common_A_B_items(tessdb_dict, zptess_dict)
    log.info(len(common_mac_keys))

    items = list()
    for mac in sorted(common_mac_keys):
        item = {}
        item['mac'] = mac
        item['new_zp'] = zptess_dict[mac][0]['zero_point']
        item['old_zp'] = tessdb_dict[mac][0]['zero_point']
        item['names'] = _names_from_mac(conn_tessdb, mac)
        items.append(item)

    context = {
        'items': items
    }

    output = render(SQL_ABSURD_ZP_TEMPLATE, context)
    with open(options.output_file, "w") as sqlfile:
        sqlfile.write(output)


# ================
# MAIN ENTRY POINT
# ================

def add_args(parser):
    # ------------------------------------------
    # Create second level parsers for 'zptess'
    # ------------------------------------------

    subparser = parser.add_subparsers(dest='command')
    zpt = subparser.add_parser(
        'easy',  help="Generate cross zptess/tessdb CSV comparison")
    zpt.add_argument('-o', '--output-file', type=str,
                     required=True, help='Output SQL File')


ENTRY_POINT = {
    'easy': easy,
}


def zp_tess(args):
    func = ENTRY_POINT[args.command]
    func(args)


def main():
    execute(main_func=zp_tess,
            add_args_func=add_args,
            name=__name__,
            version=__version__,
            description="Generate SQL to fix photometers with zp < 10.0"
            )
