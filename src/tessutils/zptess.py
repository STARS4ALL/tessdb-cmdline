# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import csv
import logging

# -------------------
# Third party imports
# -------------------

#--------------
# local imports
# -------------

from .utils import open_database, formatted_mac, is_mac, is_tess_mac
from .dbutils import get_tessdb_connection_string, get_zptess_connection_string, group_by_mac, common_A_B_items


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('zptess')

COLUMNS = ("zptess_method", "zptess_name", "zptess_mac", "tessdb_mac", "zptess_zp", "tessdb_zp", "zptess_date", "tessdb_date")

# -------------------------
# Module auxiliar functions
# -------------------------



def _photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mac_address, zero_point, valid_since
        FROM tess_t
        WHERE valid_state = 'Current'
        ''')
    return cursor.fetchall()

def _photometers_from_zptess(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mac, zero_point, session, name, calibration
        FROM summary_v
        WHERE name LIKE 'stars%'
        ''')
    return cursor.fetchall()


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[0])
        new_row['tessdb_mac'] = formatted_mac(row[0])
    except:
        return None
    new_row['tessdb_zp'] =row[1]
    new_row['tessdb_date'] = row[2]
    #new_row['tessdb_name'] = row[0]
    return new_row

def zptess_remap_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[0])
        new_row['zptess_mac'] = formatted_mac(row[0])
    except:
        return None
    new_row['zptess_zp'] =row[1]
    new_row['zptess_date'] = row[2]
    new_row['zptess_name'] = row[3]
    new_row['zptess_method'] = row[4]
    return new_row


def photometers_from_tessdb(connection):
    return list(map(tessdb_remap_info, _photometers_from_tessdb(connection)))

def photometers_from_zptess(connection):
    return list(map(zptess_remap_info, _photometers_from_zptess(connection)))



# ===================
# Module entry points
# ===================

def generate(options):
    log.info(" ====================== ANALIZING DUPLICATES IN TESSDB METADATA ======================")
    tessdb = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", tessdb)
    conn_tessdb = open_database(tessdb)
    zptess = get_zptess_connection_string()
    log.info("connecting to SQLite database %s", zptess)
    conn_zptess = open_database(zptess)
    tessdb_input_list = photometers_from_tessdb(conn_tessdb)
    log.info("%d entries from tessdb", len(tessdb_input_list))
    zptess_input_list = photometers_from_zptess(conn_zptess)
    log.info("%d entries from zptess", len(zptess_input_list))
    tessdb_input_list = group_by_mac(tessdb_input_list)
    zptess_input_list = group_by_mac(zptess_input_list)
    common_macs = common_A_B_items(zptess_input_list, tessdb_input_list)
    log.info("Common entries: %d", len(common_macs))
