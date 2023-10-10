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
from .dbutils import get_tessdb_connection_string, get_zptess_connection_string, group_by_mac, common_A_B_items, filter_out_multidict


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('zptess')

COLUMNS = ("mac", "zptess_zp", "tessdb_zp", "zptess_method", "tessdb_registered", "zptess_name", "zptess_date", "tessdb_date")

# -------------------------
# Module auxiliar functions
# -------------------------



def _photometers_from_tessdb1(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mac_address, zero_point, valid_since, registered
        FROM tess_t
        WHERE valid_state = 'Current'
        AND mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) = 1)
        ''')
    return cursor.fetchall()

def _photometers_from_tessdb2(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mac_address, zero_point, valid_since, registered
        FROM tess_t
        WHERE valid_state = 'Current'
        AND mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) = 2)
        ''')
    return cursor.fetchall()


def _photometers_from_tessdb3(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mac_address, zero_point, valid_since, registered
        FROM tess_t
        WHERE valid_state = 'Current'
        AND mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 2)
        ''')
    return cursor.fetchall()

def _photometers_from_zptess(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT mac, zero_point, session, name, calibration FROM summary_v WHERE mac in (
            SELECT mac FROM summary_v WHERE name LIKE 'stars%' AND upd_flag = 1 AND calibration = 'AUTO' GROUP BY mac HAVING count(mac) > 1 )
        UNION ALL
        SELECT mac, zero_point, session, name, calibration FROM summary_v WHERE mac in (
            SELECT mac FROM summary_v WHERE name LIKE 'stars%' AND upd_flag = 1 AND calibration = 'AUTO' GROUP BY mac HAVING count(mac) = 1 )
        ORDER BY mac, session DESC
        ''')
    return cursor.fetchall()


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[0])
    except:
        return None
    new_row['tessdb_zp'] =row[1]
    new_row['tessdb_date'] = row[2]
    new_row['tessdb_registered'] = row[3]
    #new_row['tessdb_name'] = row[0]
    return new_row

def zptess_remap_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[0])
    except:
        return None
    new_row['zptess_zp'] =row[1]
    new_row['zptess_date'] = row[2]
    new_row['zptess_name'] = row[3]
    new_row['zptess_method'] = row[4]
    return new_row


def photometers_from_tessdb(connection):
    return list(map(tessdb_remap_info, _photometers_from_tessdb1(connection)))

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
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== ZPTESS Grouping by MAC ==========================")
    zptess_input_list = group_by_mac(zptess_input_list)
    #zptess_input_list = filter_out_multidict(zptess_input_list)
    common_macs = common_A_B_items(zptess_input_list, tessdb_input_list)
    log.info("Common entries: %d", len(common_macs))
    common_list = [{**tessdb_input_list[key][0], **zptess_input_list[key][0]} for key in common_macs]
    filtered_list = filter(lambda x: x['tessdb_zp'] != x['zptess_zp'], common_list)
    sorted_list = sorted(filtered_list, key=lambda x: x['zptess_name'])
    log.info("Only %d entries differ in ZP", len(sorted_list))
    #common_zptess_list = [zptess_input_list[key][0] for key in common_macs]
    with open(options.file, 'w', newline='') as f:
        writer = csv.DictWriter(f, delimiter=';', fieldnames=COLUMNS)
        writer.writeheader()
        for row in sorted_list:
            writer.writerow(row);