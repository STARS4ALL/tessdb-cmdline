# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import os
import csv
import logging
import functools

# -------------------
# Third party imports
# -------------------

#--------------
# local imports
# -------------

from .utils import open_database, formatted_mac, is_mac, is_tess_mac
from .dbutils import get_tessdb_connection_string, get_zptess_connection_string, group_by_mac, common_A_B_items, in_A_not_in_B


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('zptess')

COMMON_COLUMNS = ("mac", "zptess_name", "tessdb_state", "zptess_zp", "tessdb_zp", 
    "zptess_method", "tessdb_registered", "zptess_date", "tessdb_date", "tessdb_entries", "zptess_entries")

TESSDB_COLUMNS = ("mac", "tessdb_state",  "tessdb_zp", "tessdb_registered",  "tessdb_date", "tessdb_entries")

# -------------------------
# Module auxiliar functions
# -------------------------


def _photometers_from_tessdb2(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mac_address, valid_state, zero_point, valid_since, registered
        FROM tess_t
        ORDER BY mac_address, valid_since
        ''')
    return cursor.fetchall()


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[0])
    except:
        return None
    new_row['tessdb_state'] =row[1]
    new_row['tessdb_zp'] =row[2]
    new_row['tessdb_date'] = row[3]
    new_row['tessdb_registered'] = row[4]
    return new_row                                                 


def ida_remap_info(row):
    new_row = { **row }
    new_row['mac'] = formatted_mac(row['mac_address'])
    del new_row['mac_address']
    del new_row['aux']
    # new_row = dict()
    # new_row['mac'] = formatted_mac(row['mac_address'])
    # new_row['name'] = row['name']
    # new_row['filename'] = row['filename']
    # new_row['data_rows'] = row['data_rows']
    # new_row['computed_zp_median'] = row['computed_zp_median']
    # new_row['computed_zp_stdev'] = row['computed_zp_stdev']
    # new_row['tessdb_zp_median'] = row['tessdb_zp_median']
    # new_row['tessdb_zp_stdev'] = row['tessdb_zp_stdev']
    # new_row['computed_zp_min'] = row['computed_zp_min']
    # new_row['computed_zp_max'] = row['computed_zp_max']
    # new_row['t0'] = row['t0']
    # new_row['t1'] = row['t1']

    return new_row

def read_ida_csv_file(path):
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile)
        return list(for item in reader)

def generate_common(conn_tessdb, ida_path, output_path):
    tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== IDA CSV Grouping by MAC ==========================")
    ida_input_list = list(map(ida_remap_info, read_ida_csv_file(ida_path)))
    log.info("%d entries from IDA CSV", len(ida_input_list))
    ida_input_list = group_by_mac(ida_input_list)

    common_macs = common_A_B_items(ida_input_list, tessdb_input_list)
    log.info("Common entries: %d", len(common_macs))
   

def generate_tessdb(conn_tessdb, ida_path, output_path):
    tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== IDA CSV Grouping by MAC ==========================")
    ida_input_list = list(map(ida_remap_info, read_ida_csv_file(ida_path)))
    log.info("%d entries from IDA CSV", len(ida_input_list))
    ida_input_list = group_by_mac(ida_input_list)

    only_tessdb_macs = in_A_not_in_B(tessdb_input_list, zptess_input_list)
    log.info("TESSDB MACs only, entries: %d", len(only_tessdb_macs))
   

def generate_csv_ida(conn_tessdb, ida_path, output_path):
    tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== IDA CSV Grouping by MAC ==========================")
    ida_input_list = list(map(ida_remap_info, read_ida_csv_file(ida_path)))
    log.info("%d entries from IDA CSV", len(ida_input_list))
    ida_input_list = group_by_mac(ida_input_list)
    only_ida_file_macs = in_A_not_in_B(zptess_input_list, ida_input_list)
    log.info("ZPTESS MACs only, entries: %d", len(only_ida_file_macs))
   


# ===================
# Module entry points
# ===================

def generate(options):
    log.info(" ====================== CROSS IDA / TESSD FILE comparison ======================")
    tessdb = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", tessdb)
    conn_tessdb = open_database(tessdb)
    ida_input_path = options.input_file
    if options.common:
        generate_common(conn_tessdb, input_file, options.historic, options.file)
    elif options.tessdb:
        generate_tessdb(conn_tessdb, conn_zptess, options.historic, options.file)
    else:
        generate_ida(conn_tessdb, conn_zptess, options.historic, options.file)
