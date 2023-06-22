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

from .utils import open_database
from .dbutils import by_location, by_photometer, log_locations, log_photometers


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('tessdb')

# -------------------------
# Module auxiliar functions
# -------------------------

def _photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name, longitude, latitude, site, location, province, "Bug", country, timezone 
        FROM tess_v 
        WHERE valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor

def tessdb_remap_info(row):
    new_row = dict()
    new_row['name'] = row[0]
    try:
        new_row['longitude'] = float(row[1])
    except ValueError:
        new_row['longitude'] = 0.0
    try:
        new_row['latitude'] = float(row[2])
    except ValueError:
        new_row['latitude'] = 0.0
    new_row['place'] = row[3]
    new_row["location"] = row[4]
    new_row["sub_region"] = row[5]
    new_row["region"] = row[6]
    new_row["country"] = row[7]
    new_row["timezone"] = row[8]
    return new_row

def photometers_from_tessdb(connection):
    return list(map(tessdb_remap_info, _photometers_from_tessdb(connection)))

# ===================
# Module entry points
# ===================

def locations(options):
    connection = open_database(options.dbase)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc  = by_location(tessdb_input_list)
    log_locations(tessdb_loc)
  

def photometers(options):
    connection = open_database(options.dbase)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_photometer(tessdb_input_list)
    log_photometers(tessdb_phot)
