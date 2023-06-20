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

def photometers_from_tessdb(connection):
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
    newrow = dict()
    newrow['name'] = row[0]
    try:
        newrow['longitude'] = float(row[1])
    except ValueError:
        newrow['longitude'] = 0.0
    try:
        newrow['latitude'] = float(row[2])
    except ValueError:
        newrow['latitude'] = 0.0
    newrow['place'] = row[3]
    newrow["location"] = row[4]
    newrow["sub_region"] = row[5]
    newrow["region"] = row[6]
    newrow["country"] = row[7]
    newrow["timezone"] = row[8]
    return newrow


# ===================
# Module entry points
# ===================

def locations(options):
    connection = open_database(options.dbase)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = list(map(tessdb_remap_info,  photometers_from_tessdb(connection)))
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc  = by_location(tessdb_input_list)
    log_locations(tessdb_loc)
  

def photometers(options):
    connection = open_database(options.dbase)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = list(map(tessdb_remap_info,  photometers_from_tessdb(connection)))
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_photometer(tessdb_input_list)
    log_photometers(tessdb_phot)
