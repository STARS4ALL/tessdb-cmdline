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

from .utils import open_database, formatted_mac
from .dbutils import get_tessdb_connection_string, by_coordinates, log_coordinates, by_place, log_places, log_names


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
        SELECT DISTINCT name, mac_address, zero_point, filter, 
        longitude, latitude, site, location, province, country, timezone,
        contact_name, contact_email,
        organization
        FROM tess_v 
        WHERE valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor

def tessdb_remap_info(row):
    new_row = dict()
    new_row['name'] = row[0]
    new_row['mac'] = formatted_mac(row[1])
    try:
        new_row['longitude'] = float(row[4])
    except ValueError:
        new_row['longitude'] = 0.0
    try:
        new_row['latitude'] = float(row[5])
    except ValueError:
        new_row['latitude'] = 0.0
    new_row['place'] = row[6]
    new_row["town"] = row[7]
    new_row["sub_region"] = row[8]
    new_row["region"] = None
    new_row["country"] = row[9]
    new_row["timezone"] = row[10]
    new_row["contact_name"] =  row[11]
    new_row["contact_email"] = row[12]
    new_row["org_name"] = row[13]
    new_row['org_email'] = None
    new_row['org_descr'] = None
    new_row['org_web'] = None
    new_row['org_logo'] = None
    return new_row

def photometers_from_tessdb(connection):
    return list(map(tessdb_remap_info, _photometers_from_tessdb(connection)))

def places_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT longitude, latitude, site, location, province, state, country, timezone, NULL
        FROM location_t 
        ''')
    result = [dict(zip(['longitude','latitude','place','town','sub_region','region','country','timezone','name'],row)) for row in cursor]
    return result


# ===================
# Module entry points
# ===================

def check(options):
    log.info(" ====================== ANALIZING DUPLICATES IN TESSDB METADATA ======================")
    database = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", database)
    connection = open_database(database)
    if options.places:
        log.info("Check for same place, different coordinates")
        tessdb_places  = places_from_tessdb(connection)
        tessdb_places  = by_place(tessdb_places)
        log_places(tessdb_places)
    elif options.coords:
        log.info("Check for same coordinates, different places")
        tessdb_coords  = places_from_tessdb(connection)
        tessdb_coords  = by_coordinates(tessdb_coords)
        log_coordinates(tessdb_coords)
    elif options.nearby:
        log.info("Check for nearby places in radius %0.0f meters", options.nearby)
        mongo_coords  = by_coordinates(mongo_input_list)
        log_coordinates_nearby(mongo_coords, options.nearby)
   
    else:
        log.error("No valid input option to subcommand 'check'")


def locations(options):
    connection = open_database(options.dbase)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc  = by_place(tessdb_input_list)
    log_places(tessdb_loc)
  

def photometers(options):
    connection = open_database(options.dbase)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_name(tessdb_input_list)
    log_names(tessdb_phot)
