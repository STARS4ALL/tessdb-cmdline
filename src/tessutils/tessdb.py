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
from .dbutils import get_tessdb_connection_string, by_coordinates, log_coordinates, log_coordinates_nearby, by_place, log_places, log_names


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('tessdb')

# -------------------------
# Module auxiliar functions
# -------------------------

def _raw_photometers_and_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT t.tess_id, n.name, t.mac_address, t.zero_point, t.filter,
        t.valid_since, t.valid_until, t.valid_state,
        t.location_id,
        t.model, t.firmware, t.channel, t.cover_offset, t.fov, t.azimuth, t.altitude,
        t.authorised, t.registered,
        l.contact_name, l.organization, l.contact_email, l.site, l.longitude, l.latitude, 
        l.elevation, l.zipcode, l.location, l.province, l.country, l.timezone
        FROM tess_t AS t
        JOIN location_t AS l   USING (location_id)
        JOIN name_to_mac_t AS n USING (mac_address)
        WHERE name LIKE 'stars%' ORDER BY name ASC
        ''')
    return cursor

def _photometers_and_locations_from_tessdb(connection):
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

def _photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name, mac_address, zero_point, filter
        FROM tess_v 
        WHERE valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor


def tessdb_remap_info(row):
    new_row = dict()
    new_row['name'] = row[0]
    new_row['mac'] = formatted_mac(row[1])
    new_row['zero_point'] =row[2]
    new_row['filter'] = row[3]
    return new_row


def tessdb_remap_all_info(row):
    new_row = dict()
    new_row['name'] = row[0]
    new_row['mac'] = formatted_mac(row[1])
    try:
        new_row['longitude'] = float(row[4]) if row[4] is not None else 0.0
    except ValueError:
        new_row['longitude'] = 0.0
    try:
        new_row['latitude'] = float(row[5]) if row[5] is not None else 0.0
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

def photometers_and_locations_from_tessdb(connection):
    return list(map(tessdb_remap_all_info, _photometers_and_locations_from_tessdb(connection)))

def places_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT longitude, latitude, site, location, province, state, country, timezone, location_id
        FROM location_t 
        ''')
    result = [dict(zip(['longitude','latitude','place','town','sub_region','region','country','timezone','name'],row)) for row in cursor]
    return result

def referenced_photometers(connection, location_id):
     row = {'location_id': location_id}
     cursor = connection.cursor()
     cursor.execute(
        '''
        SELECT COUNT(*) FROM tess_t WHERE location_id = :location_id
        ''', row)
     count = cursor.fetchone()[0]
     return count

def referenced_readings(connection, location_id):
     row = {'location_id': location_id}
     cursor = connection.cursor()
     cursor.execute(
        '''
        SELECT COUNT(*) FROM tess_readings_t WHERE location_id = :location_id
        ''', row)
     count = cursor.fetchone()[0]
     return count

def log_duplicated_coords(connection, coords_iterable):
     for coords, rows in coords_iterable.items():
        if None in coords:
            log.error("entry %s with no coordinates: %s", rows[0]['name'], coords)
        if len(rows) > 1 and all(row['place'] == rows[0]['place'] for row in rows):
            log.error("Coordinates %s has duplicated place names: %s for %s", coords, [row['place'] for row in rows], [row['name'] for row in rows])


def log_detailed_impact(connection, coords_iterable):
    for coords, rows in coords_iterable.items():
        if None in coords:
            continue
        if len(rows) == 1:
            continue
        for row in rows:
            count1 = referenced_photometers(connection, row['name'])
            count2 = referenced_readings(connection, row['name'])
            if count1 == 0 and count2 == 0:
                print("DELETE FROM location_t WHERE location_id = %d;" % row['name']);
            elif count1 != 0 and count2 != 0:
                log.info("[%d] (%s) Ojito con esta location que tiene %d referencias en tess_t y %d en tess_readings_t",
                    row['name'], row['place'], count1, count2)
            elif count1 != 0 :
                log.info("[%d] (%s) Ojito con esta location que tiene %d referencias en tess_t",
                    row['name'], row['place'], count1)
            elif count2 != 0 :
                log.info("[%d] (%s) Ojito con esta location que tiene %d referencias en tess_readings_t",
                    row['name'], row['place'], count2)


def check_proper_macs(connection):
    cursor = _raw_photometers_and_locations_from_tessdb(connection)
    bad_macs=list()
    bad_formatted=list() 
    for t in cursor:
        if not is_tess_mac(t[2]):
            log.warn("%d %s (MAC=%s) has not even a good MAC", t[0], t[1], t[2])
            bad_macs.append(t[2])
        elif not is_mac(t[2]):
            good_mac = formatted_mac(t[2])
            log.warn("%d %s (MAC=%s) should be (MAC=%s)", t[0], t[1], t[2], good_mac)
            bad_formatted.append(t[2])
    log.info("%d Bad MAC addresses aand %d bad formatted MAC addresses", len(bad_macs), len(bad_formatted))

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
        tessdb_places  = by_place(places_from_tessdb(connection))
        log_places(tessdb_places)
    elif options.coords:
        log.info("Check for same coordinates, different places")
        tessdb_coords  = by_coordinates(places_from_tessdb(connection))
        log_coordinates(tessdb_coords)
    elif options.dupl:
        log.info("Check for same coordinates, duplicated places")
        tessdb_coords  = by_coordinates(places_from_tessdb(connection))
        log_duplicated_coords(connection, tessdb_coords)
        #log_detailed_impact(connection, tessdb_coords)
    elif options.nearby:
        log.info("Check for nearby places in radius %0.0f meters", options.nearby)
        tessdb_coords  = by_coordinates(places_from_tessdb(connection))
        log_coordinates_nearby(tessdb_coords, options.nearby)
    elif options.macs:
        log.info("Check for proper MAC addresses in tess_t")
        check_proper_macs(connection);
    else:
        log.error("No valid input option to subcommand 'check'")


def locations(options):
    database = get_tessdb_connection_string()
    connection = open_database(database)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc  = by_place(tessdb_input_list)
    log_places(tessdb_loc)
  

def photometers(options):
    database = get_tessdb_connection_string()
    connection = open_database(database)
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_name(tessdb_input_list)
    log_names(tessdb_phot)
