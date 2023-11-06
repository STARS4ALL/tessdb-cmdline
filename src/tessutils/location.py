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
import math
import json
import logging
import traceback

# -------------------
# Third party imports
# -------------------

import jinja2
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#--------------
# local imports
# -------------

from . import  SQL_INSERT_LOCATIONS_TEMPLATE, SQL_PHOT_NEW_LOCATIONS_TEMPLATE, SQL_PHOT_UPD_LOCATIONS_TEMPLATE

from .utils import  open_database, formatted_mac, tessify_mac
from .dbutils import get_mongo_api_url, get_tessdb_connection_string
from .dbutils import group_by_name, group_by_mac, common_A_B_items, in_A_not_in_B, distance
from .mongodb import mongo_get_all_info

# ----------------
# Module constants
# ----------------

# Distance to consider all coordinates to be the same place
# between tessdb and MongoDB
# Experimentally determined by establishing a growth curve  
# with 1, 10, 50, 100, 150, 200 & 500 m
NEARBY_DISTANCE = 200 # meters

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('location')

# -------------------------
# Module auxiliar functions
# -------------------------
'''

-- Detalle de fotometros reparados
SELECT name, mac_address, valid_since, valid_until, valid_state FROM name_to_mac_t
WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
ORDER BY name;

-- Detalle de fotometros renombrados porque si
SELECT name, mac_address, valid_since, valid_until, valid_state FROM name_to_mac_t
WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1)
ORDER BY mac_address;


-------------------------------------------------------------------------
-- Todos los fotometros que no han sufrido ni reparaciones ni renombrados
-------------------------------------------------------------------------
SELECT name, mac_address, valid_since, valid_until, valid_state  FROM name_to_mac_t
WHERE name LIKE 'stars%'
EXCEPT
SELECT name, mac_address, valid_since, valid_until, valid_state  FROM name_to_mac_t
WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
EXCEPT
SELECT name, mac_address, valid_since, valid_until, valid_state  FROM name_to_mac_t
WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1)


----------------------------------------------------------------------------------------
-- Historial de ZP de los fotometros 'faciles' y que no están asignados a ningun 'place'
----------------------------------------------------------------------------------------

SELECT mac_address, tess_id, location_id, zero_point, valid_since, valid_until, valid_state
FROM tess_t
WHERE mac_address IN (
    SELECT mac_address  FROM name_to_mac_t
    WHERE name LIKE 'stars%'
    EXCEPT
    SELECT mac_address FROM name_to_mac_t
    WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
    EXCEPT
    SELECT mac_address FROM name_to_mac_t
    WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
AND location_id = -1
ORDER BY mac_address, valid_since

-------------------------------------
-- Lo mismo pero incluyendo el nombre
-------------------------------------

SELECT name, t.mac_address, tess_id, location_id, zero_point, t.valid_since, t.valid_until, t.valid_state
FROM tess_t AS t
JOIN name_to_mac_t USING(mac_address)
WHERE mac_address IN (
    SELECT mac_address  FROM name_to_mac_t
    WHERE name LIKE 'stars%'
    EXCEPT
    SELECT mac_address FROM name_to_mac_t
    WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
    EXCEPT
    SELECT mac_address FROM name_to_mac_t
    WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
AND location_id = -1
ORDER BY mac_address, t.valid_since

'''

def _easy_photometers_with_unknown_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, t.mac_address, tess_id, zero_point, location_id
        FROM tess_t AS t
        JOIN name_to_mac_t AS n USING(mac_address)
        WHERE mac_address IN (
            -- Photometers with no repairs and no renamings
            SELECT mac_address  FROM name_to_mac_t
            WHERE name LIKE 'stars%'
            EXCEPT -- this is the photometer substitution/repair part
            SELECT mac_address FROM name_to_mac_t
            WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            EXCEPT -- This is the renamings part
            SELECT mac_address FROM name_to_mac_t
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
        AND location_id = -1
        ORDER BY mac_address, t.valid_since
        ''')
    return cursor

def _easy_photometers_with_former_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, t.mac_address, tess_id, zero_point, location_id
        FROM tess_t AS t
        JOIN name_to_mac_t AS n USING(mac_address)
        WHERE mac_address IN (
            -- Photometers with no repairs and no renamings
            SELECT mac_address  FROM name_to_mac_t
            WHERE name LIKE 'stars%'
            EXCEPT -- this is the photometer substitution/repair part
            SELECT mac_address FROM name_to_mac_t
            WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            EXCEPT -- This is the renamings part
            SELECT mac_address FROM name_to_mac_t
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
        AND location_id > -1
        ORDER BY mac_address, t.valid_since
        ''')
    return cursor

def _coordinates_from_id(connection, location_id):
    row = dict()
    row['location_id'] = location_id
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT longitude, latitude FROM location_t WHERE location_id = :location_id
        ''', row)
    return cursor.fetchone()

def tessdb_remap_location_info(row):
    new_row = dict()
    new_row['name'] = row[0]
    try:
        new_row['mac'] = formatted_mac(row[1])
    except ValueError:
        return None
    new_row['tess_id'] = row[2]
    new_row['zero_point'] =row[3]
    new_row['location_id'] =row[4]
    return new_row



def easy_photometers_with_unknown_locations_from_tessdb(connection):
    return list(map(tessdb_remap_location_info, _easy_photometers_with_unknown_locations_from_tessdb(connection)))

def easy_photometers_with_former_locations_from_tessdb(connection):
    return list(map(tessdb_remap_location_info, _easy_photometers_with_former_locations_from_tessdb(connection)))


def render(template_path, context):
    if not os.path.exists(template_path):
        raise IOError("No Jinja2 template file found at {0}. Exiting ...".format(template_path))
    path, filename = os.path.split(template_path)
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(path or './')
    ).get_template(filename).render(context)

def generate_csv(path, iterable, fieldnames):
    with open(path, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in iterable:
            writer.writerow(row)
   
def generate_script(path, valid_coords_iterable, dbpath):
    context = dict()
    context['locations'] = valid_coords_iterable
    context['database'] = dbpath
    contents = render(CREATE_LOCATIONS_TEMPLATE, context)
    with open(path, "w") as script:
        script.write(contents)

def same_mac_filter(mongo_db_input_dict, tessdb_input_dict):
    result = list()
    names = mongo_db_input_dict.keys()
    for name in names:
        mongo_mac = mongo_db_input_dict[name][0]['mac']
        tessdb_mac = tessdb_input_dict[name][0]['mac']
        if mongo_mac  != tessdb_mac:
            log.debug("Excluding photometer %s with different MACs: MongoDB (%s) , TESSDB (%s)", name, mongo_mac, tessdb_mac)
        else:
            result.append(name)
    return result

def new_photometer_location(mongo_db_input_dict, tessdb_input_dict):
    photometers = list()
    for name, value in sorted(mongo_db_input_dict.items()):
        assert len(value) == 1
        row = value[0]
        row['masl'] = 0.0
        row['mac'] = tessify_mac(row['mac'])
        row['tess_ids'] = tuple( str(item['tess_id']) for item in tessdb_input_dict[name])
        log.debug("Must update %s [%s] with (%s,%s) coords", name, row['mac'], row['longitude'], row['latitude'])
        photometers.append(row)
    return photometers

def existing_photometer_location(mongo_db_input_dict, tessdb_input_dict, connection):
    inserters, updaters = list(), list()
    n_inserts = 0
    n_updates = 0
    for name, value in sorted(mongo_db_input_dict.items()):
        assert len(value) == 1
        row = value[0]
        row['masl'] = 0.0
        row['mac'] = tessify_mac(row['mac'])
        row['tess_ids'] = tuple( str(item['tess_id']) for item in tessdb_input_dict[name])
        locations = tuple( item['location_id'] for item in tessdb_input_dict[name])
        assert all(loc == locations[0] for loc in locations)
        row['location_id'] = locations[0]
        log.debug("Must update %s [%s] with (%s,%s) coords", name, row['mac'], row['longitude'], row['latitude'])
        tessdb_coords = _coordinates_from_id(connection, row['location_id'])
        mongodb_coords = (row['longitude'], row['latitude'])
        if distance ( mongodb_coords, tessdb_coords) < NEARBY_DISTANCE:
            n_updates += 1
            updaters.append(row)
        else:
            n_inserts += 1
            inserters.append(row)
    log.info("Must perform %d location info updates and %d location info inserts", n_updates, n_inserts)
    return inserters, updaters

# ======================
# Second level functions
# ======================

def generate_unknown(connection, mongodb_url, output_path):
    log.info("Accesing TESSDB database")
    tessdb_input_list = easy_photometers_with_unknown_locations_from_tessdb(connection)
    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("Photometer entries with unknown locations: %d", len(tessdb_input_dict))
    log.info("Accesing MongoDB database")
    mongodb_input_list = mongo_get_all_info(mongodb_url)
    mongo_db_input_dict = group_by_name(mongodb_input_list)
    common_names = common_A_B_items(tessdb_input_dict, mongo_db_input_dict)
    log.info("Photometer names that must be updated with MongoDB location: %d", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names }
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names }
    common_names = same_mac_filter(mongo_db_input_dict, tessdb_input_dict)
    log.info("Reduced list of only %d entries after MAC exclusion", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names }
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names }
    context = dict()
    context['photometers_with_new_locations'] = new_photometer_location(mongo_db_input_dict, tessdb_input_dict)
    output = render(SQL_PHOT_NEW_LOCATIONS_TEMPLATE, context)
    with open(output_path, "w") as sqlfile:
        sqlfile.write(output)

def generate_single(connection, mongodb_url, output_path):
    log.info("Accesing TESSDB database")
    tessdb_input_list = easy_photometers_with_former_locations_from_tessdb(connection)
    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("TESSDB Photometer entries with former locations: %d", len(tessdb_input_dict))
    log.info("Accesing MongoDB database")
    mongodb_input_list = mongo_get_all_info(mongodb_url)
    mongo_db_input_dict = group_by_name(mongodb_input_list)
    common_names = common_A_B_items(tessdb_input_dict, mongo_db_input_dict)
    log.info("Photometer names that must be updated with MongoDB location: %d", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names }
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names }
    common_names = same_mac_filter(mongo_db_input_dict, tessdb_input_dict)
    log.info("Reduced list of only %d entries after MAC exclusion", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names }
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names }
    context = dict()
    context['photometers_with_new_locations'],  context['photometers_with_upd_locations'] = existing_photometer_location(mongo_db_input_dict, tessdb_input_dict, connection)
    output_insert = render(SQL_PHOT_NEW_LOCATIONS_TEMPLATE, context)
    output_update = render(SQL_PHOT_UPD_LOCATIONS_TEMPLATE, context)
    with open(output_path, "w") as sqlfile:
        sqlfile.write(output_insert)
        sqlfile.write(output_update)
    

# ===================
# Module entry points
# ===================

def generate(options):
    mongodb_url = get_mongo_api_url()
    tessdb_url = get_tessdb_connection_string()
    connection = open_database(tessdb_url)
    log.info("LOCATIONS SCRIPT GENERATION")
    if options.unknown:
        generate_unknown(connection, mongodb_url, options.file)
    elif options.single:
        generate_single(connection, mongodb_url, options.file)
    else:
        raise NotImplementedError("Command line option not yet implemented")
   
    
    
   