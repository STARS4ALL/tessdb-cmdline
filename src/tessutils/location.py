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

from . import  SQL_INSERT_LOCATIONS_TEMPLATE, SQL_UPDATE_PHOT_LOCATIONS_TEMPLATE

from .utils import  open_database, formatted_mac
from .dbutils import get_mongo_api_url, get_tessdb_connection_string
from .dbutils import group_by_name, group_by_mac, common_A_B_items, in_A_not_in_B, distance
from .mongodb import mongo_get_all_info

# ----------------
# Module constants
# ----------------


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
        SELECT name, t.mac_address, tess_id, zero_point
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

def tessdb_remap_unknown_location_info(row):
    new_row = dict()
    new_row['name'] = row[0]
    try:
        new_row['mac'] = formatted_mac(row[1])
    except ValueError:
        return None
    new_row['tess_id'] = row[2]
    new_row['zero_point'] =row[3]
    return new_row

def easy_photometers_with_unknown_locations_from_tessdb(connection):
    return list(map(tessdb_remap_unknown_location_info, _easy_photometers_with_unknown_locations_from_tessdb(connection)))


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
    
def new_photometer_location(mongo_db_input_dict):
    photometers = list()
    for name, value in mongo_db_input_dict.items():
        row = value[0]
        row['masl'] = 0.0
        log.info("Must update %s [%s] with (%s,%s) coords", name, row['mac'], row['longitude'], row['latitude'])
        photometers.append(row)
    return photometers

def same_mac_filter(mongo_db_input_dict, tessdb_input_dict):
    result = list()
    names = mongo_db_input_dict.keys()
    for name in names:
        mongo_mac = mongo_db_input_dict[name][0]['mac']
        tessdb_mac = tessdb_input_dict[name][0]['mac']
        if mongo_mac  != tessdb_mac:
            log.warn("Excluding photometer %s with different MACs: MongoDB (%s) , TESSDB (%s)", name, mongo_mac, tessdb_mac)
        else:
            result.append(name)
    return result

# ======================
# Second level functions
# ======================

def generate_unknown(connection, mongodb_url):
    tessdb_input_list = easy_photometers_with_unknown_locations_from_tessdb(connection)
    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("Photometer entries with unknown locations: %d", len(tessdb_input_dict))
    mongodb_input_list = mongo_get_all_info(mongodb_url)
    mongo_db_input_dict = group_by_name(mongodb_input_list)
    common_names = common_A_B_items(tessdb_input_dict, mongo_db_input_dict)
    log.info("Photometer names that must be updates with MongoDB location: %d", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names }
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names }
    common_names = same_mac_filter(mongo_db_input_dict, tessdb_input_dict)
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names }
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names }
    context = dict()
    context['photometers'] = new_photometer_location(mongo_db_input_dict)
    output = render(SQL_UPDATE_PHOT_LOCATIONS_TEMPLATE, context)
    log.info(output)

# ===================
# Module entry points
# ===================

def generate(options):
    mongodb_url = get_mongo_api_url()
    tessdb_url = get_tessdb_connection_string()
    connection = open_database(tessdb_url)
    log.info("LOCATIONS SCRIPT GENERATION")
    if options.unknown:
        generate_unknown(connection, mongodb_url)
    elif options.single:
        raise NotImplementedError("Command line option not yet implemented")
    else:
        raise NotImplementedError("Command line option not yet implemented")
   
    
    
   