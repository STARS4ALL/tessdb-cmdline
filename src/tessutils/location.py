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

from . import  SQL_CREATE_LOCATIONS_TEMPLATE

from .utils import  open_database, formatted_mac
from .dbutils import get_mongo_api_url, get_tessdb_connection_string
from .dbutils import group_by_name, group_by_mac, common_A_B_items, in_A_not_in_B
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

def _photometers_with_unknown_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT n.name, n.mac_address, t.tess_id, t.zero_point
        FROM name_to_mac_t AS n
        JOIN tess_t AS t USING (mac_address)
        WHERE n.mac_address IN 
            (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) = 1)
        AND n.name LIKE 'stars%'
        AND t.location_id = -1
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

def photometers_with_unknown_locations_from_tessdb(connection):
    return list(map(tessdb_remap_unknown_location_info, _photometers_with_unknown_locations_from_tessdb(connection)))


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
    

# ======================
# Second level functions
# ======================

def generate_unknown(connection, mongodb_url):
    tessdb_input_list = photometers_with_unknown_locations_from_tessdb(connection)
    
    tessdb_input_dict = group_by_mac(tessdb_input_list)
    log.info("Photometers with unkown locations: %d", len(tessdb_input_dict))

    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("Photometers with unkown locations: %d", len(tessdb_input_dict))



    mongodb_input_list = mongo_get_all_info(mongodb_url)
    mongo_db_input_dict = group_by_name(mongodb_input_list)
    common_names = common_A_B_items(tessdb_input_dict, mongo_db_input_dict)
    log.info("Photometer names that must be updates with MongoDB location: %d", len(common_names))

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
        pass
    else:
        raise NotImplementedError("Command line option not yet implemented")
   
    
    
   