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

from . import CREATE_LOCATIONS_TEMPLATE, PROBLEMATIC_LOCATIONS_TEMPLATE
from .utils import open_database

# ----------------
# Module constants
# ----------------

DESPLIEGUE = "Fotometros TESS-W - Despliegue.csv"
LONGITUDE = 'Longitud'
LATITUDE = 'Latitud'
NAME = 'stars'
SITE_NAME = 'Nombre lugar'

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('location')

# -------------------------
# Module auxiliar functions
# -------------------------

def deployment_list(path, headers):
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=headers, delimiter=',')
        return [row for row in reader]
    

def database_list(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name 
        FROM tess_v 
        WHERE valid_state = 'Current'
        AND name LIKE 'stars%'
        AND location = 'Unknown'
        ''')
    return [row[0] for row in cursor]

from functools import partial


def filter_by_name(row, names_iterable):
    if row[NAME] in names_iterable:
        return True
    return False

def valid_coordinates(row):
    '''
    Requirement is to have real longitude and latitude
    and properly bound to 180 degress (longitude) or 90 degrees (latitude)
    '''
    try:
        longitude = float(row.get(LONGITUDE)) # may raise a ValueError
        latitude = float(row.get(LATITUDE))  # may raise a ValueError
        if( (math.fabs(longitude) > 180) or  (math.fabs(latitude) > 90)):
            raise ValueError
    except ValueError:
        flag = False
    else:
        flag = True
    return flag


def invalid_coordinates(row):
    return not valid_coordinates(row)


def check_disjoint_sets(valid_list, invalid_list):
    '''
    Check if a given photometer is at the same time in the valid list with valid coordinates
    and the invalid list with invalid coordinates
    '''
    valid_set = set([row[NAME] for row in valid_list])
    invalid_set = set([row[NAME] for row in invalid_list])
    problem_set = valid_set.intersection(invalid_set)
    if (len(problem_set)):
        log.info("The following set of photometers have lines in the spreadhseet with both valid and invalid coordinates at the same time:")
        log.info("%r", problem_set )
    else:
        log.info("Check photometer complete")
    return valid_set - problem_set
    
def check_not_empty_sitenames(row):
    return not (row[SITE_NAME] == '' or  row[SITE_NAME].isspace())

def check_empty_sitenames(row):
    return (row[SITE_NAME] == '' or  row[SITE_NAME].isspace())

def remove_embedded_newlines_in_sitenames(row):
    row[SITE_NAME] = row[SITE_NAME].replace("\n","")
    return row

def fieldnames(path, separator=","):
    with open(path) as f:
        headers = f.readline().replace("\n","").split(separator)
    log.info("headers = %s",headers)
    return headers

def photometer_filtering(dbase, input_file, headers):
    '''
    Analyzes all photometers from the excel and divides them into two categories:
    - the ones with valid latitud and Longitud coordinates
    - The rest
    '''
    connection = open_database(dbase)
    deployed_list = deployment_list(input_file, headers)
    registered_list = database_list(connection)
    matching_list = list(filter(partial(filter_by_name, names_iterable=registered_list), deployed_list))
    log.info("Matched %d photometers", len(matching_list))
    valid_coord_list = list(filter(valid_coordinates, matching_list))
    invalid_coord_list = list(filter(invalid_coordinates, matching_list))
    log.info("%d photometers with invalid coordinates", len(valid_coord_list))
    purged_set = check_disjoint_sets(valid_coord_list, invalid_coord_list)
    valid_coord_list = list(filter(partial(filter_by_name, names_iterable=purged_set), valid_coord_list))
    log.info("%d photometers with valid coordinates after the problematic ones have been removed", len(valid_coord_list))
    empty_sites_list = list(filter(check_empty_sitenames,valid_coord_list))
    log.info("%d photometers with empty site names", len(empty_sites_list))
    final_list = list(filter(check_not_empty_sitenames,valid_coord_list))
    final_list = list(map(remove_embedded_newlines_in_sitenames, final_list))
    log.info("%d photometers for final scrpt", len(final_list))
    return final_list, empty_sites_list, invalid_coord_list

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
    
def generate_json(path, iterable):
    with open(path, "w") as fd:
        json.dump(iterable, fd, indent=2)


def geolocate(iterable):
    addresses = list()
    fixed = list()
    not_fixed = list()
    geolocator = Nominatim(user_agent="STARS4ALL project")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)
    for row in iterable:
        location = geolocator.reverse(f"{row[LATITUDE]}, {row[LONGITUDE]}", language="en")
        address = location.raw['address']
        address['stars4all'] = dict()
        address['stars4all']['photometer'] = row[NAME]
        address['stars4all']['longitude'] = row[LONGITUDE]
        address['stars4all']['latitude'] = row[LATITUDE]
        addresses.append(address)
        found = False
        for location_type in ('leisure', 'amenity', 'tourism', 'building', 'road', 'hamlet',):
            try:
                row[SITE_NAME] = address[location_type]
            except KeyError:
                continue   
            else:
                found = True
                if location_type == 'road' and address.get('house_number'):
                    row[SITE_NAME] = address[location_type] + ", " + address['house_number']
                    address['stars4all']['location_type'] = 'road + house_number'
                else:
                    address['stars4all']['location_type'] = location_type
                address['stars4all']['location_name'] = row[SITE_NAME]
                break
        if found:
            fixed.append(row)
            log.debug("assigning %s -> '%s'  as place name to %s",location_type, address[location_type], row[NAME])
        else:
            address['stars4all']['location_type'] = None
            address['stars4all']['location_name'] = None
            not_fixed.append(row)
            log.warn("still without a valid place name to %s",row[NAME])
    return fixed, not_fixed, addresses


# ===================
# Module entry points
# ===================

def generate(options):
    log.info("LOCATIONS SCRIPT GENERATION")
    headers = fieldnames(options.input_file)
    valid_coords, empty_sites, invalid_coords = photometer_filtering(options.dbase, options.input_file, headers)
    
    empty_sites_fixed, empty_sites_not_fixed, addresses_json = geolocate(empty_sites)

    path =  options.output_prefix + "_empty_sites_fixed.csv"
    generate_csv(path, empty_sites_fixed, headers)
    log.info("generated CSV file -> %s", path)

    path =  options.output_prefix + "_empty_sites_not_fixed.csv"
    generate_csv(path, empty_sites_not_fixed, headers)
    log.info("generated CSV file -> %s", path)
    
    path =  options.output_prefix + "_empty_sites_geoloc.json"
    generate_json(path, addresses_json)
    log.info("generated CSV file -> %s", path)

    path =  options.output_prefix + "_invalid_coords.csv"
    generate_csv(path, invalid_coords, headers)
    log.info("generated CSV file -> %s", path)

    path = options.output_prefix + ".sh"
    generate_script(path, valid_coords, options.dbase)
    log.info("generated script file with valid coords -> %s", path)
    
    
   