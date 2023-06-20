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
import collections

# -------------------
# Third party imports
# -------------------

import requests

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

EARTH_RADIUS =  6371000.0 # in meters 

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('location')

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


def error_lat(latitude, arc_error):
    '''
    returns latitude estimated angle error in for an estimated arc error in meters.
    latitude given in radians
    '''
    return arc_error /  EARTH_RADIUS


def error_long(longitude, latitude, arc_error):
    '''
    returns longitude estimated angle error for an estimated arc error in meters
    longitude given in radians
    '''
    _error_lat = error_lat(latitude, arc_error)
    _term_1 = arc_error / (EARTH_RADIUS * math.cos(latitude))
    _term2 = longitude * math.tan(latitude)*_error_lat
    return _term1 - _term2


def photometers_from_mongo(url):
    response = requests.get(url)
    return response.json()

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

def mongo_remap_info(row):
    for key in ('zero_point', "filters", "latitude", "longitude", "country", "city", "place", "mov_sta_position", "local_timezone", "tester", "location"):
        row.pop('key', None)
    row["longitude"] = float(row["info_location"]["longitude"])
    row["latitude"] = float(row["info_location"]["latitude"])
    row["place"] = row["info_location"]["place"]
    row["location"] = row["info_location"].get("town")
    row["region"] = row["info_location"]["place"]
    row["sub_region"] = row["info_location"].get("sub_region")
    row["country"] = row["info_location"]["country"]
    tess = row.get("info_tess")
    if(tess):
        row["timezone"] = row["info_tess"].get("local_timezone","Etc/UTC")
    else:
        row["timezone"] = "Etc/UTC"
    return row


def check_location_consistency(place, photometers):
    '''Check for coordinates consistency among phothometers deployed on the same 'place' name'''
    longitudes = set(phot['longitude'] for phot in photometers)
    latitudes = set(phot['latitude'] for phot in photometers)
    if len(longitudes) > 1:
        log.warn("Location %s has different longitudes. %s", place, longitudes)
    if len(latitudes) > 1:
        log.warn("Location %s has different latitudes. %s", place, latitudes)



def extract_locations(phot_iterable):
    loc = collections.defaultdict(list)
    for row in phot_iterable:
        loc[row['place']].append(row)
    log.info("From %d photometers, we have extracted %d different places",len(phot_iterable), len(loc.keys()))
    for place, photometers in loc.items():
        if len(photometers) > 1:
            log.info("Location %s has %d photometers: %s", place, len(photometers), [phot['name'] for phot in photometers])
            check_location_consistency(place, photometers)
    return loc

# ===================
# Module entry points
# ===================

def intersect(options):
    connection = open_database(options.dbase)
    log.info("Common coordinates locations between MomgoDB and TessDB")
    mongo_phot_list = list(map(mongo_remap_info, photometers_from_mongo(options.url)))
    log.info("read %d photometers from MongoDB", len(mongo_phot_list))
    log.info(" ====================== ANALIZING MONGODB DATA ======================")
    mongo_loc = extract_locations(mongo_phot_list)
    log.info(" ====================== ANALIZING TESSDB  DATA ======================")
    #log.debug(json.dumps(mongo_phot_list, sort_keys=True, indent=2))
    tessdb_phot_list = list(map(tessdb_remap_info, photometers_from_tessdb(connection)))
    tessdb_loc = extract_locations(tessdb_phot_list)
    #log.info("read %d photometers from TessDB", len(tessdb_phot_list))
