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
import json
import logging

# -------------------
# Third party imports
# -------------------

import requests
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#--------------
# local imports
# -------------

from .dbutils import by_location, by_photometer, by_coordinates, log_locations, log_photometers, log_coordinates
from .dbutils import filter_dupl_coordinates


# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('mongo')

# -------------------------
# Module auxiliar functions
# -------------------------

def _photometers_from_mongo(url):
    response = requests.get(url)
    return response.json()


def mongo_remap_info(row):
    new_row = dict()
    new_row['name'] = row['name']
    new_row["longitude"] = float(row["info_location"]["longitude"])
    new_row["latitude"] = float(row["info_location"]["latitude"])
    new_row["place"] = row["info_location"]["place"]
    new_row["location"] = row["info_location"].get("town")
    new_row["region"] = row["info_location"]["place"]
    new_row["sub_region"] = row["info_location"].get("sub_region")
    new_row["country"] = row["info_location"]["country"]
    tess = row.get("info_tess")
    if(tess):
        new_row["timezone"] = row["info_tess"].get("local_timezone","Etc/UTC")
    else:
        new_row["timezone"] = "Etc/UTC"
    return new_row


def photometers_from_mongo(url):
    return list(map(mongo_remap_info, _photometers_from_mongo(url)))

# ===================
# Module entry points
# ===================

def locations(options):
    log.info(" ====================== ANALIZING MONGODB LOCATION METADATA ======================")
    mongo_input_list = photometers_from_mongo(options.url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_loc  = by_location(mongo_input_list)
    log_locations(mongo_loc)
  

def photometers(options):
    log.info(" ====================== ANALIZING MONGODB PHOTOMETER METADATA ======================")
    mongo_input_list = photometers_from_mongo(options.url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = by_photometer(mongo_input_list)
    log_photometers(mongo_phot)


def coordinates(options):
    log.info(" ====================== ANALIZING MONGODB COORDINATES METADATA ======================")
    mongo_input_list = photometers_from_mongo(options.url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    output = filter_dupl_coordinates(mongo_input_list)
    log.info("%d entries with inconsistent coordinates", len(output))
