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

#--------------
# local imports
# -------------

from .dbutils import by_location, by_photometer, log_locations, log_photometers

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


def photometers_from_mongo(url):
    response = requests.get(url)
    return response.json()

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


# ===================
# Module entry points
# ===================

def locations(options):
    log.info(" ====================== ANALIZING MONGODB LOCATION METADATA ======================")
    mongo_input_list = list(map(mongo_remap_info, photometers_from_mongo(options.url)))
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_loc  = by_location(mongo_input_list)
    log_locations(mongo_loc)
  

def photometers(options):
    log.info(" ====================== ANALIZING MONGODB PHOTOMETER METADATA ======================")
    mongo_input_list = list(map(mongo_remap_info, photometers_from_mongo(options.url)))
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = by_photometer(mongo_input_list)
    log_photometers(mongo_phot)
  

