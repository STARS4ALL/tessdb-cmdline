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

from .dbutils import by_location, by_photometer, by_coordinates, log_locations, log_photometers, log_coordinates
from .dbutils import geolocate


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
    new_row["town"] = row["info_location"].get("town")
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


def map_proposal(row):
    new_row = dict()
    keys = ["place", "place_type", "town", "sub_region", "region", "country", "timezone", "zipcode"]
    for key in keys:
        new_row[f"proposed_{key}"] = row[key]
    for key in set(row.keys()) - set(keys):
        new_row[key] = row[key]
    return new_row

def merge_info(input_iterable, proposal_iterable):
    output = list()
    for i in range(0, len(input_iterable)):
        row = {**input_iterable[i], **proposal_iterable[i]}
        output.append(row)
    return output

def proposed_location_csv(iterable, path):
    with open(path, 'w', newline='') as csvfile:
        fieldnames = ('name', 'longitude', 'latitude', 'place', 'proposed_place', 'proposed_place_type', 'town', 'proposed_town',
            'sub_region', 'proposed_sub_region', 'region', 'proposed_region', 'country', 'proposed_country', 'timezone', 'proposed_timezone', 'proposed_zipcode')
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()
        for row in iterable:
            writer.writerow(row)
        


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
    mongo_input_list  = mongo_input_list[:10]
    log.info("read %d items from MongoDB", len(mongo_input_list))
    output = geolocate(mongo_input_list)
    output = list(map(map_proposal,output))
    output = merge_info(mongo_input_list, output)
    log.info("%d entries produced", len(output))
    proposed_location_csv(output, options.output_prefix + ".csv")
