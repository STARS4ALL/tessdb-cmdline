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

#--------------
# local imports
# -------------

from .utils import open_database
from .dbutils import by_location, by_photometer, log_locations, log_photometers, distance
from .mongodb import photometers_from_mongo
from .tessdb import photometers_from_tessdb
# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('crossdb')

# -------------------------
# Module auxiliar functions
# -------------------------

def common_items(mongo_iterable, tessdb_iterable):
    return set(mongo_iterable.keys()).intersection(set(tessdb_iterable.keys()))

def in_mongo_not_in_tessdb(mongo_iterable, tessdb_iterable):
    return set(mongo_iterable.keys()) - set(tessdb_iterable.keys())

def in_tessdb_not_in_mongo(mongo_iterable, tessdb_iterable):
    return set(tessdb_iterable.keys()) - set(mongo_iterable.keys())


def common_locations(mongo_iterable, tessdb_iterable):
    locations = common_items(mongo_iterable, tessdb_iterable)
    log.info("%d locations in common between MongoDB and TessDB",len(locations))
    for location in locations:
        log.debug("Location %s", location)

def mongo_exclusive_locations(mongo_iterable, tessdb_iterable):
    locations = in_mongo_not_in_tessdb(mongo_iterable, tessdb_iterable)
    log.info("%d locations exclusive MongoDB locations",len(locations))

def tessdb_exclusive_locations(mongo_iterable, tessdb_iterable):
    locations = in_tessdb_not_in_mongo(mongo_iterable, tessdb_iterable)
    log.info("%d locations exclusive TessDB locations",len(locations))


def make_nearby_filter(row2, lower, upper):
    def distance_filter(row1):
        return (lower <= distance(row1, row2) <= upper)
    return distance_filter


# ===================
# Module entry points
# ===================


def locations(options):
    log.info(" ====================== ANALIZING CROSS DB LOCATION METADATA ======================")
    connection = open_database(options.dbase)
    mongo_input_list = photometers_from_mongo(options.url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_loc  = by_location(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc = by_location(tessdb_input_list)
    if options.mongo:
        locations = in_mongo_not_in_tessdb(mongo_loc, tessdb_loc)
        log.info("%d locations exclusive MongoDB locations",len(locations))
    if options.tess:
        locations = in_tessdb_not_in_mongo(mongo_loc, tessdb_loc)
        log.info("%d locations exclusive TessDB locations",len(locations))
    if options.common:
        locations = common_items(mongo_loc, tessdb_loc)
        log.info("%d locations in common between MongoDB and TessDB",len(locations))
        for location in locations:
            log.debug("Location %s", location)



def photometers(options):
    log.info(" ====================== ANALIZING CROSS DB PHOTOMETER METADATA ======================")
    connection = open_database(options.dbase)
    mongo_input_list = photometers_from_mongo(options.url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = by_photometer(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_photometer(tessdb_input_list)
    if options.mongo:
        photometers = in_mongo_not_in_tessdb(mongo_phot, tessdb_phot)
        log.info("%d photometers exclusive MongoDB locations",len(photometers))
    if options.tess:
        photometers = in_tessdb_not_in_mongo(mongo_phot, tessdb_phot)
        log.info("%d photometers exclusive TessDB locations",len(photometers))
    if options.common:
        photometers = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometers))
        for photometer in photometers:
            log.debug("Photometer %s", photometer)
   

def coordinates(options):
    log.info(" ====================== ANALIZING CROSS DB PHOTOMETER METADATA ======================")
    connection = open_database(options.dbase)
    mongo_input_list = photometers_from_mongo(options.url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    output = list()
    for mongo_item in mongo_input_list:
        nearby_filter = make_nearby_filter(mongo_item, options.lower, options.upper)
        nearby_list = list(filter(nearby_filter, tessdb_input_list))
        output.append(nearby_list)
        if (len(nearby_list)):
            log.info("Nearby to %s (Lon=%f, Lat=%f) are: %s", 
                mongo_item['place'], 
                mongo_item['longitude'], 
                mongo_item['latitude'], 
                [ (r['place'], r['longitude'], r['latitude']) for r in nearby_list]
            )
       