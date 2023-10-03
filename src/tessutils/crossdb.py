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
from .dbutils import by_place, by_name, by_coordinates, by_mac, log_places, log_names, distance, get_mongo_api_url, get_tessdb_connection_string
from .mongodb import mongo_get_location_info, mongo_get_all_info, mongo_get_photometer_info
from .tessdb import photometers_from_tessdb, places_from_tessdb

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


def make_nearby_filter(tuple2, lower, upper):
    def distance_filter(tuple1):
        dist = distance(tuple1, tuple2)
        return (lower <= dist <= upper) if dist is not None else False
    return distance_filter


def similar_locations_csv(iterable, path):
    with open(path, 'w', newline='') as csvfile:
        fieldnames = ('source', 'name', 'longitude', 'latitude', 'place', 'town',
            'sub_region', 'region', 'country', 'timezone', )
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()
        for row in iterable:
            writer.writerow(row)


def common_mac_check(keys, mongo_iterable, tessdb_iterable):
    log.info("comparing %d keys", len(keys))
    result = list()
    for key in sorted(keys):
        if len(mongo_iterable[key]) > 1:
            log.warn("Skippiing %s because it is duplicated: %s", key, mongo_iterable[key])
        else:
            mongo_mac = mongo_iterable[key][0]['mac']
            tessdb_mac = tessdb_iterable[key][0]['mac']
            if mongo_mac != tessdb_mac:
                result.append(key)
                log.debug("MAC differs for: %s. Mongo MAC = %s, Tessdb MAC = %s", key, 
                    mongo_mac, tessdb_mac)
    log.info("Found %d MAC differences", len(result))

def common_zp_check(keys, mongo_iterable, tessdb_iterable):
    log.info("comparing %d keys", len(keys))
    result = list()
    for key in sorted(keys):
        if len(mongo_iterable[key]) > 1:
            log.warn("Skippiing %s because it is duplicated: %s", key, mongo_iterable[key])
        else:
            mongo_zp = mongo_iterable[key][0]['zero_point']
            tessdb_zp = tessdb_iterable[key][0]['zero_point']
            tessdb_mac = tessdb_iterable[key][0]['mac']
            if mongo_zp != tessdb_zp:
                if tessdb_zp < 19.0:
                    log.warn("Fake tessdb ZP %s for %s, (%s)", tessdb_zp, key, tessdb_mac)
                else:
                    result.append(key)
                    log.debug("ZP differs for: %s. Mongo ZP = %s, Tessdb ZP = %s", key, 
                    mongo_zp, tessdb_zp)
    log.info("Found %d Zero Point differences", len(result))

# ===================
# Module entry points
# ===================

def check(options):
    log.info(" ====================== PERFORM CROSS DB CHEKCS ======================")
    database = get_tessdb_connection_string()
    connection = open_database(database)
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_photometer_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = by_name(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_name(tessdb_input_list)
    if options.mac:
        log.info("Check for MAC differentces in common photometer names")
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        common_mac_check(photometer_names, mongo_phot, tessdb_phot)
    elif options.zero_point:
        log.info("Check for Zero Point differentces in common photometer names")
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        common_zp_check(photometer_names, mongo_phot, tessdb_phot)
    else:
        log.error("No valid input option to subcommand 'check'")




def photometers(options):
    log.info(" ====================== ANALIZING CROSS DB PHOTOMETER METADATA ======================")
    database = get_tessdb_connection_string()
    connection = open_database(database)
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_photometer_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = by_name(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = by_name(tessdb_input_list)
    if options.mongo:
        photometers = in_mongo_not_in_tessdb(mongo_phot, tessdb_phot)
        log.info("%d photometers exclusive MongoDB locations",len(photometers))
    if options.tess:
        photometers = in_tessdb_not_in_mongo(mongo_phot, tessdb_phot)
        log.info("%d photometers exclusive TessDB locations",len(photometers))
    if options.common:
        photometers = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometers))
        common_mac_check(photometers, mongo_phot, tessdb_phot)
   


def locations(options):
    log.info(" ====================== ANALIZING CROSS DB LOCATION METADATA ======================")
    database = get_tessdb_connection_string()
    connection = open_database(database)
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_location_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_place  = by_place(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc = by_place(tessdb_input_list)
    if options.mongo:
        locations = in_mongo_not_in_tessdb(mongo_place, tessdb_loc)
        log.info("%d locations exclusive MongoDB locations",len(locations))
    if options.tess:
        locations = in_tessdb_not_in_mongo(mongo_place, tessdb_loc)
        log.info("%d locations exclusive TessDB locations",len(locations))
    if options.common:
        locations = common_items(mongo_place, tessdb_loc)
        log.info("%d locations in common between MongoDB and TessDB",len(locations))
        for location in locations:
            log.debug("Location %s", location)




def coordinates(options):
    log.info(" ====================== ANALIZING CROSS DB COORDINATES METADATA ======================")
    url = get_mongo_api_url()
    database = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", database)
    connection = open_database(database)
    log.info("reading items from MongoDB")
    mongo_input_map = by_coordinates(mongo_get_location_info(url))
    log.info("reading items from TessDB")
    tessdb_input_map = by_coordinates(places_from_tessdb(connection))
    output = list()
    for mongo_coords, mongo_item in mongo_input_map.items():
        nearby_filter = make_nearby_filter(mongo_coords, options.lower, options.upper)
        nearby_list = list(filter(nearby_filter, tessdb_input_map.keys()))
        if (len(nearby_list)):
            mongo_item['source'] = 'mongoDB'
            output.append(mongo_item)
            for nearby in nearby_list:
                nearby['source'] = 'tessDB'
                output.append(nearby)
            log.info("Nearby to %s (Lon=%f, Lat=%f) are: %s", 
                mongo_item['place'], 
                mongo_item['longitude'], 
                mongo_item['latitude'], 
                [ (r['place'], r['longitude'], r['latitude']) for r in nearby_list]
            )
    similar_locations_csv(output, options.output_prefix + '.csv')


X_HEADER = ('mongo_coords', 'tessdb_coords', 'distance', 'mongo_name','tessdb_name', 
    'mongo_place','tessdb_place', 'mongo_town','tessdb_town', 'mongo_sub_region', 'tessdb_sub_region',
    'mongo_region', 'tessdb_region', 'mongo_country', 'tessdb_country', 'mongo_timezone', 'tessdb_timezone')

def write_csv(sequence, header, path, delimiter=';'):
    with open(path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header, delimiter=delimiter)
        writer.writeheader()
        for row in sequence:
            writer.writerow(row)
    log.info("generated CSV file: %s", path)

def coordinates(options):
    log.info(" ====================== ANALIZING CROSS DB COORDINATES METADATA ======================")
    url = get_mongo_api_url()
    database = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", database)
    connection = open_database(database)
    log.info("reading items from MongoDB")
    mongo_input_map = by_coordinates(mongo_get_location_info(url))
    log.info("reading items from TessDB")
    tessdb_input_map = by_coordinates(places_from_tessdb(connection))
    output = list()
    for i, (mongo_coords, mongo_items) in enumerate(mongo_input_map.items()):
        nearby_filter = make_nearby_filter(mongo_coords, options.lower, options.upper)
        nearby_list = list(filter(nearby_filter, tessdb_input_map))
        nearby_map = dict(zip(nearby_list, [tessdb_input_map[k] for k in nearby_list]))
        for j, (tessdb_coords, tessdb_items) in enumerate(nearby_map.items()):
            log.info("===================")
            log.info("len(MONGO ITEM[%d]) = %d, len(TESSDB ITEM[%d] = %d)",i,len(mongo_items),j,len(tessdb_items))
            for mongo_row in mongo_items:
                for tessdb_row in tessdb_items:
                    log.info("DIST: %d, MONGO ITEM: %s TESSDB ITEM: %s", distance(mongo_coords, tessdb_coords), mongo_row, tessdb_row)
                    output.append(
                        {
                        'mongo_coords': str(mongo_coords),
                        'tessdb_coords': str(tessdb_coords),
                        'distance':  distance(mongo_coords, tessdb_coords),
                        'mongo_name':  mongo_row['name'],
                        'tessdb_name': tessdb_row['name'],
                        'mongo_place':  mongo_row['place'],
                        'tessdb_place': tessdb_row['place'],
                        'mongo_town': mongo_row['town'],
                        'tessdb_town': tessdb_row['town'],
                        'mongo_sub_region': mongo_row['sub_region'],
                        'tessdb_sub_region': tessdb_row['sub_region'],
                        'mongo_region': mongo_row['region'],
                        'tessdb_region': tessdb_row['region'],
                        'mongo_country': mongo_row['country'],
                        'tessdb_country': tessdb_row['country'],
                        'mongo_timezone': mongo_row['timezone'],
                        'tessdb_timezone': tessdb_row['timezone']
                        }
                    )

    write_csv(output, X_HEADER, options.file)       



       