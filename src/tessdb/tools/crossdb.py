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

from lica.cli import execute
from lica.validators import vfile, vdir
from lica.csv import write_csv
from lica.sqlite import open_database

#--------------
# local imports
# -------------

from .._version import __version__

from .dbutils import group_by_place, group_by_name, group_by_coordinates, group_by_mac, log_places, log_names, distance, get_mongo_api_url, ungroup_from
from .mongodb import mongo_get_location_info, mongo_get_all_info, mongo_get_photometer_info, filter_by_names, get_mac, mongo_api_body_photometer, mongo_api_update
from .tessdb import photometers_from_tessdb, photometers_and_locations_from_tessdb, places_from_tessdb

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

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


def upd_mongo_field(mongo_dict, tessdb_dict, field):
    result = list()
    for key, row in mongo_dict.items():
        mongo_field = row[0][field]
        tessdb_field = tessdb_dict[key][0][field]
        if mongo_field != tessdb_field:
            log.debug("[%s] M[%s] T[%s] UPDATING %s %s  with %s", 
                key, row[0]['mac'], tessdb_dict[key][0]['mac'],
                field, mongo_field, tessdb_field)
            row[0]['field'] = tessdb_field
            result.append(row)
    log.info("Updated %d rows for field %s",len(result), field)
    return result


def upd_mongo_mac(mongo_dict, tessdb_dict):
    return upd_mongo_field(mongo_dict, tessdb_dict, 'mac')


def upd_mongo_zp(mongo_dict, tessdb_dict):
    return upd_mongo_field(mongo_dict, tessdb_dict, 'zero_point')

def filter_fake_zero_points(tessdb_sequence):
    def _filter_fake_zero_points(x):
        log.info("%s",x)
        return 20.5 > x['zero_point'] > 18.5  if x is not None else False
    return list(filter(_filter_fake_zero_points, tessdb_sequence))

def do_update_photometer(mongo_output_list, simulated):
    url = get_mongo_api_url()
    mongo_aux_list = mongo_get_all_info(url) 
    for [row] in mongo_output_list:
        oldmac = get_mac(mongo_aux_list, row['name'])
        body = mongo_api_body_photometer(row, mongo_aux_list)
        log.info("Updating MongoDB with photometer info for %s (%s)", row['name'], oldmac)
        try:
            if(oldmac != row['mac']):
                log.info("Changing %s MAC: (%s) -> (%s)", row['name'], oldmac, row['mac'])
            mongo_api_update(url, body, oldmac, simulated)
        except ValueError:
            log.warn("Ignoring update photometer info for item %s (%s)", row['name'], oldmac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")

# ===================
# Module entry points
# ===================

def check(args):
    log.info(" ====================== PERFORM CROSS DB CHEKCS ======================")
    
    connection, path = open_database(None, 'TESSDB_URL')
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_photometer_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = group_by_name(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = group_by_name(tessdb_input_list)
    if args.mac:
        log.info("Check for MAC differences in common photometer names")
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        common_mac_check(photometer_names, mongo_phot, tessdb_phot)
    elif args.zero_point:
        log.info("Check for Zero Point differentces in common photometer names")
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        common_zp_check(photometer_names, mongo_phot, tessdb_phot)
    else:
        log.error("No valid input option to subcommand 'check'")


def photometers(args):
    log.info(" ====================== CROSS DB PHOTOMETER METADATA ======================")
    
    connection, path = open_database(None, 'TESSDB_URL')
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_all_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = group_by_name(mongo_input_list)
    tessdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))

    if args.sim_update_mac:
        tessdb_phot = group_by_name(tessdb_input_list)
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        mongo_input_list = filter_by_names(mongo_input_list, photometer_names)
        mongo_phot = group_by_name(mongo_input_list) # Again
        mongo_output_list = upd_mongo_mac(mongo_phot, tessdb_phot)
        do_update_photometer(mongo_output_list, simulated=True)
    elif args.update_mac:
        tessdb_phot = group_by_name(tessdb_input_list)
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        mongo_input_list = filter_by_names(mongo_input_list, photometer_names)
        mongo_phot = group_by_name(mongo_input_list) # Again
        mongo_output_list = upd_mongo_mac(mongo_phot, tessdb_phot)
        do_update_photometer(mongo_output_list, simulated=False)
    if args.sim_update_zp:
        tessdb_input_list = filter_fake_zero_points(tessdb_input_list)
        log.info("filtered fake ZP, remaining %d items from TessDB", len(tessdb_input_list))
        tessdb_phot = group_by_name(tessdb_input_list)
        photometer_names = common_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB",len(photometer_names))
        mongo_input_list = filter_by_names(mongo_input_list, photometer_names)
        mongo_phot = group_by_name(mongo_input_list) # Again
        mongo_output_list = upd_mongo_zp(mongo_phot, tessdb_phot)
        #do_update_photometer(mongo_output_list, simulated=True)
    

# Nos quedamos con el primer elemento del array
def flatten(grouped_iterable, keys):
    return tuple( grouped_iterable[key][0] for key in keys)

def filter_out_from_location(row):
    if 'contact_name' in row: del row['contact_name']
    if 'contact_email' in row: del row['contact_email']
    if 'org_logo' in row: del row['org_logo']
    if 'org_email' in row: del row['org_email']
    if 'org_web' in row: del row['org_web']
    if 'org_descr' in row: del row['org_descr']
    if 'org_name' in row: del row['org_name']
    if 'mac' in row: del row['mac']
    return row
  

def locations_by_place(args):
    log.info(" ====================== ANALIZING CROSS DB LOCATION METADATA ======================")
    X_HEADER = ('place', 'longitude', 'latitude',  'town', 'sub_region','region', 'country', 'timezone', 'name',)
    connection, path = open_database(None, 'TESSDB_URL')
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_location_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_place  = group_by_place(mongo_input_list)
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc = group_by_place(tessdb_input_list)
    if args.mongo:
        locations = in_mongo_not_in_tessdb(mongo_place, tessdb_loc)
        log.info("%d locations (by place name) exclusive MongoDB locations",len(locations))
        flattended_mongo_places = flatten(mongo_place, locations)
        output_list = list(filter(filter_out_from_location, flattended_mongo_places))
        suffix = "_in_mongo_not_tessdb.csv"
    if args.tess:
        locations = in_tessdb_not_in_mongo(mongo_place, tessdb_loc)
        log.info("%d locations locations (by place name) exclusive TessDB locations",len(locations))
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        output_list = list(filter(filter_out_from_location, flattened_tessdb_loc))
        suffix = "_in_tessdb_not_mongo.csv"
    if args.common:
        locations = common_items(mongo_place, tessdb_loc)
        log.info("%d locations locations (by place name) in common between MongoDB and TessDB",len(locations))
        for location in locations:
            log.debug("Location %s", location)
        flattended_mongo_places = flatten(mongo_place, locations)
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        combined_list = zip(flattended_mongo_places, flattened_tessdb_loc)
        combined_list = [j for i in zip(flattended_mongo_places, flattened_tessdb_loc) for j in i]
        output_list = list(filter(filter_out_from_location, combined_list))
        suffix = '_common_mongo_tessdb.csv'
    write_csv(args.output_prefix + suffix,  X_HEADER, output_list) 
    
def locations_by_coordinates(args):
    log.info(" ====================== ANALIZING CROSS DB LOCATION METADATA ======================")
    X_HEADER = ('place', 'longitude', 'latitude',  'town', 'sub_region','region', 'country', 'timezone', 'name',)
    connection, path = open_database(None, 'TESSDB_URL')
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_location_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_place  = group_by_coordinates(mongo_input_list)
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc = group_by_coordinates(tessdb_input_list)
    if args.mongo:
        locations = in_mongo_not_in_tessdb(mongo_place, tessdb_loc)
        log.info("%d locations (by exact coordinates) exclusive MongoDB locations",len(locations))
        flattended_mongo_places = flatten(mongo_place, locations)
        output_list = list(filter(filter_out_from_location, flattended_mongo_places))
        suffix = "_in_mongo_not_tessdb.csv"
    if args.tess:
        locations = in_tessdb_not_in_mongo(mongo_place, tessdb_loc)
        log.info("%d locations (by exact coordinates) exclusive TessDB locations",len(locations))
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        output_list = list(filter(filter_out_from_location, flattened_tessdb_loc))
        suffix = "_in_tessdb_not_mongo.csv"
    if args.common:
        locations = common_items(mongo_place, tessdb_loc)
        log.info("%d locations (by exact coordinates) in common between MongoDB and TessDB",len(locations))
        for location in locations:
            log.debug("Location %s", location)
        flattended_mongo_places = flatten(mongo_place, locations)
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        combined_list = zip(flattended_mongo_places, flattened_tessdb_loc)
        combined_list = [j for i in zip(flattended_mongo_places, flattened_tessdb_loc) for j in i]
        output_list = list(filter(filter_out_from_location, combined_list))
        suffix = '_common_mongo_tessdb.csv'
    write_csv(args.output_prefix + suffix,  X_HEADER, output_list) 
    
def locations(args):
    if args.place:
        locations_by_place(args)
    else:
        locations_by_coordinates(args)


def coordinates(args):
    '''Not being used'''
    log.info(" ====================== ANALIZING CROSS DB COORDINATES METADATA ======================")
    X_HEADER = ('mng_name','tdb_name', 'mng_coords', 'tdb_coords',
    'mng_place','tdb_place', 'mng_town','tdb_town', 'mng_sub_region', 'tdb_sub_region',
    'mng_region', 'tdb_region', 'mng_country', 'tdb_country', 'mng_timezone', 'tdb_timezone')
    url = get_mongo_api_url()
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("reading items from MongoDB")
    mongo_input_map = by_coordinates(mongo_get_location_info(url))
    log.info("reading items from TessDB")
    tessdb_input_map = by_coordinates(places_from_tessdb(connection))
    output = list()
    for i, (mongo_coords, mongo_items) in enumerate(mongo_input_map.items()):
        nearby_filter = make_nearby_filter(mongo_coords, args.lower, args.upper)
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
                        'mng_coords': str(mongo_coords),
                        'tdb_coords': str(tessdb_coords),
                        'distance':  distance(mongo_coords, tessdb_coords),
                        'mng_name':  mongo_row['name'],
                        'tdb_name': tessdb_row['name'],
                        'mng_place':  mongo_row['place'],
                        'tdb_place': tessdb_row['place'],
                        'mng_town': mongo_row['town'],
                        'tdb_town': tessdb_row['town'],
                        'mng_sub_region': mongo_row['sub_region'],
                        'tdb_sub_region': tessdb_row['sub_region'],
                        'mng_region': mongo_row['region'],
                        'tdb_region': tessdb_row['region'],
                        'mng_country': mongo_row['country'],
                        'tdb_country': tessdb_row['country'],
                        'mng_timezone': mongo_row['timezone'],
                        'tdb_timezone': tessdb_row['timezone']
                        }
                    )
    write_csv(output, X_HEADER, args.file)       


def add_args(parser):

    # -----------------------------------------
    # Create second level parsers for 'crossdb'
    # -----------------------------------------

    subparser = parser.add_subparsers(dest='command')
    
    xdbloc = subparser.add_parser('locations',  help="Cross DB locations metadata check")
    xdbloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    grp = xdbloc.add_mutually_exclusive_group(required=True)
    grp.add_argument('-p', '--place', action='store_true', help='By place name')
    grp.add_argument('-r', '--coords', action='store_true',  help='By coordinates')
    grp = xdbloc.add_mutually_exclusive_group(required=True)
    grp.add_argument('-m', '--mongo', action='store_true', help='MongoDB exclusive locations')
    grp.add_argument('-t', '--tess', action='store_true',  help='TessDB exclusive locations')
    grp.add_argument('-c', '--common', action='store_true',  help='Common locations')

    xdbphot = subparser.add_parser('photometers',  help="Cross DB photometers metadata operations")
    xdbphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    grp = xdbphot.add_mutually_exclusive_group(required=True)
    grp.add_argument('-s', '--sim-update-mac', action='store_true', help='Simulated update Mongo DB MAC with TESS-DB MAC value')
    grp.add_argument('-m', '--update-mac', action='store_true',  help='Update Mongo DB MAC with TESS-DB MAC value')
    grp.add_argument('-x', '--sim-update-zp', action='store_true', help='Simulated update Mongo DB ZP with TESS-DB ZP value')
    grp.add_argument('-z', '--update-zp', action='store_true',  help='Update Mongo DB ZP with TESS-DB ZP value')

    xdbcoord = subparser.add_parser('coordinates',  help="Cross DB photometers metadata check")
    xdbcoord.add_argument('-f', '--file', type=str, required=True, help='CSV file to generate differences')
    xdbcoord.add_argument('--lower', type=float, default=0.0, help='Lower limit in meters')
    xdbcoord.add_argument('--upper', type=float, default=1000.0, help='Upper limit in meters')

    mgphck = subparser.add_parser('check',  help="Various MongoDB metadata checks")
    mgex1 = mgphck.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-m', '--mac', action='store_true', help="Check for common photometer's MACs")
    mgex1.add_argument('-z', '--zero-point', action='store_true', help="Check for common photometer's Zero Points")

# ================
# MAIN ENTRY POINT
# ================

ENTRY_POINT = {
    'locations': locations,
    'photometers': photometers,
    'coordinates': coordinates,
    'check': check,
}
def cross_db(args):
    func = ENTRY_POINT[args.command]
    func(args)

def main():
    execute(main_func=cross_db, 
        add_args_func=add_args, 
        name=__name__, 
        version=__version__,
        description="STARS4ALL MongoDB Utilities"
    )

       