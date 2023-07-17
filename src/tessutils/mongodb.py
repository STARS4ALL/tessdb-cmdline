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
import json
import logging

# -------------------
# Third party imports
# -------------------

import requests

#--------------
# local imports
# -------------

from .dbutils import by_place, by_photometer, by_coordinates, log_places, log_photometers, log_coordinates
from .dbutils import get_mongo_api_url, get_mongo_api_key, geolocate


# ----------------
# Module constants
# ----------------

# CSV File generation rows
LOCATION_HEADER = ('name', 'longitude', 'latitude', 'place', 'town', 'sub_region', 'region','country','timezone')
PHOTOMETER_HEADER = ('name', 'mac', 'zero_point', 'filters')
ORGANIZATION_HEADER = ('name', 'org_name', 'org_description', 'org_phone', 'org_email', 'org_web_url', 'org_logo_url',)
CONTACT_HEADER = ('name', 'contact_name', 'contact_mail', 'contact_phone')
ALL_HEADER = ('name', 'mac') + PHOTOMETER_HEADER[2:] + LOCATION_HEADER[1:] + ORGANIZATION_HEADER[1:] + CONTACT_HEADER[1:]

NOMINATIM_HEADER = ('name', 'longitude', 'latitude', 'place', 'nominatim_place', 'nominatim_place_type', 'town', 'nominatim_town', 'nominatim_town_type',
            'sub_region', 'nominatim_sub_region', 'nominatim_sub_region_type', 'region', 'nominatim_region', 'nominatim_region_type', 
            'country', 'nominatim_country', 'timezone', 'nominatim_timezone', 'nominatim_zipcode',)

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('mongo')

# -------------------------
# Module auxiliar functions
# -------------------------

def mongo_api_get_names(url):
    '''This request gets name, mac & location info'''
    url = url + "/photometers_list"
    body = {"token": get_mongo_api_key() }
    response = requests.post(url, json=body)
    response.raise_for_status()
    return response.json()

def mongo_api_get_details(url):
    '''This requests gets all infomation except mac'''
    url = url + "/photometers"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def mongo_api_get_details_single(url, name):
    '''This requests gets all infomation except mac'''
    url = url + "/photometers/" + name 
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def mongo_api_create(url, body):
    '''Create a new entry in MongoDB'''
    body['isNew'] =  True
    body['token'] = get_mongo_api_key()
    name = body['tess']['name']
    mac = body['tess']['mac']
    url = url + "/photometers/" + name + "/" + mac
    response = requests.post(url, json=body)
    response.raise_for_status()
    return response.json()

def mongo_api_update(url, body, mac=None):
    '''Update an existing entry in MongoDB'''
    body['isNew'] =  False
    body['token'] = get_mongo_api_key()
    name = body['tess']['name']
    if not mac:
        mac = body['tess']['mac']
    url = url + "/photometers/" + name + "/" + mac
    response = requests.post(url, json=body)
    response.raise_for_status()
    return response.json()

def mongo_api_body_location(row, aux_iterable):
    zero_point = get_zero_point(aux_iterable, row['name']) # This is a hack, shouldn't be here
    filters = get_filters(aux_iterable, row['name']) # This is a hack, shouldn't be here
    return {
        "tess": {
            "name": row['name'].strip(),
            "info_tess": {
                "zero_point": zero_point if zero_point is not None else None,
                "filters": filters.strip() if filters is not None else None, # This is a hack, shouldn't be here
                "local_timezone": row['timezone'].strip() if row['timezone'] is not None else None,
            },
            "info_location": {
                "longitude": float(row['longitude']) if row['longitude'] is not None else None,
                "latitude": float(row['latitude']) if row['latitude'] is not None else None,
                "place": row['place'].strip() if row['place'] is not None else None,
                "town": row['town'].strip() if row['town'] is not None else None,
                "sub_region": row['sub_region'].strip() if row['sub_region'] is not None else None,
                "region": row['region'].strip() if row['region'] is not None else None,
                "country": row['country'].strip() if row['country'] is not None else None,
            },
        },
    }

def mongo_api_body_photometer(row, aux_iterable, create=False):
     # This is a hack, shouldn't be here
    local_timezone = get_timezone(aux_iterable, row['name']) if not create else 'Etc/UTC'
    return {
        "tess": {
            "name": row['name'].strip(),
            "mac": row['mac'].upper().strip(),
            "info_tess": {
                "zero_point": row['zero_point'].strip() if row['zero_point'] is not None else None,
                "filters": row['filters'].strip() if row['filters'] is not None else None,
                "local_timezone": local_timezone.strip() if local_timezone is not None else None,
            },
        },
    }


def mongo_api_body_organization(row):
    return {
        "tess": {
            "name": row['name'].strip(),
            "info_org": {
                "name": row['org_name'].strip() if row['org_name'] is not None else None,
                "web_url": row['org_web_url'].strip() if row['org_web_url'] is not None else None,
                "description": row['org_description'].strip() if row['org_description'] is not None else None,
                "logo_url": row['org_logo_url'].strip() if row['org_logo_url'] is not None else None,
                "email": row['org_email'].strip() if row['org_email'] is not None else None,
                "phone": row['org_phone'].strip() if row['org_phone'] is not None else None,
            }
        }
    }

def mongo_api_body_contact(row):
    return {
        "tess": {
            "name": row['name'].strip(),
            "info_contact": {
                "name": row['contact_name'].strip() if row['contact_name'] is not None else None,
                "mail": row['contact_mail'].strip() if row['contact_mail'] is not None else None,
                "phone": row['contact_phone'].strip() if row['contact_phone'] is not None else None,
            }
        }
    }


def mongo_get_all(url):
    '''Correlates all entries with the missing mac information using just two HTTP requests'''
    names_list = mongo_api_get_names(url)
    details_list = mongo_api_get_details(url)
    assert len(names_list) == len(details_list)
    zipped = zip(names_list, details_list)
    for item in zipped:
        assert item[0]['name'] == item[1]['name']
        item[1]['mac'] = item[0].get('mac')
    return details_list


def mongo_flatten_location(row):
    new_row = dict()
    new_row['name'] = row['name']
    info_location = row.get("info_location")
    if info_location:
        new_row["longitude"] = float(row["info_location"]["longitude"])
        new_row["latitude"] = float(row["info_location"]["latitude"])
        new_row["place"] = row["info_location"]["place"]
        new_row["town"] = row["info_location"].get("town")
        new_row["region"] = row["info_location"].get("region")
        new_row["sub_region"] = row["info_location"].get("sub_region")
        new_row["country"] = row["info_location"]["country"]
    else:
        new_row["longitude"] = None
        new_row["latitude"] = None
        new_row["place"] = None
        new_row["town"] = None
        new_row["region"] = None
        new_row["sub_region"] = None
        new_row["country"] = None
    tess = row.get("info_tess")
    if tess:
        new_row["timezone"] = row["info_tess"].get("local_timezone","Etc/UTC")
    else:
        new_row["timezone"] = "Etc/UTC"
    return new_row

def mongo_flatten_photometer(row):
    new_row = dict()
    new_row['name'] = row['name']
    new_row['mac'] = row.get('mac',None)
    tess = row.get("info_tess")
    if(tess):
        new_row["zero_point"] = row["info_tess"].get("zero_point",None)
        new_row["filters"] = row["info_tess"].get("filters",None)
    else:
        new_row["filters"] = None
        new_row["zero_point"] = None
    return new_row

def mongo_flatten_organization(row):
    new_row = dict()
    new_row['name'] = row['name']
    organization = row.get("info_org")
    if(organization):
        new_row['org_name'] = row["info_org"].get("name")
        new_row['org_email'] = row["info_org"].get("email")
        new_row['org_description'] = row["info_org"].get("description")
        new_row['org_web_url'] = row["info_org"].get("web_url")
        new_row['org_logo_url'] = row["info_org"].get("logo_url")
        new_row['org_phone'] = row["info_org"].get("logo_phone")
    else:
        new_row['org_name'] = None
        new_row['org_email'] = None
        new_row['org_description'] = None
        new_row['org_web_url'] = None
        new_row['org_logo_url'] = None
        new_row['org_phone'] = None
    return new_row

def mongo_flatten_contact(row):
    new_row = dict()
    new_row['name'] = row['name']
    contact = row.get("info_contact")
    if (contact):
        new_row['contact_name'] = row["info_contact"].get("name")
        new_row['contact_mail'] = row["info_contact"].get("mail")
        new_row['contact_phone'] = row["info_contact"].get("phone")
    else:
        new_row['contact_name'] = None
        new_row['contact_mail'] = None
        new_row['contact_phone'] = None
    return new_row

def mongo_flatten_all(row):
    dict1 = mongo_flatten_photometer(row)
    dict2 = mongo_flatten_location(row)
    dict3 = mongo_flatten_organization(row)
    dict4 = mongo_flatten_contact(row)
    new_row = {**dict1, **dict2}
    new_row = {**new_row, **dict3}
    new_row = {**new_row, **dict4}
    return new_row


def mongo_get_location_info(url):
    return list(map(mongo_flatten_location, mongo_get_all(url)))

def mongo_get_photometer_info(url):
    return list(map(mongo_flatten_photometer, mongo_get_all(url)))

def mongo_get_organization_info(url):
    return list(map(mongo_flatten_organization, mongo_get_all(url)))

def mongo_get_contact_info(url):
    return list(map(mongo_flatten_contact, mongo_get_all(url)))

def mongo_get_all_info(url):
    return list(map(mongo_flatten_all, mongo_get_all(url)))
    


def remap_nominatim(row):
    new_row = dict()
    keys = ("place", "place_type", "town", "town_type", "sub_region", "sub_region_type", "region", "region_type", "country", "timezone", "zipcode")
    for key in keys:
        new_row[f"nominatim_{key}"] = row[key]
    for key in set(row.keys()) - set(keys):
        new_row[key] = row[key]
    return new_row

def merge_info(input_iterable, nominatim_iterable):
    output = list()
    for i in range(0, len(input_iterable)):
        row = {**input_iterable[i], **nominatim_iterable[i]}
        output.append(row)
    return output

def write_csv(sequence, header, path):
    with open(path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header, delimiter=';')
        writer.writeheader()
        for row in sequence:
            writer.writerow(row)
    log.info("generated CSV file: %s", path)

def read_csv(path, header):
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        sequence = [row for row in reader]
        return sequence

def filter_by_names(iterable, names):
    def _filter_by_names(row):
        return row['name'] in names
    return list(filter(_filter_by_names, iterable))

def filter_by_name(iterable, name):
    return filter_by_names(iterable,[name])

def get_item(iterable, key, item):
    result = filter_by_name(iterable, key)
    assert len(result) == 1
    return result[0][item]

def get_mac(iterable, name):
    return get_item(iterable, name, 'mac')

def get_timezone(iterable, name):
    return get_item(iterable, name, 'timezone')

def get_zero_point(iterable, name):
    return get_item(iterable, name, 'zero_point')

def get_filters(iterable, name):
    return get_item(iterable, name, 'filters')


def do_update_location(url, path, names, dry_run):
    mongo_aux_list = mongo_get_all_info(url)
    mongo_output_list = read_csv(path, LOCATION_HEADER)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_aux_list, row['name'])
        log.info("Updating mongoDB with location info for item %s (%s)", row['name'], mac)
        body = mongo_api_body_location(row, mongo_aux_list)
        if not dry_run:
            mongo_api_update(url, body, mac)
        else:
            print("="*60)
            print(json.dumps(body, indent=2))
            print("="*60)
   
def do_update_photometer(url, path, names, dry_run):
    mongo_aux_list = mongo_get_all_info(url) 
    mongo_output_list = read_csv(options.file, PHOTOMETER_HEADER)
    log.info("read %d items from CSV file %s", len(mongo_output_list), options.file)
    if options.names:
        mongo_output_list = filter_by_names(mongo_output_list, options.names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        oldmac = get_mac(mongo_aux_list, row['name'])
        body = mongo_api_body_photometer(row, mongo_aux_list)
        log.info("Updating MongoDB with photometer info for %s (%s)", row['name'], oldmac)
        if(oldmac != row['mac']):
            log.warn("Changing %s MAC: (%s) -> (%s)", row['name'], oldmac, row['mac'])
        if not dry_run:
            mongo_api_update(url, body, oldmac)
        else:
            print("="*60)
            print(json.dumps(body, indent=2))
            print("="*60)

def do_update_organization(url, path, names, dry_run):
    mongo_input_list = mongo_get_photometer_info(url)
    mongo_output_list = read_csv(options.file, ORGANIZATION_HEADER)
    log.info("read %d items from CSV file %s", len(mongo_output_list), options.file)
    if options.names:
        mongo_output_list = filter_by_names(mongo_output_list, options.names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_input_list, row['name'])
        log.info("Updating mongoDB with organization info for item %s (%s)", row['name'], mac)
        body = mongo_api_body_organization(row)
        if not dry_run:
            mongo_api_update(url, body, mac)
        else:
            print("="*60)
            print(json.dumps(body, indent=2))
            print("="*60)

def do_update_contact(url, path, names, dry_run):
    mongo_input_list = mongo_get_photometer_info(url)
    mongo_output_list = read_csv(options.file, CONTACT_HEADER)
    log.info("read %d items from CSV file %s", len(mongo_output_list), options.file)
    if options.names:
        mongo_output_list = filter_by_names(mongo_output_list, options.names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_input_list, row['name'])
        log.info("Updating mongoDB with contact info for item %s (%s)", row['name'], mac)
        body = mongo_api_body_organization(row)
        if not dry_run:
            mongo_api_update(url, body, mac)
        else:
            print("="*60)
            print(json.dumps(body, indent=2))
            print("="*60)

def do_update_all(url, path, names, dry_run):
    pass

# ===================
# Module entry points
# ===================

def location(options):
    url = get_mongo_api_url()
    if options.list:
        mongo_input_list = mongo_get_location_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if options.names:
            mongo_input_list = filter_by_names(mongo_input_list, options.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        write_csv(mongo_input_list, LOCATION_HEADER, options.file)
    elif options.update:
        do_update_location(url, options.file, options.names, dry_run=False)
    elif options.upd_dry_run:
        do_update_location(url, options.file, options.names, dry_run=True)
    elif options.nominatim:
        mongo_input_list = mongo_get_location_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if options.names:
            mongo_input_list = filter_by_names(mongo_input_list, options.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        log.info("including Nominatim geolocalization metadata")   
        nominatim_list = geolocate(mongo_input_list)
        nominatim_list = list(map(remap_nominatim, nominatim_list))
        mongo_input_list = merge_info(mongo_input_list, nominatim_list)
        write_csv(mongo_input_list, NOMINATIM_HEADER, options.file)
    else:
        log.error("No valid input option to subcommand 'location'")


def photometer(options):
    url = get_mongo_api_url()
    if options.list:
        mongo_input_list = mongo_get_photometer_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if options.names:
            mongo_input_list = filter_by_names(mongo_input_list, options.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        write_csv(mongo_input_list, PHOTOMETER_HEADER, options.file)
    elif options.create:
        mongo_output_list = read_csv(options.file, PHOTOMETER_HEADER)
        log.info("read %d items from CSV file %s", len(mongo_output_list), options.file)
        if options.names:
            mongo_output_list = filter_by_names(mongo_output_list, options.names)
            log.info("filtered up to %d items", len(mongo_output_list))
        for row in mongo_output_list:
            body = mongo_api_body_photometer(row, list(), create=True)
            log.info("Creating new MongoDB entry with photometer info: %s (%s)", row['name'], row['mac'])
            mongo_api_create(url, body)
    elif options.update:
        do_update_photometer(url, options.file, options.names, dry_run=False)
    elif options.upd_dry_run:
        do_update_photometer(url, options.file, options.names, dry_run=True)
    else:
        log.error("No valid input option to subcommand 'photometer'")


def organization(options):
    url = get_mongo_api_url()
    if options.list:
        mongo_input_list = mongo_get_organization_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if options.names:
            mongo_input_list = filter_by_names(mongo_input_list, options.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        write_csv(mongo_input_list, ORGANIZATION_HEADER, options.file)
    elif options.update:
        do_update_organization(url, options.file, options.names, dry_run=False)
    elif options.upd_dry_run:
        do_update_organization(url, options.file, options.names, dry_run=True)
    else:
        log.error("No valid input option to subcommand 'organization'")


def contact(options):
    url = get_mongo_api_url()
    if options.list:
        log.warn("MongoDB does not store contact info")
        mongo_input_list = mongo_get_contact_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if options.names:
            mongo_input_list = filter_by_names(mongo_input_list, options.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        write_csv(mongo_input_list, CONTACT_HEADER, options.file)
    elif options.update:
        do_update_contact(url, options.file, options.names, dry_run=False)
    elif options.upd_dry_run:
        do_update_contact(url, options.file, options.names, dry_run=True)
    else:
        log.error("No valid input option to subcommand 'contact'")


def all(options):
    url = get_mongo_api_url()
    if options.list:
        mongo_input_list = mongo_get_all_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if options.names:
            mongo_input_list = filter_by_names(mongo_input_list, options.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        write_csv(mongo_input_list, ALL_HEADER, options.file)
    elif options.update:
        mongo_input_list = mongo_get_photometer_info(url)
        mongo_output_list = read_csv(options.file, ALL_HEADER)
        log.info("read %d items from CSV file %s", len(mongo_output_list), options.file)
        if options.names:
            mongo_output_list = filter_by_names(mongo_output_list, options.names)
            log.info("filtered up to %d items", len(mongo_output_list))
        for row in mongo_output_list:
            mac = get_mac(mongo_input_list, row['name'])
            log.info("Updating mongoDB with all info for item %s (%s)", row['name'], mac)
            body = mongo_api_body_organization(row)
            mongo_api_update(url, body, mac)
    else:
        log.error("No valid input option to subcommand 'all'")


def check(options):
    log.info(" ====================== ANALIZING DUPLICATES IN MONGODB METADATA ======================")
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_all_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    if options.names:
        mongo_phot = by_photometer(mongo_input_list)
        log_photometers(mongo_phot)
    elif options.coords:
        mongo_place  = by_place(mongo_input_list)
        log_places(mongo_place)
    elif options.places:
        mongo_place  = by_place(mongo_input_list)
        log_places(mongo_place)
    else:
        log.error("No valid input option to subcommand 'duplicate'")
