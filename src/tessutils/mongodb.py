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

from .dbutils import by_location, by_photometer, by_coordinates, log_locations, log_photometers, log_coordinates
from .dbutils import get_mongo_api_url, get_mongo_api_key, geolocate


# ----------------
# Module constants
# ----------------

STARS4ALL_API_KEY = "foobar"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('mongo')

# -------------------------
# Module auxiliar functions
# -------------------------


def _photometers_from_mongo(url):
    url = url + "/photometers_list"
    body = {"token": get_mongo_api_key() }
    response = requests.post(url, json=body).json()
    try:
        response[0] # Single photometer or list
    except:
        response = [response]
    else:
        pass
    return response


def mongo_remap_info(row):
    new_row = dict()
    new_row['name'] = row['name']
    new_row['mac'] = row.get('mac',None)
    new_row["longitude"] = float(row["info_location"]["longitude"])
    new_row["latitude"] = float(row["info_location"]["latitude"])
    new_row["place"] = row["info_location"]["place"]
    new_row["town"] = row["info_location"].get("town")
    new_row["region"] = row["info_location"].get("region")
    new_row["sub_region"] = row["info_location"].get("sub_region")
    new_row["country"] = row["info_location"]["country"]
    tess = row.get("info_tess")
    if(tess):
        new_row["timezone"] = row["info_tess"].get("local_timezone","Etc/UTC")
        new_row["zero_point"] = row["info_tess"].get("zero_point",None)
        new_row["filter"] = row["info_tess"].get("filters",None)
    else:
        new_row["timezone"] = "Etc/UTC"
        new_row["filter"] = None
        new_row["zero_point"] = None

    organization = row.get("info_org")
    if(organization):
        new_row['org_name'] = row["info_org"].get("name")
        new_row['org_email'] = row["info_org"].get("mail")
        new_row['org_descr'] = row["info_org"].get("description")
        new_row['org_web'] = row["info_org"].get("web_url")
        new_row['org_logo'] = row["info_org"].get("logo_url")
    else:
        new_row['org_name'] = None
        new_row['org_email'] = None
        new_row['org_descr'] = None
        new_row['org_web'] = None
        new_row['org_logo'] = None

    contact = row.get("info_contact")
    if (contact):
        new_row['contact_name'] = row["info_contact"].get("name")
        new_row['contact_email'] = row["info_contact"].get("mail")
    else:
        new_row['contact_name'] = None
        new_row['contact_email'] = None
    return new_row


def photometers_from_mongo(url):
    return list(map(mongo_remap_info, _photometers_from_mongo(url)))


def map_proposal(row):
    new_row = dict()
    keys = ["place", "place_type", "town", "town_type", "sub_region", "sub_region_type", "region", "region_type", "country", "timezone", "zipcode"]
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
        fieldnames = ('name', 'mac', 'longitude', 'latitude', 'place', 'proposed_place', 'proposed_place_type', 'town', 'proposed_town', 'proposed_town_type',
            'sub_region', 'proposed_sub_region', 'proposed_sub_region_type', 'region', 'proposed_region', 'proposed_region_type', 
            'country', 'proposed_country', 'timezone', 'proposed_timezone', 'proposed_zipcode',
            'org_name','org_descr','org_web','org_logo','org_email','contact_name','contact_email','zero_point','filter')
        writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()
        for row in iterable:
            writer.writerow(row)
        

def http_body_info_tess(info):
    body = dict()
    token = os.environ.get('STARS4ALL_API_KEY')
    if not token:
        raise KeyError("STARS4ALL_API_KEY environment variable is not defined")
    return {
        "token": token,
        "isNew": False,
        "tess": {
            "name": info['name'],
            "mac": "qq:qq:qq:qq:qq:qq",
            "info_tess": {
                # "zero_point": 20.5,
                "filters": "UV/IR-740",
                "period": 60
            },
        },
    }


def http_body_info_location(info):
    token = os.environ.get('STARS4ALL_API_KEY')
    if not token:
        raise KeyError("STARS4ALL_API_KEY environment variable is not defined")
    return {
        "token": token,
        "isNew": False,
        "tess": {
            "name": info['name'],
            "mac": "qq:qq:qq:qq:qq:qq",
            "info_tess": {
                "local_timezone": info['proposed_timezone'],
            },
            "info_location": {
                "longitude": info['longitude'],
                "latitude": info['latitude'],
                "place": info['proposed_place'],
                "town": info['proposed_town'],
                "sub_region": info['proposed_sub_region'],
                "country": info['proposed_sub_region'],
                "region": info['proposed_region'],
            },
        },
    }


def http_body_info_img(info):
    token = os.environ.get('STARS4ALL_API_KEY')
    if not token:
        raise KeyError("STARS4ALL_API_KEY environment variable is not defined")
    return {
        "token": token,
        "isNew": False,
        "tess": {
            "name": info['name'],
            "mac": "qq:qq:qq:qq:qq:qq",
            "info_img": {
                "urls": [],
            }
        }
    }


def http_body_info_org(info):
    token = os.environ.get('STARS4ALL_API_KEY')
    if not token:
        raise KeyError("STARS4ALL_API_KEY environment variable is not defined")
    return {
        "token": token,
        "isNew": False,
        "tess": {
            "name": info['name'],
            "mac": "qq:qq:qq:qq:qq:qq",
            "info_org": {
                "name": info['info_org_name'],
                "web_url": info['info_org_web_url'],
                "description": info['info_org_description'],
                "logo_url": info['info_org_logo_url'],
                "email": info['info_org_email'],
                "phone": info['info_org_phone'],
            }
        }
    }

# ===================
# Module entry points
# ===================

def locations(options):
    log.info(" ====================== ANALIZING MONGODB LOCATION METADATA ======================")
    url = get_mongo_api_url()
    mongo_input_list = photometers_from_mongo(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_loc  = by_location(mongo_input_list)
    log_locations(mongo_loc)


def photometers(options):
    log.info(" ====================== ANALIZING MONGODB PHOTOMETER METADATA ======================")
    url = get_mongo_api_url()
    mongo_input_list = photometers_from_mongo(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    mongo_phot = by_photometer(mongo_input_list)
    log_photometers(mongo_phot)


def propose(options):
    log.info(" ====================== PROPOSE NEW MONGODB LOCATION METADATA ======================")
    url = get_mongo_api_url()
    mongo_input_list = photometers_from_mongo(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    output = geolocate(mongo_input_list)
    output = list(map(map_proposal,output))
    output = merge_info(mongo_input_list, output)
    log.info("%d entries produced", len(output))
    proposed_location_csv(output, options.output_prefix + ".csv")



def update(options):
    log.info(" ====================== UPDATING MONGODB METADATA ======================")
    url = get_mongo_api_url() + "/photometers"
    with open(options.input_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        mongo_input_list = [row for row in reader]
    log.info("read %d items from CSV file", len(mongo_input_list))
    
    if not options.all:
        mongo_input_list = [row for row in mongo_input_list if row['name'] == options.name]
        log.info("filtered %d items from CSV file containing %s", len(mongo_input_list), options.name)

    for row in mongo_input_list:
        write_url =  url + '/' + row['name'] + '/' + 'qq:qq:qq:qq:qq:qq'
        log.debug("write URL is %s", write_url)
   
    
    #resp = requests.post(url, json=http_body(row))
    #log.info(f"Request response code is {resp.status_code}")
    #resp.raise_for_status()
