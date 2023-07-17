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
import math
import logging
import collections

# -------------------
# Third party imports
# -------------------

from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

#--------------
# local imports
# -------------

# ----------------
# Module constants
# ----------------

EARTH_RADIUS =  6371009.0 # in meters 

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('dbutils')

def get_mongo_api_url():
    url = os.environ.get("STARS4ALL_API")
    if not url:
        raise KeyError("'STARS4ALL_API' environment variable not set")
    return url

def get_mongo_api_key():
    token = os.environ.get("STARS4ALL_API_KEY")
    if not token:
        raise KeyError("'STARS4ALL_API_KEY' environment variable not set")
    return token


def distance(row1, row2):
    '''
    Compute approximate geographical distance between 
    two points on Earth
    Accurate for small distances only
    '''
    delta_long = math.radians(row1['longitude'] - row2['longitude'])
    delta_lat = math.radians(row1['latitude'] - row2['latitude'])
    mean_lat = math.radians((row1['latitude'] + row2['latitude'])/2)

    return EARTH_RADIUS*math.sqrt(delta_lat**2 + (math.cos(mean_lat)*delta_long)**2)



def _make_remap_location(geolocator, tzfinder):
    def _remap_location_func(row):
        location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", language="en")
        metadata = location.raw['address']
        out_row = dict()
        out_row['name'] = row['name'] # Photometer name (this may go away ....)
        out_row['latitude']  = row['latitude']
        out_row['longitude'] = row['longitude']
        found = False
        for place_type in ('leisure', 'amenity', 'tourism', 'building', 'road', 'hamlet',):
            try:
                out_row['place'] = metadata[place_type]
            except KeyError:
                continue   
            else:
                found = True
                if place_type == 'road' and metadata.get('house_number'):
                    out_row['place'] = metadata[place_type] + ", " + metadata['house_number']
                    out_row['place_type'] = 'road + house_number'
                else:
                    out_row['place_type'] = place_type
                break
        if found:
            log.info("proposal: '%s' (%s)  as place name to '%s'", metadata[place_type], place_type, row['place'])
        else:
            out_row['place'] = None
            out_row['place_type'] = None
            log.warn("still without a valid place name to '%s'",row['name'])

        for location_type in ('village','town','city','municipality'):
            try:
                out_row['town'] = metadata[location_type]
                out_row['town_type'] = location_type
            except KeyError:
                out_row['town'] = None
                out_row['town_type'] = None
                continue
            else:
                break
        for province_type in ('state_district','province'):
            try:
                out_row['sub_region'] = metadata[province_type]
                out_row['sub_region_type'] = province_type
            except KeyError:
                out_row['sub_region'] = None
                out_row['sub_region_type'] = None
                continue
            else:
                break
        out_row['region']  = metadata.get('state',None)
        out_row['region_type'] = 'state'
        out_row['zipcode'] = metadata.get('postcode',None)
        out_row['country'] = metadata.get('country',None)
        out_row['timezone'] = tzfinder.timezone_at(lng=row['longitude'], lat=row['latitude'])
        if(row['timezone'] != row['timezone']):
            log.info("Proposal new timezone: %s -> %s", row['timezone'], out_row['timezone'])
        return out_row
    return _remap_location_func



def geolocate(iterable):
    geolocator = Nominatim(user_agent="STARS4ALL project")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)
    tzfinder   = TimezoneFinder()
    remap_location = _make_remap_location(geolocator, tzfinder)
    return list(map(remap_location, iterable))
    



# -------------------------
# Module auxiliar functions
# -------------------------

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


def check_place_same_coords(place, photometers):
    '''Check for coordinates consistency among phothometers deployed on the same 'place' name'''
    result = False
    longitudes = set(phot['longitude'] for phot in photometers)
    latitudes = set(phot['latitude'] for phot in photometers)
    if len(longitudes) > 1:
        result = True
        log.warn("Place %s has different %d longitudes. %s", place, len(longitudes), longitudes)
    if len(latitudes) > 1:
        result = True
        log.warn("Place %s has different %d latitudes. %s", place, len(latitudes), latitudes)
    return result

def check_place_dupl_phot(place, photometers):
    '''Check duplicate entries fro the same place'''
    result = False
    distinct_photometers = set([phot['name'] for phot in photometers])
    if len(distinct_photometers) == 1:
        log.error("Place %s has %d duplicated photometer entries for %s", place, len(photometers), distinct_photometers)
        result = True
    return result 


def by_place(iterable):
    loc = collections.defaultdict(list)
    for row in iterable:
        loc[row['place']].append(row)
    log.info("From %d photometers, we have extracted %d different places",len(iterable), len(loc.keys()))
    return loc


def log_places(locations_iterable):
    for place, photometers in locations_iterable.items():
        if len(photometers) > 1:
            log.info("Place %s has %d photometers: %s", place, len(photometers), [phot['name'] for phot in photometers])
            check_place_same_coords(place, photometers)
            check_place_dupl_phot(place, photometers)


def filter_dupl_coordinates(iterable):
    output = list()
    loc_iterable = by_place(iterable)
    for place, photometers in loc_iterable.items():
        if len(photometers) > 1 and check_place_same_coords(place, photometers):
            output.extend(photometers)
    return output


def check_photometer_dupl_places(name, places):
    distinct_places = set([place['place'] for place in places])
    if len(distinct_places) == 1:
        log.error("Photometer %s has %d duplicated places entries for %s", name, len(places), distinct_places)


def log_photometers(photometers_iterable):
    for name, places in photometers_iterable.items():
        if len(places) > 1:
            log.info("Photometer %s has %d places: %s", name, len(places), [place['place'] for place in places])
            check_photometer_dupl_places(name, places)


def by_photometer(iterable):
    phot = collections.defaultdict(list)
    for row in iterable:
        phot[row['name']].append(row)
    log.info("From %d lines, we have extracted %d different photometers",len(iterable), len(phot.keys()))
    return phot

def by_coordinates(iterable):
    coords = collections.defaultdict(list)
    for row in iterable:
        coords[(row['longitude'],row['latitude'])].append(row)
    log.info("From %d photometers, we have extracted %d different coordinates",len(iterable), len(coords.keys()))
    return coords

def log_coordinates(coords_iterable):
    for coords, places in coords_iterable.items():
        if len(places) > 1:
            log.info("Coordinates %s has %d places: %s and photometers: %s", coords, len(places), [pla['place'] for pla in places], [pla['name'] for pla in places])
