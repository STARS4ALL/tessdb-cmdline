# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

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

EARTH_RADIUS =  6371000.0 # in meters 

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('dbutils')


def make_remap_location(geolocator, tzfinder):
    def remap_location_func(row):
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
            log.info("proposal: %s -> '%s'  as place name to %s",place_type, metadata[place_type], row['place'])
        else:
            out_row['place'] = None
            out_row['place_type'] = None
            log.warn("still without a valid place name to %s",row['name'])

        for location_type in ('village','town','city','municipality'):
            try:
                out_row['location'] = metadata[location_type]
            except KeyError:
                out_row['location'] = None
                continue
            else:
                break
        for province_type in ('state','province','state_district'):
            try:
                out_row['sub_region'] = metadata[province_type]
            except KeyError:
                out_row['sub_region'] = None
                continue
            else:
                break
        out_row['region']  = metadata.get('state_district',None)
        out_row['zipcode'] = metadata.get('postcode',None)
        out_row['country'] = metadata.get('country',None)
        out_row['timezone'] = tzfinder.timezone_at(lng=row['longitude'], lat=row['latitude'])
        if(row['timezone'] != row['timezone']):
            log.info("Proposal new timezone: %s -> %s", row['timezone'], out_row['timezone'])
        return out_row
    return remap_location_func



def geolocate(iterable):
    geolocator = Nominatim(user_agent="STARS4ALL project")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)
    tzfinder   = TimezoneFinder()
    remap_location = make_remap_location(geolocator, tzfinder)
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


def check_location_same_coords(place, photometers):
    '''Check for coordinates consistency among phothometers deployed on the same 'place' name'''
    result = False
    longitudes = set(phot['longitude'] for phot in photometers)
    latitudes = set(phot['latitude'] for phot in photometers)
    if len(longitudes) > 1:
        result = True
        log.warn("Location %s has different %d longitudes. %s", place, len(longitudes), longitudes)
    if len(latitudes) > 1:
        result = True
        log.warn("Location %s has different %d latitudes. %s", place, len(latitudes), latitudes)
    return result

def check_location_dupl_phot(place, photometers):
    '''Check duplicate entries fro the same place'''
    result = False
    distinct_photometers = set([phot['name'] for phot in photometers])
    if len(distinct_photometers) == 1:
        log.error("Location %s has %d duplicated photometer entries for %s", place, len(photometers), distinct_photometers)
        result = True
    return result 


def by_location(iterable):
    loc = collections.defaultdict(list)
    for row in iterable:
        loc[row['place']].append(row)
    log.info("From %d photometers, we have extracted %d different places",len(iterable), len(loc.keys()))
    return loc


def log_locations(locations_iterable):
    for place, photometers in locations_iterable.items():
        if len(photometers) > 1:
            log.info("Location %s has %d photometers: %s", place, len(photometers), [phot['name'] for phot in photometers])
            check_location_same_coords(place, photometers)
            check_location_dupl_phot(place, photometers)


def filter_dupl_coordinates(iterable):
    output = list()
    loc_iterable = by_location(iterable)
    for place, photometers in loc_iterable.items():
        if len(photometers) > 1 and check_location_same_coords(place, photometers):
            output.extend(photometers)
    return output


def check_photometer_dupl_locations(name, locations):
    distinct_locations = set([loc['place'] for loc in locations])
    if len(distinct_locations) == 1:
        log.error("Photometer %s has %d duplicated location entries for %s", name, len(locations), distinct_locations)


def log_photometers(photometers_iterable):
    for name, locations in photometers_iterable.items():
        if len(locations) > 1:
            log.info("Photometer %s has %d locations: %s", name, len(locations), [loc['place'] for loc in locations])
            check_photometer_dupl_locations(name, locations)


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
    for coords, locations in coords_iterable.items():
        if len(locations) > 1:
            log.info("Coordinates %s has %d locations: %s and photometers: %s", coords, len(locations), [loc['place'] for loc in locations], [loc['name'] for loc in locations])
