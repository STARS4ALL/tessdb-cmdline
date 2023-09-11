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
import itertools
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

# -------------------------
# Module auxiliar functions
# -------------------------


def common_A_B_items(iterable_A, iterable_B):
    return set(iterable_A.keys()).intersection(set(iterable_B.keys()))

def in_A_not_in_B(iterable_A, iterable_B):
    return set(iterable_A.keys()) - set(iterable_B.keys())


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

# ----------------------
# Photometers names check
# ----------------------

def log_names(names_iterable):
    for name, rows in names_iterable.items():
        if len(rows) > 1:
            log.warn("Photometer %s has %d places: %s", name, len(rows), [row['place'] for row in rows])
            log.warn("Photometer %s has %d coordinates: %s", name, len(rows), [(row['longitude'],row['latitude']) for row in rows])


def by_name(iterable):
    names = collections.defaultdict(list)
    for row in iterable:
        names[row['name']].append(row)
    log.info("From %d MongoDB entries, we have extracted %d different photometer names",len(iterable), len(names.keys()))
    return names

# ----------------------
# Photometers MACs check
# ----------------------

def by_mac(iterable):
    macs = collections.defaultdict(list)
    for row in iterable:
        macs[row['mac']].append(row)
    log.info("From %d MongoDB entries, we have extracted %d different photometer MACs",len(iterable), len(macs.keys()))
    return macs

def log_macs(macs_iterable):
    for mac, rows in macs_iterable.items():
        if len(rows) > 1:
            log.warn("MAC %s has %d photometer names: %s", mac, len(rows), [row['name'] for row in rows])

# ------------------------
# Photometers Places check
# ------------------------

def by_place(iterable):
    places = collections.defaultdict(list)
    for row in iterable:
        places[row['place']].append(row)
    log.info("From %d MongoDB entries, we have extracted %d different places",len(iterable), len(places.keys()))
    return places


def log_places(places_iterable):
    for place, rows in places_iterable.items():
        if place is None:
            log.warn("No place defined for '%s'",rows[0]['name'])
        elif len(place.lstrip()) != len(place):
            log.warn("Place '%s' has leading spaces", place)
        elif len(place.rstrip()) != len(place):
            log.warn("Place '%s' has trailing spaces", place)
        if len(rows) > 1:
            log.debug("Place %s has %d photometers: %s", place, len(rows), [row['name'] for row in rows])
            check_place_same_coords(place, rows)

def check_place_same_coords(place, rows):
    '''Check for coordinates consistency among phothometers deployed on the same 'place' name'''
    result = False
    longitudes = set(phot['longitude'] for phot in rows)
    latitudes = set(phot['latitude'] for phot in rows)
    if len(longitudes) > 1:
        result = True
        log.warn("Place %s has different %d longitudes. %s -> %s", place, len(longitudes), 
            [phot['longitude'] for phot in rows], [phot['name'] for phot in rows])
    if len(latitudes) > 1:
        result = True
        log.warn("Place %s has different %d latitudes. %s -> %s", place, len(latitudes), 
            [phot['latitude'] for phot in rows], [phot['name'] for phot in rows])
    return result

# ------------------------
# Photometers Coords check
# ------------------------

def by_coordinates(iterable):
    coords = collections.defaultdict(list)
    for row in iterable:
        coords[(row['longitude'],row['latitude'])].append(row)
    log.info("From %d MongoDB entries, we have extracted %d different coordinates",len(iterable), len(coords.keys()))
    return coords


def log_coordinates(coords_iterable):
    '''Check for coordinates consistency among phothometers deployed on the same 'place' name'''
    for coords, rows in coords_iterable.items():
        if None in coords:
            log.error("entry %s with no coordinates: %s", rows[0]['name'], coords)
        if len(rows) > 1 and all(row['name'] == rows[0]['name'] for row in rows):
            log.error("Coordinates %s has %d duplicated photometers: %s", coords, len(rows), [row['name'] for row in rows])
        if len(rows) > 1 and not all(row['place'] == rows[0]['place'] for row in rows):
            log.error("Coordinates %s has different place names: %s for %s", coords, [row['place'] for row in rows], [row['name'] for row in rows])


def log_coordinates_nearby(coords_iterable, limit):
    '''Check for possibly duplicates nearby coordinates/places'''
    coords_seq = tuple(coords_iterable.keys())
    coords_seq = tuple(filter(lambda x: x[0] is not None and x[1] is not None, coords_seq))
    coord_pairs = tuple(itertools.combinations(coords_seq, 2))
    for pair in coord_pairs:
        d = distance(pair[0], pair[1])
        if d <= limit:
            place_a = coords_iterable[pair[0]][0]['place']
            place_b = coords_iterable[pair[1]][0]['place']
            name_a = coords_iterable[pair[0]][0]['name']
            name_b = coords_iterable[pair[1]][0]['name']
            log.warn("Place 1 (%s): '%s' %s vs Place 2 (%s): '%s' %s [%d meters]", name_a, place_a, pair[0], name_b, place_b, pair[1], d)



def distance(coords_A, coords_B):
    '''
    Compute approximate geographical distance (arc) [meters] between two points on Earth
    Accurate for small distances only
    '''
    longitude_A = coords_A[0]
    longitude_B = coords_B[0]
    latitude_A = coords_A[1]
    latitude_B = coords_B[1]
    delta_long = math.radians(longitude_A - longitude_B)
    delta_lat = math.radians(latitude_A - latitude_B)
    mean_lat = math.radians((latitude_A + latitude_B)/2)
    return round(EARTH_RADIUS*math.sqrt(delta_lat**2 + (math.cos(mean_lat)*delta_long)**2),0)
