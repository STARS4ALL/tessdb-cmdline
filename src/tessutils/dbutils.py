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


def check_location_consistency(place, photometers):
    '''Check for coordinates consistency among phothometers deployed on the same 'place' name'''
    longitudes = set(phot['longitude'] for phot in photometers)
    latitudes = set(phot['latitude'] for phot in photometers)
    if len(longitudes) > 1:
        log.warn("Location %s has different longitudes. %s", place, longitudes)
    if len(latitudes) > 1:
        log.warn("Location %s has different latitudes. %s", place, latitudes)


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
            check_location_consistency(place, photometers)

def log_photometers(photometers_iterable):
    for name, locations in photometers_iterable.items():
        if len(locations) > 1:
            log.info("Photometer %s has %d locations: %s", name, len(locations), [loc['place'] for loc in locations])

def by_photometer(iterable):
    phot = collections.defaultdict(list)
    for row in iterable:
        phot[row['name']].append(row)
    log.info("From %d lines, we have extracted %d different photometers",len(iterable), len(phot.keys()))
    return phot
