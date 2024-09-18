# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import os
import csv
import logging
import functools

# -------------------
# Third party imports
# -------------------
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from lica.cli import execute
from lica.validators import vfile, vdir

# --------------
# local imports
# -------------

from .._version import __version__

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

geolocator = Nominatim(user_agent="STARS4ALL project")
tf = TimezoneFinder()

# -------------------------
# Module auxiliar functions
# -------------------------


# ===================
# Module entry points
# ===================  

def search(args):
    row = dict()
    row['longitude'] = args.longitude
    row['latitude'] = args.latitude
    log.info(
        " ====================== SEARCHING NOMINATIM METADATA ======================")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", language="en")
    address = location.raw['address']
    print(f"################## Geolocating Lat. {row['latitude']} Long. {row['longitude']} ##############")
    log.info("RAW NOMINATIM METADATA IS\n%s",address)
    print("#"*78)
    for location_type in ('village', 'town', 'city', 'municipality'):
        try:
            row['town'] = address[location_type]
        except KeyError:
            row['town'] = "Unknown"
            continue
        else:
            break
    for sub_region in ('province', 'state', 'state_district'):
        try:
            row['sub_region'] = address[sub_region]
        except KeyError:
            row['sub_region'] = "Unknown"
            continue
        else:
            break
    for region in ('state', 'state_district'):
        try:
            row['region'] = address[region]
        except KeyError:
            row['region'] = "Unknown"
            continue
        else:
            break
    row['zipcode'] = address.get('postcode', "Unknown")
    row['country'] = address.get('country', "Unknown")
    row['tzone'] = tf.timezone_at(lng=row['longitude'], lat=row['latitude'])
    log.info(row)


# ================
# MAIN ENTRY POINT
# ================

def add_args(parser):
    # ------------------------------------------
    # Create second level parsers for 'zptess'
    # ------------------------------------------

    subparser = parser.add_subparsers(dest='command')
    nsch = subparser.add_parser(
        'search',  help="Search Nominatim metadata from Coords")
    nsch.add_argument('-lo', '--longitude', type=float, required=True, help='Longitude (degrees)')
    nsch.add_argument('-la', '--latitude', type=float, required=True, help='latitude (degrees)')
  
ENTRY_POINT = {
    'search': search,
}


def main_func(args):
    func = ENTRY_POINT[args.command]
    func(args)


def main():
    execute(main_func=main_func,
            add_args_func=add_args,
            name=__name__,
            version=__version__,
            description="STARS4ALL MongoDB Utilities"
            )
