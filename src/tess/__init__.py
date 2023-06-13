# ----------------------------------------------------------------------
# Copyright (c) 2014 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import datetime

# ---------------
# Twisted imports
# ---------------

#--------------
# local imports
# -------------

from ._version import get_versions


# -----------------------
# Module global variables
# -----------------------

DEFAULT_DBASE = "/var/dbase/tess.db"

UNKNOWN       = 'Unknown'

INFINITE_TIME = "2999-12-31T23:59:59"
EXPIRED       = "Expired"
CURRENT       = "Current"
TSTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

OUT_OF_SERVICE = "Out of Service"

MANUAL         = "Manual"

# Default values for version-controlled attributes
DEFAULT_AZIMUTH  =  0.0 # Degrees, 0.0 = North
DEFAULT_ALTITUDE = 90.0 # Degrees, 90.0 = Zenith

# Default dates whend adjusting in a rwnge of dates
DEFAULT_START_DATE = datetime.datetime(year=2000,month=1,day=1)
DEFAULT_END_DATE   = datetime.datetime(year=2999,month=12,day=31)


__version__ = get_versions()['version']



del get_versions
