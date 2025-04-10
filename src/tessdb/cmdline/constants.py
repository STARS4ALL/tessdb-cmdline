#--------------------
# System wide imports
# -------------------

from lica import StrEnum

import datetime

# ----------------
# Module constants
# ----------------

UNKNOWN       = 'Unknown'

EXPIRED       = "Expired"
CURRENT       = "Current"
TSTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

OUT_OF_SERVICE = "Out of Service"

MANUAL         = "Manual"

# Default values for version-controlled attributes
DEFAULT_AZIMUTH  =  0.0 # Degrees, 0.0 = North
DEFAULT_ALTITUDE = 90.0 # Degrees, 90.0 = Zenith

# Default dates whend adjusting in a rwnge of dates
DEFAULT_START_DATE = datetime.datetime(year=2000,month=1,day=1, tzinfo=datetime.timezone.utc)
DEFAULT_END_DATE   = datetime.datetime(year=2999,month=12,day=31, tzinfo=datetime.timezone.utc)

class ObserverType(StrEnum):
	PERSON = "Individual"
	ORGANIZATION = "Organization"

class ValidState(StrEnum):
	EXPIRED = "Expired"
	CURRENT = "Current"