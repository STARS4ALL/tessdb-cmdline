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
import sys

from importlib.resources import files

#--------------
# local imports
# -------------

from ._version import __version__

# ----------------
# Module constants
# ----------------

# JINJA2 TEMPLATES RESOURCES

CREATE_LOCATIONS_TEMPLATE = files('tessutils.templates').joinpath('location-create.j2')
SQL_INSERT_LOCATIONS_TEMPLATE = files('tessutils.templates').joinpath('sql-location-insert.j2')
SQL_PHOT_NEW_LOCATIONS_TEMPLATE = files('tessutils.templates').joinpath('sql-phot-new-locations.j2')
SQL_PHOT_UPD_LOCATIONS_TEMPLATE = files('tessutils.templates').joinpath('sql-phot-upd-locations.j2')
SQL_PHOT_UPD_META_LOCATIONS_TEMPLATE = files('tessutils.templates').joinpath('sql-phot-upd-locations-metadata.j2')
