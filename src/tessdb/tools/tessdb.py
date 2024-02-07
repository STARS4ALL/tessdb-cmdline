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
import logging
import sqlite3
import functools

# -------------------
# Third party imports
# -------------------

from lica.cli import execute
from lica.validators import vfile, vdir
from lica.jinja2 import render_from
from lica.sqlite import open_database

#--------------
# local imports
# -------------

from .._version import __version__

from .utils import formatted_mac, is_mac, is_tess_mac
from .dbutils import group_by_coordinates, group_by_mac, log_coordinates, log_coordinates_nearby, group_by_place, log_places, log_names

# ----------------
# Module constants
# ----------------

SQL_PHOT_UPD_MAC_ADDRESS = 'sql-phot-upd-mac.j2'
SQL_PHOT_UPD_READINGS_LOCATIONS = 'sql-phot-upd-readings-locations.j2'

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# -------------------------
# Module auxiliar functions
# -------------------------

render = functools.partial(render_from, 'tessutils')

def _get_tessid_with_unknown_locations_in_readings_but_known_current_location(connection, threshold):
    param = {'threshold': threshold}
    cursor = connection.cursor()
    cursor.execute('''
        SELECT mac_address, tess_id, location_id
        FROM tess_t AS t
        WHERE mac_address IN (
            SELECT mac_address FROM tess_readings_t AS r JOIN tess_t AS t USING(tess_id)
            WHERE r.location_id = -1 GROUP BY t.mac_address HAVING COUNT(*) > :threshold
            EXCEPT  
            SELECT mac_address FROM name_to_mac_t WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            EXCEPT  
            SELECT mac_address FROM name_to_mac_t WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1)
        )
        AND location_id >= 0
        ORDER BY mac_address, valid_since;
        ''', param)
    return tuple(dict(row) for row in cursor.fetchall())


def _get_mac_addresses(connection):
    cursor = connection.cursor()
    cursor.execute('SELECT DISTINCT mac_address FROM tess_t AS t')
    return cursor

def fake_zero_points(connection):
    cursor = connection.cursor()
    cursor.execute('''
        SELECT t.mac_address, t.zp1, n.name, n.valid_since, n.valid_until
        FROM tess_t AS t
        JOIN name_to_mac_t AS n USING(mac_address)
        WHERE t.zp1 < 18.5
        AND t.valid_state = 'Current'
        AND n.name LIKE 'stars%'
        ORDER BY mac_address
        ''')
    return cursor

def _raw_photometers_and_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT t.tess_id, n.name, t.mac_address, t.zp1, t.filter1,
        t.valid_since, t.valid_until, t.valid_state,
        t.location_id,
        t.model, t.firmware, t.channel, t.cover_offset, t.fov, t.azimuth, t.altitude,
        t.authorised, t.registered,
        l.contact_name, l.organization, l.contact_email, l.place, l.longitude, l.latitude, 
        l.elevation, l.zipcode, l.town, l.sub_region, l.country, l.timezone
        FROM tess_t AS t
        JOIN location_t AS l   USING (location_id)
        JOIN name_to_mac_t AS n USING (mac_address)
        WHERE name LIKE 'stars%' ORDER BY name ASC
        ''')
    return cursor

def _photometers_and_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name, mac_address, zp1, filter1 
        longitude, latitude, place, town, sub_region, country, timezone,
        contact_name, contact_email,
        organization
        FROM tess_v 
        WHERE valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor

def _photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name, mac_address, zp1, filter1
        FROM tess_v 
        WHERE valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[1])
    except ValueError:
        return None
    new_row['mac'] = formatted_mac(row[1])
    new_row['zero_point'] =row[2]
    new_row['filter'] = row[3]
    new_row['name'] = row[0]
    return new_row


def tessdb_remap_all_info(row):
    new_row = dict()
    try:
        new_row['mac'] = formatted_mac(row[1])
    except ValueError:
         return None
    try:
        new_row['longitude'] = float(row[4]) if row[4] is not None else 0.0
    except ValueError:
        new_row['longitude'] = 0.0
    try:
        new_row['latitude'] = float(row[5]) if row[5] is not None else 0.0
    except ValueError:
        new_row['latitude'] = 0.0
    new_row['name'] = row[0]
    new_row['place'] = row[6]
    new_row["town"] = row[7]
    new_row["sub_region"] = row[8]
    new_row["region"] = None
    new_row["country"] = row[9]
    new_row["timezone"] = row[10]
    new_row["contact_name"] =  row[11]
    new_row["contact_email"] = row[12]
    new_row["org_name"] = row[13]
    new_row['org_email'] = None
    new_row['org_descr'] = None
    new_row['org_web'] = None
    new_row['org_logo'] = None
    return new_row



def photometers_from_tessdb(connection):
    return list(map(tessdb_remap_info, _photometers_from_tessdb(connection)))

def photometers_and_locations_from_tessdb(connection):
    return list(map(tessdb_remap_all_info, _photometers_and_locations_from_tessdb(connection)))

def places_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT longitude, latitude, place, town, sub_region, region, country, timezone, location_id
        FROM location_t 
        ''')
    result = [dict(zip(['longitude','latitude','place','town','sub_region','region','country','timezone','name'],row)) for row in cursor]
    return result

def referenced_photometers(connection, location_id):
     row = {'location_id': location_id}
     cursor = connection.cursor()
     cursor.execute(
        '''
        SELECT COUNT(*) FROM tess_t WHERE location_id = :location_id
        ''', row)
     count = cursor.fetchone()[0]
     return count

def referenced_readings(connection, location_id):
     row = {'location_id': location_id}
     cursor = connection.cursor()
     cursor.execute(
        '''
        SELECT COUNT(*) FROM tess_readings_t WHERE location_id = :location_id
        ''', row)
     count = cursor.fetchone()[0]
     return count

def log_duplicated_coords(connection, coords_iterable):
     for coords, rows in coords_iterable.items():
        if None in coords:
            log.error("entry %s with no coordinates: %s", rows[0]['name'], coords)
        if len(rows) > 1 and all(row['place'] == rows[0]['place'] for row in rows):
            log.error("Coordinates %s has duplicated place names: %s for %s", coords, [row['place'] for row in rows], [row['name'] for row in rows])


def log_detailed_impact(connection, coords_iterable):
    for coords, rows in coords_iterable.items():
        if None in coords:
            continue
        if len(rows) == 1:
            continue
        for row in rows:
            count1 = referenced_photometers(connection, row['name'])
            count2 = referenced_readings(connection, row['name'])
            if count1 == 0 and count2 == 0:
                print("DELETE FROM location_t WHERE location_id = %d;" % row['name']);
            elif count1 != 0 and count2 != 0:
                log.info("[%d] (%s) Ojito con esta location que tiene %d referencias en tess_t y %d en tess_readings_t",
                    row['name'], row['place'], count1, count2)
            elif count1 != 0 :
                log.info("[%d] (%s) Ojito con esta location que tiene %d referencias en tess_t",
                    row['name'], row['place'], count1)
            elif count2 != 0 :
                log.info("[%d] (%s) Ojito con esta location que tiene %d referencias en tess_readings_t",
                    row['name'], row['place'], count2)

def check_fake_zero_points(connection):
    fake_zero_points(connection)
    for mac, zp, name, valid_since, valid_until in fake_zero_points(connection):
        log.info("Photometer %s ZP=%s [%s] (%s - %s) ", formatted_mac(mac), zp, name, valid_since, valid_until)

def check_proper_macs(connection):
    cursor = _raw_photometers_and_locations_from_tessdb(connection)
    bad_macs=list()
    bad_formatted=list() 
    for t in cursor:
        if not is_tess_mac(t[2]):
            log.warn("%d %s (MAC=%s) has not even a good MAC", t[0], t[1], t[2])
            bad_macs.append(t[2])
        elif not is_mac(t[2]):
            good_mac = formatted_mac(t[2])
            log.warn("%d %s (MAC=%s) should be (MAC=%s)", t[0], t[1], t[2], good_mac)
            bad_formatted.append(t[2])
    log.info("%d Bad MAC addresses and %d bad formatted MAC addresses", len(bad_macs), len(bad_formatted))


def fix_mac_addresses(connection, output_dir):
    cursor = _get_mac_addresses(connection)
    bad_macs=list()
    for t in cursor:
        if not is_tess_mac(t[0]):
            log.warn("(MAC=%s) isn't even a good MAC", t[0])
        elif not is_mac(t[0]):
            good_mac = formatted_mac(t[0])
            log.warn("(MAC=%s) should be (MAC=%s)", t[0], good_mac)
            bad_macs.append({'mac': t[0], 'good_mac': good_mac})
    if len(bad_macs) > 0:
        context = {'rows': bad_macs}
        output = render(SQL_PHOT_UPD_MAC_ADDRESS, context)
        output_path = os.path.join(output_dir, "001_upd_mac.sql")
        log.info("generating SQL file %s", output_path)
        with open(output_path, "w") as sqlfile:
            sqlfile.write(output)
    else:
         log.info("No SQL file to generate")
        
def fix_location_readings(connection, output_dir):
    result = _get_tessid_with_unknown_locations_in_readings_but_known_current_location(connection, threshold=0)
    unique_photometers = group_by_mac(result, column_name='mac_address')
    log.info("%d photometers need fixing in tess_readings_t", len(unique_photometers))
    for i, row in enumerate(result,1):
        context = {'row': row}
        output = render(SQL_PHOT_UPD_READINGS_LOCATIONS, context)
        output_path = os.path.join(output_dir, f"{i:03d}_upd_unknown_readings_locations.sql")
        log.info("Photometer %s: generating SQL file for MAC %s", row['mac_address'], output_path)
        with open(output_path, "w") as sqlfile:
            sqlfile.write(output)


# ===================
# Module entry points
# ===================

def check(args):
    log.info("====================== ANALIZING DUPLICATES IN TESSDB METADATA ======================")
    
    log.info("connecting to SQLite database %s", database)
    connection, path = open_database(None, 'TESSDB_URL')
    if args.places:
        log.info("Check for same place, different coordinates")
        tessdb_places  = group_by_place(places_from_tessdb(connection))
        log_places(tessdb_places)
    elif args.coords:
        log.info("Check for same coordinates, different places")
        tessdb_coords  = group_by_coordinates(places_from_tessdb(connection))
        log_coordinates(tessdb_coords)
    elif args.dupl:
        log.info("Check for same coordinates, duplicated places")
        tessdb_coords  = group_by_coordinates(places_from_tessdb(connection))
        log_duplicated_coords(connection, tessdb_coords)
        #log_detailed_impact(connection, tessdb_coords)
    elif args.nearby:
        log.info("Check for nearby places in radius %0.0f meters", args.nearby)
        tessdb_coords  = group_by_coordinates(places_from_tessdb(connection))
        log_coordinates_nearby(tessdb_coords, args.nearby)
    elif args.macs:
        log.info("Check for proper MAC addresses in tess_t")
        check_proper_macs(connection);
    elif args.fake_zero_points:
        log.info("Check for fake Zero Points in tess_t")
        check_fake_zero_points(connection)
    else:
        log.error("No valid input option to subcommand 'check'")

def fix(args):
    log.info(" ====================== GENERATE SQL FILES TO FIX TESSDB METADATA ======================")
    
    log.info("connecting to SQLite database %s", database)
    connection, path = open_database(None, 'TESSDB_URL')
    connection.row_factory = sqlite3.Row
    if args.macs:
        log.info("Fixing bas formatted MAC addresses")
        fix_mac_addresses(connection, args.directory)
    elif args.location_readings:
        fix_location_readings(connection, args.directory)
    else:
        log.error("No valid input option to subcommand 'check'")

def locations(args):
    
    connection, path = open_database(None, 'TESSDB_URL')
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc  = group_by_place(tessdb_input_list)
    log_places(tessdb_loc)
  

def photometers(args):
    
    connection, path = open_database(None, 'TESSDB_URL')
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_phot = group_by_name(tessdb_input_list)
    log_names(tessdb_phot)


def add_args(parser):

    subparser = parser.add_subparsers(dest='command')

    tdloc = subparser.add_parser('locations',  help="TessDB locations metadata check")
    tdloc.add_argument('-d', '--dbase', type=vfile, required=True, help='TessDB database file path')
    tdloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    tdphot = subparser.add_parser('photometer',  help="TessDB photometers metadata check")
    tdphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    tdcheck = subparser.add_parser('check',  help="Various TESSDB metadata checks")
    tdex1 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-p', '--places', action='store_true', help='Check same places, different coordinates')
    tdex1.add_argument('-c', '--coords', action='store_true', help='Check same coordinates, different places')
    tdex1.add_argument('-d', '--dupl', action='store_true', help='Check same coordinates, duplicated places')
    tdex1.add_argument('-b', '--nearby', type=float, default=0, help='Check for nearby places, distance in meters')
    tdex1.add_argument('-m', '--macs', action='store_true', help='Check for proper MACS in tess_t')
    tdex1.add_argument('-z', '--fake-zero-points', action='store_true', help='Check for proper MACS in tess_t')

    tdfix = subparser.add_parser('fix',  help="Fix TessDB data/metadata")
    tdex1 = tdfix.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-m', '--macs', action='store_true', help='generate SQL to fix bad formatted MACS in tess_t')
    tdex1.add_argument('-lr', '--location-readings', action='store_true',  help='Output SQL directory')
    tdfix.add_argument('-d', '--directory', type=vdir, required=True, help='Directory to place output SQL files')

# ================
# MAIN ENTRY POINT
# ================

ENTRY_POINT = {
    'location': locations,
    'photometer': photometers,
    'fix': fix,
    'check': check,
}

def tessdb_db(args):
    func = ENTRY_POINT[args.command]
    func(args)

def main():
    execute(main_func=tessdb_db, 
        add_args_func=add_args, 
        name=__name__, 
        version=__version__,
        description="STARS4ALL MongoDB Utilities"
    )
