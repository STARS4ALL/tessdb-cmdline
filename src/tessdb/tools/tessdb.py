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
import datetime
import sqlite3
import functools
import fractions

# -------------------
# Third party imports
# -------------------

from lica.cli import execute
from lica.validators import vfile, vdir, vmac
from lica.jinja2 import render_from
from lica.sqlite import open_database
from lica.csv import write_csv

#--------------
# local imports
# -------------

from .._version import __version__

from .utils import formatted_mac, is_mac, is_tess_mac
from .dbutils import group_by_coordinates, group_by_mac, group_by_name, log_coordinates, log_coordinates_nearby, group_by_place, log_places, log_names

# ----------------
# Module constants
# ----------------

SQL_PHOT_UPD_MAC_ADDRESS = 'sql-phot-upd-mac.j2'
SQL_PHOT_UPD_READINGS_LOCATIONS = 'sql-phot-upd-readings-locations.j2'

TSTAMP_FMT = '%Y-%m-%d %H:%M:%S+00:00'

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

def _photometers_and_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT tess_id, name, mac_address, model, firmware, 
        nchannels, zp1, filter1, zp2, filter2, zp3, filter3, zp4, filter4,
        cover_offset, fov, azimuth, altitude,
        longitude, latitude, place, town, sub_region, country, timezone,
        contact_name, contact_email, organization -- This should be removed at some point
        FROM tess_t AS t
        JOIN location_t USING(location_id)
        JOIN name_to_mac_t AS n USING (mac_address) 
        WHERE n.valid_state = 'Current'
        AND t.valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor

def photometers_with_unknown_current_location(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT tess_id, mac_address, name, n.valid_since, n.valid_until
        FROM tess_t AS t
        JOIN name_to_mac_t AS n USING (mac_address)
        WHERE t.valid_state = 'Current'
        AND n.valid_state = 'Current'
        AND location_id = -1
        AND name LIKE 'stars%'
        ORDER BY mac_address
        ''')
    result = [dict(zip(['tess_id','mac','name','valid_since', 'valid_until'],row)) for row in cursor]
    return result

def _photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name, mac_address, zp1, filter1
        FROM tess_t AS t
        JOIN name_to_mac_t AS n USING (mac_address) 
        WHERE t.valid_state = 'Current'
        AND n.valid_state = 'Current'
        AND name LIKE 'stars%'
        ''')
    return cursor


def tess_id_from_mac(connection, mac_address):
    params = {'mac_address': mac_address}
    cursor = connection.cursor()
    cursor.execute('''
        SELECT tess_id, location_id, observer_id
        FROM tess_t
        WHERE mac_address = :mac_address
    ''', params)
    return cursor


def filter_contiguous(values):
    result = all(values[i]['valid_until'] == values[i+1]['valid_since'] for i in range(len(values)-1))
    # Makes sure that we end-up the chain in the far future
    result = result and values[-1]['valid_until'] == '2999-12-31 23:59:59+00:00' 
    return result
   

def filter_all_unknown_id(values, item_key):
    return not all(  all(ide == -1 for ide in value[item_key]) for value in values)

def filter_any_unknown_id(values, item_key):
    return any(  all(ide == -1 for ide in value[item_key]) for value in values)


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
    cursor = _photometers_and_locations_from_tessdb(connection)
    bad_macs=list()
    bad_formatted=list() 
    for t in cursor:
        if not is_tess_mac(t[2]):
            log.warn("Id=%d %s (MAC=%s) has not even a good MAC", t[0], t[1], t[2])
            bad_macs.append(t[2])
        elif not is_mac(t[2]):
            good_mac = formatted_mac(t[2])
            log.warn("Id=%d %s (MAC=%s) should be (MAC=%s)", t[0], t[1], t[2], good_mac)
            bad_formatted.append(t[2])
    log.info("%d Bad MAC addresses and %d bad formatted MAC addresses", len(bad_macs), len(bad_formatted))

def check_repaired(connection):
    log.info("Accesing TESSDB database")
    tessdb_input_list = photometers_repaired(connection)
    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("Repaired photometers: %d", len(tessdb_input_dict))
    valid_names = list()
    for key, values in tessdb_input_dict.items():
        accepted = filter_contiguous(values)
        if accepted:
            valid_names.append(key)
        log.debug("KEY = %s , LEN VALUES = %d, ACCEPTED = %s", key, len(values), accepted)
    # Filter based on true good repairs
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in valid_names}
    log.info("After detecting true pure repairs, the list has %d entries", len(tessdb_input_dict))
    for key, values in tessdb_input_dict.items():
        log.info("---------------------- %s ----------------------", key)
        for item in values:
            mac = item['mac']
            cursor = tess_id_from_mac(connection, mac)
            result = tuple(zip(*cursor))
            item['tess_ids'] = result[0]
            item['location_ids'] = result[1]
            log.info("MAC: %s, TESS IDS: %s LOCATION_IDS: %s", mac, item['tess_ids'], item['location_ids'])
            item['observer_ids'] = result[2]
    tessdb_input_dict = {key: values for key, values in tessdb_input_dict.items() if filter_all_unknown_id(values, 'location_ids')}
    log.info("After purging all -1 location_ids, the list has %d entries", len(tessdb_input_dict))
    log.info("="*64)
    for key, values in tessdb_input_dict.items():
        log.info("---------------------- %s ----------------------", key)
        for item in values:
            mac = item['mac']
            log.info("MAC: %s, TESS IDS: %s LOCATION_IDS: %s", mac, item['tess_ids'], item['location_ids'])
    tessdb_input_dict = {key: values for key, values in tessdb_input_dict.items() if filter_any_unknown_id(values, 'location_ids')}
    log.info("="*64)
    for key, values in tessdb_input_dict.items():
        log.info("---------------------- %s ----------------------", key)
        for item in values:
            mac = item['mac']
            log.info("MAC: %s, TESS IDS: %s LOCATION_IDS: %s", mac, item['tess_ids'], item['location_ids'])


def check_easy(connection):
    log.info("Accesing TESSDB database")
    tessdb_input_list = photometers_easy(connection)
    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("Easy photometers: %d", len(tessdb_input_dict))
    for key, values in tessdb_input_dict.items():
        for item in values:
            mac = item['mac']
            cursor = tess_id_from_mac(connection, mac)
            result = tuple(zip(*cursor))
            item['tess_ids'] = result[0]
            item['location_ids'] = result[1]
            log.info("%s MAC: %s, TESS IDS: %s LOCATION_IDS: %s",key, mac, item['tess_ids'], item['location_ids'])
            item['observer_ids'] = result[2]
    tessdb_input_dict = {key: values for key, values in tessdb_input_dict.items() if filter_any_unknown_id(values, 'location_ids')}
    log.info("="*64)
    for key, values in tessdb_input_dict.items():
        for item in values:
            mac = item['mac']
            log.info("%s MAC: %s, TESS IDS: %s LOCATION_IDS: %s", key, mac, item['tess_ids'], item['location_ids'])
   


def check_renamings(connection):
    log.info("Accesing TESSDB database")
    tessdb_input_list = photometers_renamed(connection)
    tessdb_input_dict = group_by_mac(tessdb_input_list)
    log.info("Renamed photometers: %d", len(tessdb_input_dict))
    valid_macs = list()
    for key, values in tessdb_input_dict.items():
        accepted = filter_contiguous(values)
        if accepted:
            valid_macs.append(key)
        log.debug("KEY = %s , LEN VALUES = %d, ACCEPTED = %s", key, len(values), accepted)
    # Filter based on true good renamings
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in valid_macs}
    log.info("After detecting true pure renamings, the list has %d entries", len(tessdb_input_dict))
    for key, values in tessdb_input_dict.items():
        log.info("---------------------- %s ----------------------", key)
        for item in values:
            name = item['name']
            cursor = tess_id_from_mac(connection, key)
            result = tuple(zip(*cursor))
            item['tess_ids'] = result[0]
            item['location_ids'] = result[1]
            log.info("NAME: %s, TESS IDS: %s LOCATION_IDS: %s", name, item['tess_ids'], item['location_ids'])
            item['observer_ids'] = result[2]
    tessdb_input_dict = {key: values for key, values in tessdb_input_dict.items() if filter_any_unknown_id(values, 'location_ids')}
    log.info("="*64)
    for key, values in tessdb_input_dict.items():
        log.info("---------------------- %s ----------------------", key)
        for item in values:
            name = item['name']
            log.info("NAME: %s, TESS IDS: %s LOCATION_IDS: %s", name, item['tess_ids'], item['location_ids'])
   



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


def check_photometers_with_unknown_location(connection):
    result = photometers_with_unknown_current_location(connection)
    result = group_by_name(result)
    for name, values in result.items():
        tess_ids = [item['tess_id'] for item in values]
        macs = [item['mac'] for item in values]
        log.info("NAME %s => %s %s", name, macs, tess_ids )
    log.info("%d Photometer entries in tess_t with unknown current location", len(result))



# ===================
# Module entry points
# ===================

def check(args):
    log.info("====================== ANALIZING DUPLICATES IN TESSDB METADATA ======================")
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("connecting to SQLite database %s", path)
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
    elif args.unknown_location:
        log.info("Check for Unknown Location in tess_t")
        check_photometers_with_unknown_location(connection)
    elif args.unknown_observer:
        log.info("Check for Unknown Location in tess_t")
        check_unknown_observer(connection)
    elif args.renamings:
        log.info("Check for Unknown Location in tess_t")
        check_renamings(connection)
    elif args.repaired:
        log.info("Check for Unknown Location in tess_t")
        check_repaired(connection)
    elif args.easy:
        log.info("Check for Unknown Location in tess_t")
        check_easy(connection)
    else:
        log.error("No valid input option to subcommand 'check'")

def fix(args):
    log.info(" ====================== GENERATE SQL FILES TO FIX TESSDB METADATA ======================")
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("connecting to SQLite database %s", path)
    connection.row_factory = sqlite3.Row
    if args.macs:
        log.info("Fixing bas formatted MAC addresses")
        fix_mac_addresses(connection, args.directory)
    elif args.location_readings:
        fix_location_readings(connection, args.directory)
    else:
        log.error("No valid input option to subcommand 'check'")

def locations(args):
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("connecting to SQLite database %s", path)
    tessdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tessdb_input_list))
    tessdb_loc  = group_by_place(tessdb_input_list)
    log_places(tessdb_loc)

# ===============================
# PHOTOMETER 'photometer' COMMAND
# ===============================

def photometers(args):
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("connecting to SQLite database %s", path)
    to_console = args.output_file is None
    if args.repaired:
        output = photometers_repaired(connection)
        if to_console: 
            for item in output: log.info(item)
        log.info("Got %d photometers repaired entries", len(output))
        HEADER = ('name','mac','valid_since','valid_until','valid_state')
    elif args.renamed:
        output = photometers_renamed(connection)
        if to_console: 
            for item in output: log.info(item)
        log.info("Got %d photometers renamed entries", len(output))
        HEADER = ('mac','name','valid_since','valid_until','valid_state')
    elif args.easy:
        output = photometers_easy(connection)
        if to_console: 
            for item in output: log.info(item)
        log.info("Got %d 'easy' photometers (not repaired, nor renamed entries)", len(output))
        HEADER = ('name','mac','valid_since','valid_until','valid_state')
    else:
        raise ValueError("Unkown option")
    if args.output_file:
        write_csv(args.output_file, HEADER, output)


def photometers_repaired(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, mac_address, valid_since, valid_until, valid_state 
        FROM name_to_mac_t 
        WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
        ORDER BY name, valid_since
    ''')
    result = [dict(zip(['name','mac','valid_since', 'valid_until','valid_state'],row)) for row in cursor]
    return result

def photometers_renamed(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, mac_address, valid_since, valid_until, valid_state 
        FROM name_to_mac_t 
        WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1)
        ORDER BY mac_address, valid_since
    ''')
    result = [dict(zip(['name','mac','valid_since', 'valid_until','valid_state'],row)) for row in cursor]
    return result

def photometers_easy(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, mac_address, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address IN (
            -- Photometers with no repairs and no renamings
            SELECT mac_address  FROM name_to_mac_t
            WHERE name LIKE 'stars%'
            EXCEPT -- this is the photometer substitution/repair part
            SELECT mac_address FROM name_to_mac_t
            WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            EXCEPT -- This is the renamings part
            SELECT mac_address FROM name_to_mac_t
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
        ORDER BY name, valid_since
    ''')
    result = [dict(zip(['name','mac','valid_since', 'valid_until','valid_state'],row)) for row in cursor]
    return result

# ============================
# PHOTOMETER 'history' COMMAND
# ============================

def photometer_current_history_sql(name):
    if name is not None:
        sql = '''
            SELECT name, mac_address, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE name = :name
            ORDER BY valid_since
        '''
    else:
        sql = '''
            SELECT mac_address, name, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE mac_address = :mac
            ORDER BY valid_since
        '''
    return sql

def photometer_previous_related_history_sql(name):
    if name is not None:
        sql = '''
            SELECT name, mac_address, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_until = :tstamp
            ORDER BY valid_since
        '''
    else:
        sql = '''
            SELECT mac_address, name, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_until = :tstamp
            ORDER BY valid_since
        '''
    return sql

def photometer_next_related_history_sql(name):
    if name is not None:
        sql = '''
            SELECT name, mac_address, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_since = :tstamp
            ORDER BY valid_since
        '''
    else:
        sql = '''
            SELECT mac_address, name, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_since = :tstamp
            ORDER BY valid_since
        '''
    return sql
        
def photometer_previous_related_history(connection, start_tstamp, name, mac):
    cursor = connection.cursor()
    history = list()
    uncertain_history = False
    params = {'tstamp': start_tstamp, 'name': name, 'mac': mac}
    sql = photometer_previous_related_history_sql(name)
    while True:
        cursor.execute(sql, params)
        fragment = cursor.fetchall()
        L = len(fragment)
        if L == 0:
            break
        elif L > 1:
            uncertain_history = True
            history.extend(fragment)
            break
        else:
            history.extend(fragment)
            tstamp =  fragment[0][2]
            params = {'tstamp': tstamp} 
    history.reverse()
    history = [list(item) for item in history]
    return uncertain_history, history


def photometer_next_related_history(connection, end_tstamp, name, mac):
    cursor = connection.cursor()
    history = list()
    uncertain_history = False
    params = {'tstamp': end_tstamp, 'name': name, 'mac': mac}
    sql = photometer_next_related_history_sql(name)
    while True:
        cursor.execute(sql, params)
        fragment = cursor.fetchall()
        L = len(fragment)
        if L == 0:
            break
        elif L > 1:
            uncertain_history = True
            history.extend(fragment)
            break
        else:
            history.extend(fragment)
            tstamp =  fragment[0][3]
            params = {'tstamp': tstamp}
    history = [list(item) for item in history]
    return uncertain_history, history
      

def photometer_history(connection, name, mac):
    assert name is not None or mac is not None, f"either name={name} or mac={mac} is None"
    params = {'name': name, 'mac': mac}
    cursor = connection.cursor()
    sql = photometer_current_history_sql(name)
    cursor.execute(sql, params)
    history = [list(item) for item in cursor.fetchall()]
    break_end_tstamps = list()
    break_start_tstamps = list()
    for i in range(len(history)-1):
        if history[i][3] != history[i+1][2]:
            history[i][4] = '-'
            break_end_tstamps.append(history[i][3])
            break_start_tstamps.append(history[i+1][2])
    truncated = history[-1][5] == 'Expired'
    return history, break_end_tstamps, break_start_tstamps, truncated

def history(args):
    connection, path = open_database(None, 'TESSDB_URL')
    name = args.name ; 
    mac = args.mac
    history, break_end_tstamps, break_start_tstamps, truncated = photometer_history(connection, name, mac)
    start_tstamp = history[0][2]
    end_tstamp = history[-1][3]
    uncertain_history1, prev_history = photometer_previous_related_history(connection, start_tstamp, name, mac)
    uncertain_history2, next_history = photometer_next_related_history(connection, end_tstamp, name, mac)
    if prev_history and uncertain_history1:
        tag = "UNCERTAIN"
    elif prev_history:
        tag = ""
    else:
        tag = "NO"
    log.info("------------------------------- %s PREVIOUS RELATED HISTORY " + "-"*75, tag)
    for item in prev_history: log.info(item)
    if len(break_end_tstamps) == 0:
        tag = "CONTIGUOUS"
    else:
        tag = "NON CONTIGUOUS"
    log.info("=============================== %s %9s HISTORY BEGINS " + "="*63, tag, name)
    for item in history: log.info(item)
    log.info("=============================== %s %9s HISTORY ENDS   " + "="*63, tag, name)
    if next_history and uncertain_history2:
        tag = "UNCERTAIN"
    elif next_history:
        tag = ""
    else:
        tag = "NO"
    log.info("------------------------------- %s NEXT RELATED HISTORY " + "-"*79, tag)
    for item in next_history: log.info(item)
    for break_tstamp in break_end_tstamps:
        log.info("------------------------------- %s BROKEN END TIMESTAMP RELATED HISTORY " + "-"*40, break_tstamp)
        uncertain_history, middle_history = photometer_next_related_history(connection, break_tstamp, name, mac)
        for item in middle_history: log.info(item)
    for break_tstamp in break_start_tstamps:
        log.info("------------------------------- %s BROKEN START TIMESTAMP RELATED HISTORY " + "-"*38, break_tstamp)
        uncertain_history, middle_history = photometer_next_related_history(connection, break_tstamp, name, mac)
        for item in middle_history: log.info(item)

# ============================
# PARSER AND MAIN ENTRY POINTS
# ============================

def add_args(parser):

    subparser = parser.add_subparsers(dest='command')

    tdloc = subparser.add_parser('locations',  help="TessDB locations metadata check")
    tdloc.add_argument('-d', '--dbase', type=vfile, required=True, help='TessDB database file path')
    tdloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    tdcheck = subparser.add_parser('check',  help="Various TESSDB metadata checks")
    tdex1 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-p', '--places', action='store_true', help='Check same places, different coordinates')
    tdex1.add_argument('-c', '--coords', action='store_true', help='Check same coordinates, different places')
    tdex1.add_argument('-d', '--dupl', action='store_true', help='Check same coordinates, duplicated places')
    tdex1.add_argument('-b', '--nearby', type=float, default=0, help='Check for nearby places, distance in meters')
    tdex1.add_argument('-m', '--macs', action='store_true', help='Check for proper MACS in tess_t')
    tdex1.add_argument('-z', '--fake-zero-points', action='store_true', help='Check for proper MACS in tess_t')
    tdex1.add_argument('-ul', '--unknown-location', action='store_true', help='Check unknown location in tess_t')
    tdex1.add_argument('-uo', '--unknown-observer', action='store_true', help='Check unknown observer in tess_t')
    tdex1.add_argument('-rn', '--renamings', action='store_true', help='Check renamed photometers')
    tdex1.add_argument('-rp', '--repaired', action='store_true', help='Check reparied photometers')
    tdex1.add_argument('-ea', '--easy', action='store_true', help='Check "easy" photometers')

    tdfix = subparser.add_parser('fix',  help="Fix TessDB data/metadata")
    tdex1 = tdfix.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-m', '--macs', action='store_true', help='generate SQL to fix bad formatted MACS in tess_t')
    tdex1.add_argument('-lr', '--location-readings', action='store_true',  help='Output SQL directory')
    tdfix.add_argument('-d', '--directory', type=vdir, required=True, help='Directory to place output SQL files')

    tdphot = subparser.add_parser('photometer',  help="TessDB photometers metadata check")
    tdphot.add_argument('-o', '--output-file', type=str,  help='Optional output file prefix for the different files to generate')
    tdex0 = tdphot.add_mutually_exclusive_group(required=True)
    tdex0.add_argument('-rn', '--renamed', action='store_true', help='List renamed photometers to CSV')
    tdex0.add_argument('-rp', '--repaired', action='store_true',  help='List repaired photometers to CSV')
    tdex0.add_argument('-ea', '--easy', action='store_true',  help='List not repaired or renamed photometers to CSV')

    tdis = subparser.add_parser('history',  help="Single TESSDB photometer history")
    grp = tdis.add_mutually_exclusive_group(required=True)
    grp.add_argument('-n', '--name', type=str, help='Photometer name')
    grp.add_argument('-m', '--mac', type=vmac, help='Photometer MAC Address')
  
    

# ================
# MAIN ENTRY POINT
# ================

ENTRY_POINT = {
    'location': locations,
    'photometer': photometers,
    'fix': fix,
    'check': check,
    'history': history,
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
