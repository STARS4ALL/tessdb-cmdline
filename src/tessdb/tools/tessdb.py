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

PHOTOMETER_TYPE = ('easy', 'repaired', 'renamed', 'complicated')

HEADER_NAME = ('name', 'mac', 'valid_since', 'valid_until', 'contiguous_flag', 'valid_state', 'valid_days')
HEADER_MAC  = ('mac', 'name', 'valid_since', 'valid_until', 'contiguous_flag', 'valid_state', 'valid_days')

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# -------------------------
# Module auxiliar functions
# -------------------------

render = functools.partial(render_from, 'tessutils')


# ================================ BEGIN GOOD REUSABLE FUNCTIONS ============================

def readings_unknown_location(connection, name_mac_list, known_flag, threshold=0):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {'name': row['name'], 'mac': row['mac'], 'threshold': threshold}
        if known_flag:
            cursor.execute('''
                SELECT :name, mac_address, tess_id, t.location_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.location_id = -1
                AND t.location_id > -1 -- known location_id in tess_t !
                GROUP BY tess_id
                HAVING cnt > :threshold
                ''', params)
        else:
            cursor.execute('''
                SELECT :name, mac_address, tess_id, t.location_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.location_id = -1
                GROUP BY tess_id
                HAVING cnt > :threshold
                ''', params)
        temp = [dict(zip(['name','mac','tess_id','location_id','count'],row)) for row in cursor]
        for row in temp: 
            log.info("Unknown location in readings for %s",row)
        result.extend(temp)
    return result

def readings_unknown_observer(connection, name_mac_list, known_flag, threshold=0):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {'name': row['name'], 'mac': row['mac'], }
        if known_flag:
            cursor.execute('''
                SELECT :name, mac_address, tess_id, t.observer_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.observer_id = -1
                AND t.observer_id > -1 -- known observer_id in tess_t !
                GROUP BY tess_id
                HAVING cnt > :threshold
                ''', params)
        else:
            cursor.execute('''
                SELECT :name, mac_address, tess_id, t.observer_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.observer_id = -1
                GROUP BY tess_id
                HAVING cnt > :threshold
                ''', params)
        temp = [dict(zip(['name','mac','tess_id','observer_id','count'],row)) for row in cursor]
        for row in temp: 
            log.info("Unknown observer in readings for %s",row)
        result.extend(temp)
    return result

def photometers_fake_zero_points(connection, name_mac_list, threshold=18.5):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {'name': row['name'], 'mac': row['mac'], 'zp': threshold}
        cursor.execute('''
            SELECT :name, mac_address, tess_id, zp1
            FROM tess_t
            WHERE mac_address = :mac
            AND zp1 < :zp
            ''', params)
        result.extend([dict(zip(['name','mac','tess_id','zp1'],row)) for row in cursor])
    return result

def photometers_location_id(connection, name_mac_list, location_id):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {'name': row['name'], 'mac': row['mac'], 'location_id': location_id}
        cursor.execute('''
            SELECT :name, mac_address, tess_id, location_id 
            FROM tess_t
            WHERE mac_address = :mac
            AND location_id = :location_id
            ''', params)
        temp = [dict(zip(['name','mac','tess_id','location_id'],row)) for row in cursor]
        result.extend(temp)
    return result

def photometers_observer_id(connection, name_mac_list, observer_id):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {'name': row['name'], 'mac': row['mac'], 'observer_id': observer_id}
        cursor.execute('''
            SELECT :name, mac_address, tess_id, observer_id 
            FROM tess_t
            WHERE mac_address = :mac
            AND observer_id = :observer_id
            ''', params)
        temp = [dict(zip(['name','mac','tess_id','observer_id'],row)) for row in cursor]
        result.extend(temp)
    return result

def name_mac_current_history_sql(name):
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

def name_mac_previous_related_history_sql(name):
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

def name_mac_next_related_history_sql(name):
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
        
def name_mac_previous_related_history(connection, start_tstamp, name, mac):
    cursor = connection.cursor()
    history = list()
    uncertain_history = False
    params = {'tstamp': start_tstamp, 'name': name, 'mac': mac}
    sql = name_mac_previous_related_history_sql(name)
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
    sql = name_mac_next_related_history_sql(name)
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
      

def name_mac_current_history(connection, name, mac):
    assert name is not None or mac is not None, f"either name={name} or mac={mac} is None"
    params = {'name': name, 'mac': mac}
    cursor = connection.cursor()
    sql = name_mac_current_history_sql(name)
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


def photometer_classification(args):
    if args.easy:
        return 'easy'
    if args.renamed:
        return 'renamed'
    if args.repaired:
        return 'repaired'
    return 'complicated'

def selected_name_mac_list(connection, classification):
    if classification == 'easy':
        result = photometers_easy(connection)
    elif classification == 'renamed':
        result = photometers_renamed(connection)
    elif classification == 'repaired':
        result = photometers_repaired(connection)
    else:
        result = photometers_complicated(connection)
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
    return [dict(zip(['name','mac','valid_since', 'valid_until','valid_state'],row)) for row in cursor]

def photometers_not_easy(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, mac_address, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address IN (
            -- Photometers with with substitution
            SELECT mac_address FROM name_to_mac_t
            WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            UNION -- This is the renamings part
            SELECT mac_address FROM name_to_mac_t
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
        ORDER BY name, valid_since
    ''')
    return [dict(zip(['name','mac','valid_since', 'valid_until','valid_state'],row)) for row in cursor]


def photometers_repaired(connection):
    output = list()
    for row in photometers_not_easy(connection):
        name = row['name']
        history, break_end_tstamps, break_start_tstamps, truncated = name_mac_current_history(connection, name, mac=None)
        start_tstamp = history[0][2]
        end_tstamp = history[-1][3]
        uncertain_history1, prev_history = name_mac_previous_related_history(connection, start_tstamp, name, mac=None)
        uncertain_history2, next_history = photometer_next_related_history(connection, end_tstamp, name, mac=None)
        pure_repair = len(history) > 1 and len(break_end_tstamps) == 0 and len(prev_history) == 0 and len(next_history) == 0
        if pure_repair:
            output.append(row)
    return output

def photometers_renamed(connection):
    output = list()
    for row in photometers_not_easy(connection):
        mac = row['mac']
        history, break_end_tstamps, break_start_tstamps, truncated = name_mac_current_history(connection, name=None, mac=mac)
        start_tstamp = history[0][2]
        end_tstamp = history[-1][3]
        uncertain_history1, prev_history = name_mac_previous_related_history(connection, start_tstamp, name=None, mac=mac)
        uncertain_history2, next_history = photometer_next_related_history(connection, end_tstamp, name=None, mac=mac)
        pure_renaming = len(history) > 1 and len(break_end_tstamps) == 0 and len(prev_history) == 0 and len(next_history) == 0
        if pure_renaming:
            output.append(row)
    return output

def photometers_complicated(connection):
    total = photometers_not_easy(connection)
    only_repaired = photometers_repaired(connection)
    only_renamed = photometers_renamed(connection)
    keys = total[0].keys()
    total = set( list(zip(*item.items()))[1] for item in total)
    only_repaired = set( list(zip(*item.items()))[1] for item in only_repaired)
    only_renamed = set( list(zip(*item.items()))[1] for item in only_renamed)
    total = list(total - only_repaired - only_renamed)
    output = [dict(zip(keys, item)) for item in total]
    return output

def photometers_with_unknown_location(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    return photometers_location_id(connection, name_mac_list, location_id=-1)

def photometers_with_unknown_observer(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    return photometers_observer_id(connection, name_mac_list, observer_id=-1)

def places_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT longitude, latitude, place, town, sub_region, region, country, timezone, location_id
        FROM location_t 
        ''')
    return [dict(zip(['longitude','latitude','place','town','sub_region','region','country','timezone','name'],row)) for row in cursor]

# ================================ END GOOD REUSABLE FUNCTIONS ===============================




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

def fix(args):
    log.info(" ====================== GENERATE SQL FILES TO FIX TESSDB METADATA ======================")
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("connecting to SQLite database %s", path)
    connection.row_factory = sqlite3.Row
    if args.location_readings:
        fix_location_readings(connection, args.directory)
    else:
        log.error("No valid input option to command 'fix'")

# =============================
# PHOTOMETER 'readings' COMMAND
# =============================

def readings(args):
    log.info("====================== CHECKING PHOTOMETERS METADATA IN TESSDB ======================")
    classification = photometer_classification(args)
    connection, path = open_database(None, 'TESSDB_URL')
    log.info("connecting to SQLite database %s", path)
    name_mac_list = selected_name_mac_list(connection, classification)
    if args.unknown_location:
        result = readings_unknown_location(connection, name_mac_list, args.known)
        log.info("Found %d photometer entries with unknown location in readings and %s location in tess_t", len(result), args.known)
    elif args.unknown_observer:
        result = readings_unknown_observer(connection, name_mac_list, args.known)
        log.info("Found %d photometer entries with unknown observer in readings and %s observer in tess_t", len(result), args.known)
    else:
        log.error("Not implemented option to command 'readings'")

# ==========================
# PHOTOMETER 'check' COMMAND
# ==========================

def check(args):
    log.info("====================== CHECKING PHOTOMETERS METADATA IN TESSDB ======================")
    classification = photometer_classification(args)
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
        check_proper_macs(connection, classification);
    elif args.fake_zero_points:
        log.info("Check for fake Zero Points in tess_t")
        check_fake_zero_points(connection, classification)
    elif args.unknown_location:
        log.info("Check for Unknown Location in tess_t")
        check_photometers_with_unknown_location(connection, classification)
    elif args.unknown_observer:
        log.info("Check for Unknown Observer in tess_t")
        check_photometers_with_unknown_observer(connection, classification)
    else:
        log.error("No valid input option to command 'check'")


def check_photometers_with_unknown_location(connection, classification):
    result = photometers_with_unknown_location(connection, classification)
    log.info("Must update location in %d %s photometers (%d entries)", classification, len(group_by_mac(result)), len(result))

def check_photometers_with_unknown_observer(connection, classification):
    result = photometers_with_unknown_opbserver(connection, classification)
    log.info("Must update observer in %d %s photometers (%d entries)", classification, len(group_by_mac(result)), len(result))

def check_fake_zero_points(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    for row in photometers_fake_zero_points(connection, name_mac_list):
        log.info(row)

def check_proper_macs(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    bad_macs=list()
    for row in name_mac_list:
        try:
            mac = vmac(row['mac'])
        except Exception:
            bad_macs.append(row['mac'])
            log.warn("%s has a bad mac address", row['name'])
    log.info("%d Bad MAC addresses ", len(bad_macs))


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
        output_grp = group_by_name(output)
        if to_console: 
            for name, values in output_grp.items(): log.info("%s => %d entries", name, len(values))
        log.info("Got %d photometers repaired entries", len(output))
        HEADER = ('name','mac','valid_since','valid_until','valid_state')
    elif args.renamed:
        output = photometers_renamed(connection)
        output_grp = group_by_mac(output)
        if to_console:
            for name, values in output_grp.items(): log.info("%s => %d entries", name, len(values))
        log.info("Got %d photometers renamed entries", len(output))
        HEADER = ('mac','name','valid_since','valid_until','valid_state')
    elif args.easy:
        output = photometers_easy(connection)
        if to_console: 
            for item in output: log.info(item)
        log.info("Got %d 'easy' photometers (not repaired, nor renamed entries)", len(output))
        HEADER = ('name','mac','valid_since','valid_until','valid_state')
    elif args.complicated:
        output = photometers_complicated(connection)
        if to_console: 
            for item in output: log.info(item)
        log.info("Got %d really 'complicated' photometers entries (with repairs and renaming entries)", len(output))
        HEADER = ('name','mac','valid_since','valid_until','valid_state')
    else:
        raise ValueError("Unknown option")
    if args.output_file:
        write_csv(args.output_file, HEADER, output)


# ============================
# PHOTOMETER 'history' COMMAND
# ============================

def history(args):
    assert args.name is None or args.mac is None, "Either name or mac addresss should be None"
    name = args.name 
    mac = args.mac
    header = HEADER_NAME if name is not None else HEADER_MAC
    global_history = list()
    global_history.append(header)
    connection, path = open_database(None, 'TESSDB_URL')
    history, break_end_tstamps, break_start_tstamps, truncated = name_mac_current_history(connection, name, mac)
    start_tstamp = history[0][2]
    end_tstamp = history[-1][3]
    uncertain_history1, prev_history = name_mac_previous_related_history(connection, start_tstamp, name, mac)
    uncertain_history2, next_history = photometer_next_related_history(connection, end_tstamp, name, mac)
    global_history.append(('xxxx', 'xxxx', 'valid_since', 'valid_until', 'prev_related', 'valid_state', 'valid_days'))
    global_history.extend(prev_history)
    global_history.append(('xxxx', 'xxxx', 'valid_since', 'valid_until', 'current', 'valid_state', 'valid_days'))
    global_history.extend(history)
    global_history.append(('xxxx', 'xxxx', 'valid_since', 'valid_until', 'next_related', 'valid_state', 'valid_days'))
    global_history.extend(next_history)
    for break_tstamp in break_end_tstamps:
        uncertain_history3, broken_end_history = photometer_next_related_history(connection, break_tstamp, name, mac)
        global_history.append(('xxxx', 'xxxx', 'valid_since', 'valid_until', 'broken_end', 'valid_state', 'valid_days'))
        global_history.extend(broken_end_history)
    for break_tstamp in break_start_tstamps:
        uncertain_history4, broken_start_history = photometer_next_related_history(connection, break_tstamp, name, mac)
        global_history.append(('xxxx', 'xxxx', 'valid_since', 'valid_until', 'broken_start', 'valid_state', 'valid_days'))
        global_history.extend(broken_start_history)

    if args.output_file:
        log.info("%d rows of previous related history", len(prev_history))
        log.info("%d rows of proper history", len(history))
        log.info("Proper history breaks in %d end timestamp points", len(break_end_tstamps))
        log.info("Proper history breaks in %d start timestamp points", len(break_start_tstamps))
        log.info("%d rows of next related history", len(next_history))
        with open(args.output_file,'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            for row in global_history:
                writer.writerow(row)
    else:
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
            for item in broken_end_history: log.info(item)
        for break_tstamp in break_start_tstamps:
            log.info("------------------------------- %s BROKEN START TIMESTAMP RELATED HISTORY " + "-"*38, break_tstamp)
            for item in broken_start_history: log.info(item)
           


# ============================
# PARSER AND MAIN ENTRY POINTS
# ============================

def add_args(parser):

    subparser = parser.add_subparsers(dest='command')

    tdread = subparser.add_parser('readings',  help="TessDB readings check")
    tdex0 = tdread.add_mutually_exclusive_group(required=True)
    tdex0.add_argument('-rn', '--renamed', action='store_true', help='renamed photometers only')
    tdex0.add_argument('-rp', '--repaired', action='store_true',  help='repaired photometers only')
    tdex0.add_argument('-ea', '--easy', action='store_true',  help='"easy" (not repaired nor renamed photometers)')
    tdex0.add_argument('-co', '--complicated', action='store_true',  help='complicated photometers (with repairs AND renamings)')
    tdex1 = tdread.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-ul', '--unknown-location', action='store_true', help='Check unknown location_id in tess_readings_t')
    tdex1.add_argument('-uo', '--unknown-observer', action='store_true', help='Check unknown observer_id in tess_readings_t')
    tdread.add_argument('-k', '--known', action='store_true', help='Select only with known location/observer id in tess_t')
    
    tdcheck = subparser.add_parser('check',  help="Various TESSDB metadata checks")
    tdex0 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex0.add_argument('-rn', '--renamed', action='store_true', help='renamed photometers only')
    tdex0.add_argument('-rp', '--repaired', action='store_true',  help='repaired photometers only')
    tdex0.add_argument('-ea', '--easy', action='store_true',  help='"easy" (not repaired nor renamed photometers)')
    tdex0.add_argument('-co', '--complicated', action='store_true',  help='complicated photometers (with repairs AND renamings)')
    tdex1 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-p', '--places', action='store_true', help='Check same places, different coordinates')
    tdex1.add_argument('-c', '--coords', action='store_true', help='Check same coordinates, different places')
    tdex1.add_argument('-d', '--dupl', action='store_true', help='Check same coordinates, duplicated places')
    tdex1.add_argument('-b', '--nearby', type=float, default=0, help='Check for nearby places, distance in meters')
    tdex1.add_argument('-m', '--macs', action='store_true', help='Check for proper MACS in tess_t table')
    tdex1.add_argument('-z', '--fake-zero-points', action='store_true', help='Check for fake zero points tess_t')
    tdex1.add_argument('-ul', '--unknown-location', action='store_true', help='Check unknown location in tess_t')
    tdex1.add_argument('-uo', '--unknown-observer', action='store_true', help='Check unknown observer in tess_t')
   

    tdfix = subparser.add_parser('fix',  help="Fix TessDB data/metadata")
    tdex1 = tdfix.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-lr', '--location-readings', action='store_true',  help='Fix unknown location readings')
    tdfix.add_argument('-d', '--directory', type=vdir, required=True, help='Directory to place output SQL files')

    tdphot = subparser.add_parser('photometer',  help="TessDB photometers metadata list")
    tdphot.add_argument('-o', '--output-file', type=str,  help='Optional output CSV file for output')
    tdex0 = tdphot.add_mutually_exclusive_group(required=True)
    tdex0.add_argument('-rn', '--renamed', action='store_true', help='renamed photometers only')
    tdex0.add_argument('-rp', '--repaired', action='store_true',  help='repaired photometers only')
    tdex0.add_argument('-ea', '--easy', action='store_true',  help='"easy" (not repaired nor renamed photometers)')
    tdex0.add_argument('-co', '--complicated', action='store_true',  help='complicated photometers (with repairs AND renamings)')

    tdis = subparser.add_parser('history',  help="Single TESSDB photometer history")
    tdis.add_argument('-o', '--output-file', type=str,  help='Optional output CSV file for output')
    grp = tdis.add_mutually_exclusive_group(required=True)
    grp.add_argument('-n', '--name', type=str, help='Photometer name')
    grp.add_argument('-m', '--mac', type=vmac, help='Photometer MAC Address')
  
    

# ================
# MAIN ENTRY POINT
# ================

ENTRY_POINT = {
    'readings': readings,
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
