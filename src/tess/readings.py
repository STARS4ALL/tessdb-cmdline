# -*- coding: utf-8 -*-

# TESS UTILITY TO PERFORM SOME MAINTENANCE COMMANDS

# ----------------------------------------------------------------------
# Copyright (c) 2014 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import sys
import argparse
import sqlite3
import os
import os.path
import datetime

#--------------
# other imports
# -------------

#--------------
# local imports
# -------------

from . import DEFAULT_DBASE, UNKNOWN, INFINITE_TIME, EXPIRED, CURRENT
from . import OUT_OF_SERVICE, MANUAL, DEFAULT_AZIMUTH, DEFAULT_ALTITUDE
from . import TSTAMP_FORMAT, DEFAULT_START_DATE, DEFAULT_END_DATE

from .utils      import paging

# --------------------
# READINGS SUBCOMMANDS
# --------------------


def readings_unassigned(connection, options):
    cursor = connection.cursor()
    row = {}
    row['count'] = options.count
    cursor.execute(
        '''
        SELECT m.name, i.mac_address, l.site, min(d.sql_date), max(d.sql_date), count(*)
        FROM name_to_mac_t AS m, tess_readings_t AS r
        JOIN tess_t     AS i USING (tess_id)
        JOIN date_t     AS d USING (date_id)
        JOIN location_t AS l USING (location_id)
        WHERE m.mac_address = i.mac_address 
        AND r.location_id < 0
        GROUP BY m.name
        ORDER BY CAST(substr(m.name, 6) as decimal) ASC;
        ''' , row)
    paging(cursor, ["TESS","MAC","Location","Earliest Date","Latest Date","Records"], size=options.count)


def readings_list_name_single(connection, options):
    cursor = connection.cursor()
    row = {}
    row['name']  = options.name
    row['count'] = options.count
    cursor.execute(
        '''
        SELECT (d.sql_date || 'T' || t.time) AS timestamp, :name, i.mac_address, l.site, r.frequency, r.magnitude, r.signal_strength
        FROM tess_readings_t as r
        JOIN date_t     as d USING (date_id)
        JOIN time_t     as t USING (time_id)
        JOIN location_t as l USING (location_id)
        JOIN tess_t     as i USING (tess_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        ORDER BY r.date_id DESC, r.time_id DESC
        LIMIT :count
        ''' , row)
    paging(cursor, ["Timestamp (UTC)","TESS","MAC","Location","Frequency","Magnitude","RSS"], size=options.count)

def readings_list_mac_single(connection, options):
    cursor = connection.cursor()
    row = {}
    row['mac']  = options.mac
    row['count'] = options.count
    cursor.execute(
        '''
        SELECT (d.sql_date || 'T' || t.time) AS timestamp, (SELECT name FROM name_to_mac_t WHERE mac_address == :mac AND valid_state = "Current"), i.mac_address, l.site, r.frequency, r.magnitude, r.signal_strength
        FROM tess_readings_t AS r
        JOIN date_t     as d USING (date_id)
        JOIN time_t     as t USING (time_id)
        JOIN location_t as l USING (location_id)
        JOIN tess_t     as i USING (tess_id)
        WHERE i.mac_address == :mac
        ORDER BY r.date_id DESC, r.time_id DESC
        LIMIT :count
        ''' , row)
    paging(cursor, ["Timestamp (UTC)","TESS","MAC","Location","Frequency","Magnitude","RSS"], size=options.count)
   
def readings_list_all(connection, options):
    cursor = connection.cursor()
    row = {}
    row['count'] = options.count
    cursor.execute(
        '''
        SELECT (d.sql_date || 'T' || t.time) AS timestamp, m.name, i.mac_address, l.site, r.frequency, r.magnitude, r.signal_strength
        FROM name_to_mac_t AS m, tess_readings_t AS r
        JOIN date_t     as d USING (date_id)
        JOIN time_t     as t USING (time_id)
        JOIN location_t as l USING (location_id)
        JOIN tess_t     as i USING (tess_id)
        WHERE i.mac_address == m.mac_address
        ORDER BY r.date_id DESC, r.time_id DESC
        LIMIT :count
        ''', row)
    paging(cursor, ["Timestamp (UTC)","TESS","MAC","Location","Frequency","Magnitude","RSS"], size=options.count)


def readings_list(connection, options):
    if options.name is None and options.mac is None:
        readings_list_all(connection, options)
    elif options.name is not None:
        readings_list_name_single(connection, options)
    else:
        readings_list_mac_single(connection, options)



def readings_adjloc(connection, options):
    row = {}
    row['new_site']   = options.new_site
    row['old_site']   = options.old_site
    row['start_date'] = int(options.start_date.strftime("%Y%m%d%H%M%S"))
    row['end_date']   = int(options.end_date.strftime("%Y%m%d%H%M%S"))
   
    
    cursor = connection.cursor()
    # Test if old and new locations exists and return its Id
    cursor.execute("SELECT location_id FROM location_t WHERE site == :old_site", row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot adjust location readings. Old name site '%s' does not exist." 
            % (options.old_site,) )
    row['old_site_id'] = result[0]
  

    cursor.execute("SELECT location_id FROM location_t WHERE site == :new_site", row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot adjust location readings. New name site '%s' does not exist." 
            % (options.new_site,) )
    row['new_site_id'] = result[0]

    if options.mac is not None:
        row['mac']        = options.mac
        # Find out how many rows to change fro infromative purposes
        cursor.execute(
            '''
            SELECT (SELECT name FROM name_to_mac_t WHERE mac_address == :mac AND valid_state = "Current"), :mac, tess_id, :old_site_id, :new_site_id, MIN(date_id), MAX(date_id), COUNT(*) 
            FROM tess_readings_t
            WHERE location_id == :old_site_id
            AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            AND   tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :mac)
            GROUP BY tess_id
            ''', row)
        paging(cursor,["TESS","MAC", "TESS Id.", "From Loc. Id", "To Loc. Id", "Start Date", "End Date", "Records to change"], size=5)
        if not options.test:
            # And perform the change
            cursor.execute(
                '''
                UPDATE tess_readings_t SET location_id = :new_site_id 
                WHERE location_id == :old_site_id
                AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
                AND   tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :mac)
                ''', row)
    else:
        row['name']       = options.name
        cursor.execute(
            '''
            SELECT :name, i.mac_address , tess_id, :old_site_id, :new_site_id, MIN(date_id), MAX(date_id), COUNT(*) 
            FROM tess_readings_t AS r
            JOIN tess_t AS i USING (tess_id) 
            WHERE r.location_id == :old_site_id
            AND  (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            AND tess_id IN (SELECT tess_id FROM tess_t JOIN name_to_mac_t AS m USING (mac_address) WHERE m.name == :name)
            GROUP BY r.tess_id, r.location_id
            ''', row)
        paging(cursor,["TESS","MAC", "TESS Id.", "From Loc. Id", "To Loc. Id", "Start Date", "End Date", "Records to change"], size=5)
        if not options.test:
            # And perform the change
            cursor.execute(
                '''
                UPDATE tess_readings_t SET location_id = :new_site_id 
                WHERE location_id == :old_site_id
                AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
                AND   tess_id IN (SELECT tess_id FROM tess_t JOIN name_to_mac_t AS m USING (mac_address) WHERE m.name == :name)
                ''', row)

    connection.commit()

def readings_adjins(connection, options):
    row = {}
    row['new_mac']   = options.new
    row['old_mac']   = options.old
    row['state']     = CURRENT
    row['start_date'] = int(options.start_date.strftime("%Y%m%d%H%M%S"))
    row['end_date']   = int(options.end_date.strftime("%Y%m%d%H%M%S"))
    
    cursor = connection.cursor()
    cursor.execute('''
        SELECT tess_id 
        FROM tess_t WHERE mac_address == :new_mac
        AND valid_state = :state
        ''', row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot adjust instrument readings. New instrument '%s' does not exist." 
            % (options.new,) )
    row['new_tess_id'] = result[0]


    # Find out how many rows to change fro infromative purposes
    cursor.execute(
        '''
        SELECT :old_mac, tess_id, :new_mac, :new_tess_id, MIN(date_id), MAX(date_id), COUNT(*) 
        FROM tess_readings_t
        WHERE tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :old_mac)
        AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
        GROUP BY tess_id
        ''', row)
    paging(cursor,["From MAC", "From TESS Id.", "To MAC", "To TESS Id.", "Start Date", "End Date", "Records to change"], size=5)

    if not options.test:
        # And perform the change
        cursor.execute(
            '''
            UPDATE tess_readings_t SET tess_id = :new_tess_id 
            WHERE  tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :old_mac)
            AND (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            ''', row)
        connection.commit()


def readings_purge(connection, options):
    row = {}
    row['site']   = options.location
    row['start_date'] = int(options.start_date.strftime("%Y%m%d%H%M%S"))
    row['end_date']   = int(options.end_date.strftime("%Y%m%d%H%M%S"))
   
    cursor = connection.cursor()
    # Test if location exists and return its Id
    cursor.execute("SELECT location_id FROM location_t WHERE site == :site", row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot adjust location readings. Site '%s' does not exist." 
            % (options.site,) )
    row['site_id'] = result[0]
  
    if options.mac is not None:
        row['mac']        = options.mac
        # Find out how many rows to change fro infromative purposes
        cursor.execute(
            '''
            SELECT (SELECT name FROM name_to_mac_t WHERE mac_address == :mac AND valid_state = "Current"), :mac, tess_id, :site, MIN(date_id), MAX(date_id), COUNT(*)
            FROM tess_readings_t
            WHERE location_id == :site_id
            AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            AND   tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :mac)
            GROUP BY tess_id
            ''', row)
        paging(cursor,["TESS","MAC", "TESS Id.", "Location", "Start Date", "End Date", "Records to delete"], size=5)

        if not options.test:
            # And perform the change
            cursor.execute(
                '''
                DELETE FROM tess_readings_t
                WHERE location_id == :site_id
                AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
                AND   tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :mac)
                ''', row)
    else:
        row['name']       = options.name
        cursor.execute(
            '''
            SELECT :name, i.mac_address , tess_id, :site, MIN(date_id), MAX(date_id), COUNT(*) 
            FROM tess_readings_t AS r
            JOIN tess_t AS i USING (tess_id) 
            WHERE r.location_id == :site_id
            AND  (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            AND tess_id IN (SELECT tess_id FROM tess_t JOIN name_to_mac_t AS m USING (mac_address) WHERE m.name == :name)
            GROUP BY r.tess_id
            ''', row)
        paging(cursor,["TESS","MAC", "TESS Id.", "Location", "Start Date", "End Date", "Records to delete"], size=5)
        if not options.test:
            # And perform the change
            cursor.execute(
                '''
                DELETE FROM tess_readings_t
                WHERE location_id == :site_id
                AND   (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
                AND   tess_id IN (SELECT tess_id FROM tess_t JOIN name_to_mac_t AS m USING (mac_address) WHERE m.name == :name)
                ''', row)
    connection.commit()


def readings_count(connection, options):
    row = {}
    row['start_date'] = int(options.start_date.strftime("%Y%m%d%H%M%S"))
    row['end_date']   = int(options.end_date.strftime("%Y%m%d%H%M%S"))
    cursor = connection.cursor()

    if options.mac is not None:
        row['mac']        = options.mac
        # Find out how many rows to change fro infromative purposes
        cursor.execute(
            '''
            SELECT (SELECT name FROM name_to_mac_t WHERE mac_address == :mac AND valid_state = "Current"), :mac, tess_id, l.site, MIN(date_id), MAX(date_id), COUNT(*)
            FROM tess_readings_t
            JOIN location_t AS l USING (location_id)
            JOIN tess_t AS i USING (tess_id)
            WHERE (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            AND i.mac_address == :mac
            GROUP BY tess_id,  l.location_id
            ''', row)
        paging(cursor,["TESS", "MAC", "TESS Id.", "Location", "Start Date", "End Date", "Records"], size=5)
    else:
        row['name']        = options.name
        cursor.execute(
            '''
            SELECT :name, i.mac_address, tess_id, l.site,  MIN(date_id), MAX(date_id), COUNT(*)
            FROM tess_readings_t
            JOIN location_t AS l USING (location_id)
            JOIN tess_t     AS i USING (tess_id)
            WHERE (date_id*1000000 + time_id) BETWEEN :start_date AND :end_date
            AND i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
            GROUP BY tess_id, l.location_id
            ''', row)
        paging(cursor,["TESS", "MAC", "TESS Id.", "Location", "Start Date", "End Date", "Records"], size=5)
