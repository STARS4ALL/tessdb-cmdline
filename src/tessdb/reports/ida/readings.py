
# GENERATE IDA-FORMAT OBSERVATIONS FILE

# ----------------------------------------------------------------------
# Copyright (c) 2017 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import logging

# ----------------
# Other librarires
# ----------------

#--------------
# local imports
# -------------

from .. import  TSTAMP_FORMAT

from .metadata import tess_type

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)


# -----------------------
# Module global functions
# -----------------------
    

def available(name, month, connection):
    instrument_type = tess_type(name, connection)
    if instrument_type is None:
        raise ValueError(f"Could not find: {name}")
    if instrument_type[0] == 'TESS-W':
        log.debug("[%s] photometer is a TESS-W", name)
        return available_tessw(name, month, connection)
    elif instrument_type[0] == 'TESS4C':
        log.debug("[%s] photometer is a TESS4C", name)
        return available_tess4c(name, month, connection)
    else:
        raise NotImplementedError(f"Unknown photometer model: {instrument_type}")

def available_tessw(name, month, connection):
    '''Return a count of readings related to this name, 
    grouped by location'''
    row = {'name': name, 'from_date': month.strftime(TSTAMP_FORMAT)}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT COUNT (*), r.location_id, l.place
        FROM tess_readings_t AS r
        JOIN date_t          AS d USING (date_id)
        JOIN time_t          AS t USING (time_id)
        JOIN tess_t          AS i USING (tess_id)
        JOIN location_t      AS l USING(location_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        AND     datetime(d.sql_date || 'T' || t.time || '.000') 
        BETWEEN datetime(:from_date) 
        AND     datetime(:from_date, '+1 month')
        GROUP BY r.location_id
        ''', row)
    return cursor.fetchall()


def available_tess4c(name, month, connection):
    '''Return a count of readings related to this name, 
    grouped by location'''
    row = {'name': name, 'from_date': month.strftime(TSTAMP_FORMAT)}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT COUNT (*), r.location_id, l.place
        FROM tess_readings4c_t AS r
        JOIN date_t          AS d USING (date_id)
        JOIN time_t          AS t USING (time_id)
        JOIN tess_t          AS i USING (tess_id)
        JOIN location_t      AS l USING(location_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        AND     datetime(d.sql_date || 'T' || t.time || '.000') 
        BETWEEN datetime(:from_date) 
        AND     datetime(:from_date, '+1 month')
        GROUP BY r.location_id
        ''', row)
    return cursor.fetchall()


def fetch(name, month, location_id, connection):
    instrument_type = tess_type(name, connection)
    if instrument_type is None:
        raise ValueError(f"Could not find: {name}")
    if instrument_type[0] == 'TESS-W':
        log.debug("[%s] photometer is a TESS-W", name)
        return fetch_tessw(name, month, location_id, connection)
    elif instrument_type[0] == 'TESS4C':
        log.debug("[%s] photometer is a TESS4C", name)
        return fetch_tess4c(name, month, location_id, connection)
    else:
        raise NotImplementedError(f"Unknown photometer model: {instrument_type}")



def fetch_tessw(name, month, location_id, connection):
    '''From start of month at midday UTC'''
    row = {'name': name, 'location_id': location_id, 'from_date': month.strftime(TSTAMP_FORMAT)}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT (d.sql_date || 'T' || t.time || '.000') AS timestamp, r.box_temperature, r.sky_temperature, r.frequency, r.magnitude, i.zp1,  r.sequence_number
        FROM tess_readings_t as r
        JOIN date_t     as d USING (date_id)
        JOIN time_t     as t USING (time_id)
        JOIN tess_t     as i USING (tess_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        AND r.location_id == :location_id
        AND datetime(timestamp) BETWEEN datetime(:from_date) 
                                AND     datetime(:from_date, '+1 month')
        ORDER BY r.date_id ASC, r.time_id ASC
        ''', row)
    return cursor

def fetch_tess4c(name, month, location_id, connection):
    '''From start of month at midday UTC'''
    row = {'name': name, 'location_id': location_id, 'from_date': month.strftime(TSTAMP_FORMAT)}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT (d.sql_date || 'T' || t.time || '.000') AS timestamp, r.box_temperature, r.sky_temperature, 
        r.freq1, r.mag1, i.zp1, r.freq2, r.mag2, i.zp2, r.freq3, r.mag3, i.zp3, r.freq4, r.mag4, i.zp4, r.sequence_number
        FROM tess_readings4c_t as r
        JOIN date_t     as d USING (date_id)
        JOIN time_t     as t USING (time_id)
        JOIN tess_t     as i USING (tess_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        AND r.location_id == :location_id
        AND datetime(timestamp) BETWEEN datetime(:from_date) 
                                AND     datetime(:from_date, '+1 month')
        ORDER BY r.date_id ASC, r.time_id ASC
        ''', row)
    return cursor