
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

#--------------
# other imports
# -------------

#--------------
# local imports
# -------------

from .. import MONTH_FORMAT, TSTAMP_FORMAT, EXPIRED

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# Hack while there is no observer SQL table
observer_data = {}

# ------------------
# AUXILIAR FUNCTIONS
# ------------------

def number_of_data_columns(nchannels):
    return 8 if nchannels == 1 else 17

def tess_type(name, connection):
    row = {'name': name, }
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT i.model
        FROM tess_t AS i
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        ''', row)
    return cursor.fetchone()

def get_mac_valid_period(connection, name, mac):
    log.debug("getting valid period for ({%s},{%s})", name, mac)
    cursor = connection.cursor()
    row = {'name': name, 'mac': mac}
    cursor.execute(
        '''
        SELECT valid_since,valid_until,valid_state
        FROM name_to_mac_t
        WHERE mac_address == :mac
        AND name  == :name
        ''', row)
    result =  cursor.fetchone()
    return {
        'value': mac, 
        'valid_since': result[0],
        'valid_until': result[1],
        'valid_state': result[2]
    }


def instrument_in_one_location(name, tess):
    mac_address = {'changed': False, 'current': {'value': tess[1]}}
    nchannels = int(tess[4])
    alt_az = tuple([tess[18], 90 - tess[19]])*nchannels
    if nchannels == 1:
        zps  = tess[5]
        flts = tess[6]
    else:
        zps = tuple(tess[i] for i in range(5, 9))
        flts =tuple(tess[i] for i in range(9, 13))
    zero_point  = {'changed': False, 'current': {'value': zps, 'valid_since': tess[13], 'valid_until':tess[14], 'valid_state': tess[15] }}
    filters     = {'changed': False, 'current': {'value': flts, 'valid_since': tess[13], 'valid_until':tess[14], 'valid_state': tess[15] }}
    return {
        'name':         name,
        'mac_address':  mac_address,
        'zero_point':   zero_point,
        'filter':       filters,
        'columns':      number_of_data_columns(nchannels),
        'model':        tess[2],
        'firmware':     tess[3],
        'nchannels':    tess[4],
        'cover_offset': tess[16], # TO BE DEPRECATED IN THE DATA MODEL
        'fov':          tess[17], # TO BE DEPRECATED IN THE DATA MODEL
        'az_alt': alt_az          # TO BE DEPRECATED IN THE DATA MODEL
    }

def if_changed(tess_list, index):
    nchannels1 = tess_list[0][4]
    nchannels2 = tess_list[1][4]
    valueA = tuple(tess_list[0][i] for i in range(index, index+4)) if nchannels1 > 1 else tess_list[0][index]
    valueB =  tuple(tess_list[1][i] for i in range(index, index+4)) if nchannels1 > 1 else tess_list[1][index]
    var1 = {
        'value':        valueA, 
        'valid_since':  tess_list[0][13], 
        'valid_until':  tess_list[0][14], 
        'valid_state':  tess_list[0][15] 
    }
    var2 = {
        'value':        valueB, 
        'valid_since':  tess_list[1][13], 
        'valid_until':  tess_list[1][14], 
        'valid_state':  tess_list[1][15] 
    }
    changed = True # Assumed this by default
    if nchannels1 == nchannels2:
        changed = valueA != valueB
    return var1, var2, changed

def maybe_swap(var1, var2):
    if var2['valid_state'] == EXPIRED:
        return var1, var2
    else:
        return var2, var1

def instrument_in_several_locations(name, tess_list, connection):
    mac_address = {'changed': False}
    zero_point  = {}
    filters     = {}
    mac1 = tess_list[0][1]
    mac2 = tess_list[1][1]
   
    # Even in the case of the change of MAC, there is almost 100% that the
    # zero point will change
    zp1, zp2, zero_point['changed']      = if_changed(tess_list, 5)
    filter1, filter2, filters['changed'] = if_changed(tess_list, 9)

    mac_record1 = get_mac_valid_period(connection, name, mac1)
    if mac1 != mac2 :
        # Change of MAC means also change of ZP with almost 100% probab.
        mac_address['changed']  = True
        mac_record2 = get_mac_valid_period(connection, name, mac2)
        if mac_record2['valid_state'] == EXPIRED:
            mac_address['current']  = mac_record1
            mac_address['previous'] = mac_record2
            zero_point['current']  = zp1
            zero_point['previous'] = zp2
            filters['current']  = filter1
            filters['previous'] = filter2
        else:
            mac_address['current']  = mac_record2
            mac_address['previous'] = mac_record1
            zero_point['current']  = zp2
            zero_point['previous'] = zp1
            filters['current']  = filter2
            filters['previous'] = filter1
    else:
        # No change of MAC means change of ZP, filter, azimuth or altitude.
        mac_address['current'] = mac_record1
        zero_point['current'], zero_point['previous'] = maybe_swap(zp1, zp2)
        filters['current'],    filters['previous']    = maybe_swap(filter1, filter2)
    nchannels = int(tess_list[0][4])
    alt_az = tuple([tess_list[0][18], 90 - tess_list[0][19]])*nchannels
    return {
        'name':         name,
        'mac_address':  mac_address,
        'zero_point':   zero_point,
        'filter':       filters,
        'columns':      number_of_data_columns(nchannels),
        'model':        tess_list[0][2],
        'firmware':     tess_list[0][3],
        'nchannels':    tess_list[0][4],
        'cover_offset': tess_list[0][16], # TO BE DEPRECATED IN THE DATA MODEL
        'fov':          tess_list[0][17], # TO BE DEPRECATED IN THE DATA MODEL
        'az_alt': alt_az,                 # TO BE DEPRECATED IN THE DATA MODEL
    }

def available(name, month, location_id, connection):
    instrument_type = tess_type(name, connection)
    if instrument_type is None:
        raise ValueError(f"Could not find: {name}")
    if instrument_type[0] == 'TESS-W':
        log.debug("[%s] photometer is a TESS-W", name)
        return available_tessw(name, month, location_id, connection)
    elif instrument_type[0] == 'TESS4C':
        log.debug("[%s] photometer is a TESS4C", name)
        return available_tess4c(name, month, location_id, connection)
    else:
        raise NotImplementedError(f"Unknown photometer model: {instrument_type}")

def available_tessw(name, month, location_id, connection):
    '''Return a a list of TESS for the given month and a given location_id'''
    row = {'name': name, 'location_id': location_id, 'from_date': month.strftime(TSTAMP_FORMAT)}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT i.tess_id, i.mac_address, i.model, i.firmware,  -- Offset 0
            i.nchannels, i.zp1, i.zp2, i.zp3, i.zp4, i.filter1, i.filter2, i.filter3, i.filter4, -- Offset 4
            i.valid_since, i.valid_until, i.valid_state, -- Offset 13
            i.cover_offset,  i.fov, i.azimuth, i.altitude -- Offset 16  TO BE DEPRECATED
        FROM tess_readings_t AS r
        JOIN date_t          AS d USING (date_id)
        JOIN time_t          AS t USING (time_id)
        JOIN tess_t          AS i USING (tess_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)                       -- THIS WILL HAVE TO BE REMOVED FOR TESS-4C
        AND   r.location_id == :location_id
        AND     datetime(d.sql_date || 'T' || t.time || '.000') 
        BETWEEN datetime(:from_date) 
        AND     datetime(:from_date, '+1 month')
        ORDER BY i.valid_state ASC -- 'Current' before 'Expired'
        ''', row)
    tess_list = cursor.fetchall()
    log.debug("[%s]: tess_list = %s", name, tess_list) 
    l = len(tess_list)
    if l == 1:
        log.debug("[%s]: Only 1 tess_id for this location id %d and month %s", name, location_id, month.strftime(MONTH_FORMAT))
    elif l == 2:
        log.info("[%s]: 2 tess_id (%d,%d) for this location id %d and month %s", name, tess_list[0][0], tess_list[1][0],location_id, month.strftime(MONTH_FORMAT) )
    elif l > 2:
        log.warning("[%s]: Oh no! %d tess_id for this location id %d and month %s", name, l, location_id, month.strftime(MONTH_FORMAT))
    else:
        log.error("[%s]: THIS SHOULD NOT HAPPEN No data for location id %d in month %s", name, location_id, month.strftime(MONTH_FORMAT))
    return tess_list, (l == 1)


def available_tess4c(name, month, location_id, connection):
    '''Return a a list of TESS for the given month and a given location_id'''
    row = {'name': name, 'location_id': location_id, 'from_date': month.strftime(TSTAMP_FORMAT)}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT i.tess_id, i.mac_address, i.model, i.firmware,  -- Offset 0
            i.nchannels, i.zp1, i.zp2, i.zp3, i.zp4, i.filter1, i.filter2, i.filter3, i.filter4, -- Offset 4
            i.valid_since, i.valid_until, i.valid_state, -- Offset 13
            i.cover_offset,  i.fov, i.azimuth, i.altitude -- Offset 16  TO BE DEPRECATED
        FROM tess_readings4c_t AS r
        JOIN date_t          AS d USING (date_id)
        JOIN time_t          AS t USING (time_id)
        JOIN tess_t          AS i USING (tess_id)
        WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)                       -- THIS WILL HAVE TO BE REMOVED FOR TESS-4C
        AND   r.location_id == :location_id
        AND     datetime(d.sql_date || 'T' || t.time || '.000') 
        BETWEEN datetime(:from_date) 
        AND     datetime(:from_date, '+1 month')
        ORDER BY i.valid_state ASC -- 'Current' before 'Expired'
        ''', row)
    tess_list = cursor.fetchall()
    log.debug("[%s]: tess_list = %s", name, tess_list) 
    l = len(tess_list)
    if l == 1:
        log.debug("[%s]: Only 1 tess_id for this location id %d and month %s", name, location_id, month.strftime(MONTH_FORMAT))
    elif l == 2:
        log.info("[%s]: 2 tess_id (%d,%d) for this location id %d and month %s", name, tess_list[0][0], tess_list[1][0],location_id, month.strftime(MONTH_FORMAT) )
    elif l > 2:
        log.warning("[%s]: Oh no! %d tess_id for this location id %d and month %s", name, l, location_id, month.strftime(MONTH_FORMAT))
    else:
        log.error("[%s]: THIS SHOULD NOT HAPPEN No data for location id %d in month %s", name, location_id, month.strftime(MONTH_FORMAT))
    return tess_list, (l == 1)


# --------------
# MAIN FUNCTIONS
# --------------

# Single instrument refers to an isntrument in a single location (i.e. not moved)
def instrument(name, month, location_id, connection):
    log.debug("[%s]: Exporting instrument data for month %s", name, month)
    context = {}
    tess_list_per_location, is_single = available(name, month, location_id, connection)
    if is_single:
        return instrument_in_one_location(name, tess_list_per_location[0])
    else:
        return instrument_in_several_locations(name, tess_list_per_location, connection)


# =======================================================
# EL NUEVO MODELO DE DATOS YA TIENE UNA TABLA DE OBSERVER
# PEEERO:
# ESTO VA A SEGUIR ASI HASTA QUE NO HAGA LA MIGRACION DE
# MOVER LOS DATOS DE OBSERVER DE LOS ANTIGUOS CAMPOS DE
# LOCATION
# =======================================================


def location(location_id, connection):
    global observer_data
    log.debug("Exporting location data for location_id %s", location_id)
    cursor = connection.cursor()
    row = {'location_id': location_id}
    cursor.execute(
            '''
            SELECT  longitude, latitude, elevation,             -- Offset 0
            place, town, sub_region, region, country, timezone, -- Offset 3
            contact_name, organization                          -- Offset 9 TO BE DEPRECATED
            FROM location_t
            WHERE location_id == :location_id
            ''', row)
    result = cursor.fetchone()

    # Hack while there is no observer SQL table
    observer_data['name']         = result[9]    # TO BE DEPRECATED
    observer_data['organization'] = result[10]   # TO BE DEPRECATEDS
    return {
        'longitude'      : result[0],
        'latitude'       : result[1],
        'elevation'      : result[2],
        'place'          : result[3],
        'town'           : result[4],
        'sub_region'     : result[5],
        'region'         : result[6],
        'country'        : result[7],
        'timezone'       : result[8],
    }

def observer(month, connection):
    log.debug("Exporting observer data for month %s", month)
    global observer_data     # Hack while there is no observer SQL table
    return observer_data

