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

# ----------------
# Module constants
# ----------------


# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------



# ----------------------
# INSTRUMENT SUBCOMMANDS
# ----------------------

def instrument_coalesce(connection, options):
    if options.name:
        instrument_coalesce_by_name(connection, options)
    elif options.mac:
        instrument_coalesce_by_mac(connection, options)
    elif options.all:
        instrument_coalesce_all(connection, options)


def instrument_coalesce_by_mac(connection,options):
    cursor = connection.cursor()
    row = {'mac': options.mac}
    cursor.execute(
        '''
        SELECT (SELECT name FROM name_to_mac_t WHERE mac_address == src.mac_address), src.mac_address,src.tess_id, dst.tess_id, src.zero_point, src.filter, src.azimuth, src.altitude
        FROM tess_t AS src
        CROSS JOIN tess_t AS dst
        WHERE src.mac_address == dst.mac_address
        AND src.tess_id       != dst.tess_id
        AND src.valid_until   == dst.valid_since
        AND src.zero_point    == dst.zero_point
        AND src.filter        == dst.filter
        AND src.azimuth       == dst.azimuth
        AND src.altitude      == dst.altitude
        AND src.mac_address   == :mac
        ORDER BY src.valid_since ASC
        ''', row)
    paging(cursor,["TESS","MAC","From Id","To Id","ZP","Filter","Azimuth","Altitude"],size=20)
    cursor.execute(
        '''
        SELECT :mac, count(*) 
        FROM tess_readings_t
        WHERE tess_id IN 
        (
            SELECT src.tess_id
            FROM tess_t AS src
            CROSS JOIN tess_t AS dst
            WHERE src.mac_address == dst.mac_address
            AND src.tess_id       != dst.tess_id
            AND src.valid_until   == dst.valid_since
            AND src.zero_point    == dst.zero_point
            AND src.filter        == dst.filter
            AND src.azimuth       == dst.azimuth
            AND src.altitude      == dst.altitude
            AND src.mac_address   == :mac
            ORDER BY src.valid_since ASC
        )
        ''', row)
    paging(cursor,["MAC","#Readings"])
    if options.test:
        return

    cursor.execute(
        '''
        SELECT MAX(dst.tess_id), MIN(src.valid_since)
        FROM tess_t AS src
        CROSS JOIN tess_t AS dst
        WHERE src.mac_address == dst.mac_address
        AND src.tess_id       != dst.tess_id
        AND src.valid_until   == dst.valid_since
        AND src.zero_point    == dst.zero_point
        AND src.filter        == dst.filter
        AND src.azimuth       == dst.azimuth
        AND src.altitude      == dst.altitude
        AND src.mac_address   == :mac
        GROUP BY src.mac_address
        ORDER BY src.valid_since ASC;
        ''', row)
    result = cursor.fetchone()
    row['target_tess_id'] = result[0]
    row[ 'target_date']   = result[1]
    # change all readings frist
    cursor.execute(
        '''
        UPDATE tess_readings_t
        SET  tess_id = :target_tess_id
        WHERE tess_id IN
        (
            SELECT src.tess_id
            FROM tess_t AS src
            CROSS JOIN tess_t AS dst
            WHERE src.mac_address == dst.mac_address
            AND src.tess_id       != dst.tess_id
            AND src.valid_until   == dst.valid_since
            AND src.zero_point    == dst.zero_point
            AND src.filter        == dst.filter
            AND src.azimuth       == dst.azimuth
            AND src.altitude      == dst.altitude
            AND src.mac_address   == :mac
            ORDER BY src.valid_since ASC
        )
        ''', row)
    

    # delete all intermediate tess_ids
    cursor.execute(
        '''
        DELETE FROM tess_t
        WHERE tess_id IN
        (
            SELECT src.tess_id
            FROM tess_t AS src
            CROSS JOIN tess_t AS dst
            WHERE src.mac_address == dst.mac_address
            AND src.tess_id       != dst.tess_id
            AND src.valid_until   == dst.valid_since
            AND src.zero_point    == dst.zero_point
            AND src.filter        == dst.filter
            AND src.azimuth       == dst.azimuth
            AND src.altitude      == dst.altitude
            AND src.mac_address   == :mac
            ORDER BY src.valid_since ASC
        )
        ''', row)
    
    # Fix target tess_id, this must be done last
    cursor.execute(
        '''
        UPDATE tess_t
        SET  valid_since = :target_date
        WHERE tess_id   == :target_tess_id
        ''', row)
    connection.commit()





def instrument_coalesce_by_name(connection,options):
    cursor = connection.cursor()
    row = {'name': options.name}
    cursor.execute(
        '''
        SELECT :name, src.mac_address, src.tess_id, dst.tess_id, src.zero_point, src.filter, src.azimuth, src.altitude
        FROM tess_t AS src
        CROSS JOIN tess_t AS dst
        WHERE src.mac_address == dst.mac_address
        AND src.tess_id       != dst.tess_id
        AND src.valid_until   == dst.valid_since
        AND src.zero_point    == dst.zero_point
        AND src.filter        == dst.filter
        AND src.azimuth       == dst.azimuth
        AND src.altitude      == dst.altitude
        AND src.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        ORDER BY src.valid_since ASC
        ''', row)
    paging(cursor,["TESS","MAC","From Id","To Id","ZP", "Filter","Azimuth","Altitude"], size=20)
    cursor.execute(
        '''
        SELECT :name, count(*) 
        FROM tess_readings_t
        WHERE tess_id IN 
        (
            SELECT src.tess_id
            FROM tess_t AS src
            CROSS JOIN tess_t AS dst
            WHERE src.mac_address == dst.mac_address
            AND src.tess_id       != dst.tess_id
            AND src.valid_until   == dst.valid_since
            AND src.zero_point    == dst.zero_point
            AND src.filter        == dst.filter
            AND src.azimuth       == dst.azimuth
            AND src.altitude      == dst.altitude
            AND src.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
            ORDER BY src.valid_since ASC
        )
        ''', row)
    paging(cursor,["Name","#Readings"], size=20)
    if options.test:
        return

    cursor.execute(
        '''
        SELECT MAX(dst.tess_id), MIN(src.valid_since)
        FROM tess_t AS src
        CROSS JOIN tess_t AS dst
        WHERE src.mac_address == dst.mac_address
        AND src.tess_id       != dst.tess_id
        AND src.valid_until   == dst.valid_since
        AND src.zero_point    == dst.zero_point
        AND src.filter        == dst.filter
        AND src.azimuth       == dst.azimuth
        AND src.altitude      == dst.altitude
        AND src.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
        GROUP BY src.mac_address
        ORDER BY src.valid_since ASC;
        ''', row)
    result = cursor.fetchone()
    row['target_tess_id'] = result[0]
    row[ 'target_date']   = result[1]

    # change all readings first
    cursor.execute(
        '''
        UPDATE tess_readings_t
        SET  tess_id = :target_tess_id
        WHERE tess_id IN
        (
            SELECT src.tess_id
            FROM tess_t AS src
            CROSS JOIN tess_t AS dst
            WHERE src.mac_address == dst.mac_address
            AND src.tess_id       != dst.tess_id
            AND src.valid_until   == dst.valid_since
            AND src.zero_point    == dst.zero_point
            AND src.filter        == dst.filter
            AND src.azimuth       == dst.azimuth
            AND src.altitude      == dst.altitude
            AND src.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
            ORDER BY src.valid_since ASC
        )
        ''', row)
   

    # delete all intermediate tess_ids
    cursor.execute(
        '''
        DELETE FROM tess_t
        WHERE tess_id IN
        (
            SELECT src.tess_id
            FROM tess_t AS src
            CROSS JOIN tess_t AS dst
            WHERE src.mac_address == dst.mac_address
            AND src.tess_id       != dst.tess_id
            AND src.valid_until   == dst.valid_since
            AND src.zero_point    == dst.zero_point
            AND src.filter        == dst.filter
            AND src.azimuth       == dst.azimuth
            AND src.altitude      == dst.altitude
            AND src.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name)
            ORDER BY src.valid_since ASC
        )
        ''', row)

      # Fix target tess_id, this must be done last
    cursor.execute(
        '''
        UPDATE tess_t
        SET  valid_since = :target_date
        WHERE tess_id   = :target_tess_id
        ''', row)

    connection.commit()



   


def instrument_coalesce_all(connection,options):
    cursor = connection.cursor()
    row = {'name': options.name}
    cursor.execute(
        '''
        SELECT src.mac_address,  MIN(src.tess_id), MAX(dst.tess_id), COUNT(*), MIN(src.valid_since), MAX(dst.valid_until), src.zero_point, src.filter, src.azimuth, src.altitude
        FROM tess_t AS src
        CROSS JOIN tess_t AS dst
        WHERE src.mac_address == dst.mac_address
        AND src.tess_id       != dst.tess_id
        AND src.valid_until   == dst.valid_since
        AND src.zero_point    == dst.zero_point
        AND src.filter        == dst.filter
        AND src.azimuth       == dst.azimuth
        AND src.altitude      == dst.altitude
        GROUP BY src.mac_address
        ORDER BY src.valid_since ASC;
        ''', row)
    paging(cursor,["MAC","From TESS Id","To TESS Id","Count","From Date","To Date","ZP","Filter","Azimuth","Altitude"], size=20)
    
    cursor.execute(
        '''
        SELECT MAX(dst.tess_id), MIN(src.valid_since)
        FROM tess_t AS src
        CROSS JOIN tess_t AS dst
        WHERE src.mac_address == dst.mac_address
        AND src.tess_id       != dst.tess_id
        AND src.valid_until   == dst.valid_since
        AND src.zero_point    == dst.zero_point
        AND src.filter        == dst.filter
        AND src.azimuth       == dst.azimuth
        AND src.altitude      == dst.altitude
        GROUP BY src.mac_address
        ORDER BY src.valid_since ASC;
        ''', row)
    






def instrument_assign(connection, options):
    cursor = connection.cursor()
    row = {'site': options.location,  'state': CURRENT}
    cursor.execute("SELECT location_id FROM location_t WHERE site == :site",row)
    res =  cursor.fetchone()
    if not res:
        print("Location not found by {0}".format(row['site']))
        sys.exit(1)
    row['loc_id'] = res[0]
    if options.name is not None:
        row['name'] = options.name
        cursor.execute(
            '''
            UPDATE tess_t SET location_id = :loc_id
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name AND valid_state == :state)
            ''', row)
    else:
         row['mac'] = options.mac
         cursor.execute(
            '''
            UPDATE tess_t SET location_id = :loc_id
            WHERE mac_address = :mac)
            ''', row)
    
    cursor.execute(
        '''
        SELECT name,mac_address,site
        FROM tess_v
        WHERE valid_state == :state
        AND name = :name
        ''',row)
    paging(cursor,["TESS","MAC","Site"])
    connection.commit()    


def instrument_single_history(connection, options):
    cursor = connection.cursor()
    row = {'tess': options.name, 'state': CURRENT}
    cursor.execute(
            '''
            SELECT name,tess_id,mac_address,zero_point,filter,azimuth,altitude,valid_since,valid_until
            FROM tess_v
            WHERE name == :name
            ORDER BY tess_t.valid_since ASC;
            ''',row)
    paging(cursor,["TESS","Id","MAC Addr.","Zero Point","Filter","Azimuth","Altitude","Since","Until"], size=100)


def instrument_list(connection, options):
    if options.name is None and options.mac is None:
        if options.log:
            instrument_all_attribute_changes(connection, options)
        else:
            instrument_all_current_list(connection, options)
    elif options.name is not None and options.mac is None:
        if options.extended:
            instrument_name_history_changes1(connection, options)
        if options.log:
            instrument_name_attribute_changes(connection, options)
        else:
            instrument_name_current_attributes(connection, options)
    else:
        if options.extended:
            instrument_name_history_changes2(connection, options)
        if options.log:
            instrument_mac_attribute_changes(connection, options)
        else:
            instrument_mac_current_attributes(connection, options)


def instrument_mac_current_attributes(connection, options):
    cursor = connection.cursor()
    row = {'mac': options.mac, 'state': CURRENT}
    cursor.execute(
            '''
            SELECT tess_id,mac_address,zero_point,filter,valid_state,authorised,registered,l.site
            FROM tess_t
            JOIN location_t AS l USING (location_id)
            WHERE mac_address = :mac 
            AND valid_state == :state
            ORDER BY tess_id ASC;
            ''', row)
    paging(cursor,["TESS Id","MAC Addr.","Zero Point","Filter","State","Enabled","Registered","Site"], size=100)

def instrument_mac_attribute_changes(connection, options):
    cursor = connection.cursor()
    row = {'mac': options.mac}
    cursor.execute(
            '''
            SELECT tess_id,mac_address,zero_point,filter,valid_state,authorised,registered,l.site
            FROM tess_t
            JOIN location_t AS l USING (location_id)
            WHERE mac_address = :mac
            ORDER BY tess_id ASC;
            ''', row)
    paging(cursor,["TESS Id","MAC Addr.","Zero Point","Filter","State","Enabled","Registered","Site"], size=100)


def instrument_all_current_list(connection, options):
    cursor = connection.cursor()
    row = {'state': CURRENT}
    cursor.execute(
            '''
            SELECT name,mac_address,zero_point,filter,site,authorised,registered
            FROM tess_v
            WHERE valid_state == :state
            ORDER BY CAST(substr(tess_v.name, 6) as decimal) ASC;
            ''', row)
    paging(cursor,["TESS","MAC Addr.","Zero Point","Filter","Site","Enabled","Registered"], size=100)

def instrument_all_attribute_changes(connection, options):
    cursor = connection.cursor()
    cursor.execute(
            '''
            SELECT name,tess_id,mac_address,zero_point,filter,site,valid_since,valid_until,authorised,registered
            FROM tess_v
            ORDER BY CAST(substr(tess_v.name, 6) as decimal) ASC, tess_v.valid_since ASC;
            ''')
    paging(cursor,["TESS","Id","MAC Addr.","Zero Point","Filter","Site","Since","Until","Enabled","Registered"], size=100)


def instrument_name_attribute_changes(connection, options):
    cursor = connection.cursor()
    row = {'state': CURRENT, 'name': options.name}
    cursor.execute(
            '''
            SELECT name,tess_id,mac_address,zero_point,filter,site,valid_since,valid_until,authorised,registered
            FROM tess_v
            WHERE name == :name
            ORDER BY tess_v.valid_since ASC;
            ''',row)
    paging(cursor,["TESS","Id","MAC Addr.","Zero Point","Filter","Site","Since","Until","Enabled","Registered"], size=100)


def instrument_name_history_changes1(connection, options):
    cursor = connection.cursor()
    row = {'name': options.name}
    cursor.execute(
            '''
            SELECT name,mac_address,valid_state,valid_since,valid_until
            FROM name_to_mac_t
            WHERE name == :name
            ORDER BY valid_since ASC;
            ''',row)
    paging(cursor,["TESS","MAC Addr.","State","Name Valid Since","Name Valid Until"], size=100)


def instrument_name_history_changes2(connection, options):
    cursor = connection.cursor()
    row = {'mac': options.mac}
    cursor.execute(
            '''
            SELECT name,mac_address,valid_state,valid_since,valid_until
            FROM name_to_mac_t
            WHERE mac_address == :mac
            ORDER BY valid_since ASC;
            ''',row)
    paging(cursor,["TESS","MAC Addr.","State","Name Valid Since","Name Valid Until"], size=100)


def instrument_all_current_list(connection, options):
    cursor = connection.cursor()
    row = {'state': CURRENT}
    cursor.execute(
            '''
            SELECT name,tess_id,mac_address,zero_point,filter,site,authorised,registered
            FROM tess_v
            WHERE valid_state == :state
            ORDER BY CAST(substr(tess_v.name, 6) as decimal) ASC;
            ''', row)
    paging(cursor,["TESS","Id","MAC Addr.","Zero Point","Filter","Site","Enabled","Registered"], size=100)


def instrument_name_current_attributes(connection, options):
    cursor = connection.cursor()
    row = {'state': CURRENT, 'name': options.name}
    cursor.execute(
            '''
            SELECT name,tess_id,mac_address,zero_point,filter,site,authorised,registered
            FROM tess_v
            WHERE valid_state == :state
            AND name == :name;
            ''', row)
    paging(cursor,["TESS","Id","MAC Addr.","Zero Point","Filter","Site","Enabled","Registered"], size=100)


def instrument_anonymous(connection, options):
    cursor = connection.cursor()
    row = {'state': EXPIRED}
    cursor.execute(
            '''
            SELECT name,mac_address,min(valid_since),max(valid_until),min(valid_state)
            FROM name_to_mac_t
            GROUP BY name
            HAVING min(valid_state) = :state
            ORDER BY CAST(substr(name, 6) as decimal) ASC;
            ''', row)
    paging(cursor,["TESS Tag (free)","Previous MAC Addr.","Name valid since","Name valid until","State"])

def instrument_renamings(connection, options):
    cursor = connection.cursor()
    row = {'state': EXPIRED}
    if options.summary:
        cursor.execute(
            '''
            SELECT dst.valid_since,src.name,dst.name
            FROM name_to_mac_t AS src
            JOIN name_to_mac_t AS dst USING (mac_address)
            WHERE src.name != dst.name
            AND   src.valid_state == "Expired"
            ORDER BY dst.valid_since ASC;
            ''', row)
        paging(cursor,["When","Original TESS Name","Renamed To TESS name"], size=100)

    elif options.name:
        cursor.execute(
            '''
            SELECT name,mac_address,valid_since,valid_until,valid_state
            FROM name_to_mac_t
            WHERE name in (SELECT name FROM name_to_mac_t GROUP BY name HAVING count(*) > 1)
            ORDER BY CAST(substr(name, 6) as decimal) ASC;
            ''', row)
        paging(cursor,["TESS","MAC Addr.","Name valid since","Name valid until","State"], size=100)
    else:
        cursor.execute(
            '''
            SELECT name,mac_address,valid_since,valid_until,valid_state
            FROM name_to_mac_t
            WHERE mac_address in (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING count(*) > 1)
            ORDER BY CAST(substr(name, 6) as decimal) ASC;
            ''', row)
        paging(cursor,["TESS","MAC Addr.","Name valid since","Name valid until","State"], size=100)


def instrument_unassigned(connection, options):
    cursor = connection.cursor()
    row = {'state': CURRENT, 'site1': UNKNOWN, 'site2': OUT_OF_SERVICE}
    cursor.execute(
            '''
            SELECT name,tess_id,mac_address,zero_point,filter,azimuth,altitude,site,authorised,registered
            FROM tess_v
            WHERE valid_state == :state
            AND (site == :site1 OR site == :site2)
            ORDER BY CAST(substr(tess_v.name, 6) as decimal) ASC;
            ''', row)
    paging(cursor,["TESS","Id","MAC Addr.","Zero Point","Filter","Azimuth","Altitude","Site","Enabled","Registered"], size=100)


def instrument_enable(connection, options):
    cursor = connection.cursor()
    row = {'tess': options.name, 'state': CURRENT}
    cursor.execute('''
        UPDATE tess_t 
        SET authorised = 1 
        WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name AND valid_state == :state)
        AND valid_state == :state
        ''',row)
    
    cursor.execute(
        '''
        SELECT name,site,authorised
        FROM tess_v
        WHERE valid_state == :state
        AND name = :tess
        ''',row)
    paging(cursor,["TESS","Site","Authorised"])
    connection.commit()    

def instrument_disable(connection, options):
    cursor = connection.cursor()
    row = {'tess': options.name, 'state': CURRENT}
    cursor.execute('''
        UPDATE tess_t 
        SET authorised = 0 
        WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name AND valid_state == :state) 
        AND valid_state == :state
        ''',row)
    
    cursor.execute(
        '''
        SELECT name,site,authorised
        FROM tess_v
        WHERE valid_state == :state
        AND name = :tess
        ''',row)
    paging(cursor,["TESS","Site","Authorised"])
    connection.commit()    


def instrument_create(connection, options):
    cursor = connection.cursor()
    row = {}
    row['name']       = options.name
    row['mac']        = options.mac
    row['zp']         = options.zp
    row['filter']     = options.filter
    row['azimuth']    = options.azimuth
    row['altitude']   = options.altitude
    row['valid_flag'] = CURRENT
    row['eff_date']   = datetime.datetime.utcnow().strftime(TSTAMP_FORMAT)
    row['exp_date']   = INFINITE_TIME
    row['registered'] = MANUAL;
    
    # Find existing MAC and abort if so
    cursor.execute(
        '''
        SELECT mac_address
        FROM tess_t 
        WHERE mac_address == :mac
        AND valid_state == :valid_flag
        ''', row)
    result = cursor.fetchone()
    if result:
        raise IndexError("Already existing MAC %s" % (row['mac'],) )
    # Find existing name and abort if so
    cursor.execute(
        '''
        SELECT mac_address
        FROM tess_t 
        WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name AND valid_state == :valid_flag)
        AND valid_state == :valid_flag 
        ''', row)
    result = cursor.fetchone()
    if result:
        raise IndexError("Other instrument already using friendly name %s" (row['name'],) )
    # Write into database
    cursor.execute(
        '''
        INSERT INTO tess_t (
            mac_address, 
            zero_point,
            filter,
            azimuth,
            altitude,
            registered,
            valid_since,
            valid_until,
            valid_state
        ) VALUES (
            :mac,
            :zp,
            :filter,
            :azimuth,
            :altitude,
            :registered,
            :eff_date,
            :exp_date,
            :valid_flag
        )
        ''',  row)
    cursor.execute(
        '''
        INSERT INTO name_to_mac_t (
            name,
            mac_address, 
            valid_since,
            valid_until,
            valid_state
        ) VALUES (
            :name
            :mac,
            :eff_date,
            :exp_date,
            :valid_flag
        )
        ''',  row)
    connection.commit()
    # Now display it
    cursor.execute(
        '''
        SELECT name,mac_address,valid_state,valid_since,valid_until
        FROM   name_to_mac_t
        WHERE  name == :name
        ''', row)
    paging(cursor,["TESS","MAC Addr.","State","Name Valid Since","Name Valid Until"])
    cursor.execute(
        '''
        SELECT name, mac_address, zero_point, filter, azimuth, altitude, registered, site
        FROM   tess_v
        WHERE  name == :name
        AND    valid_state == :valid_flag
        ''', row)
    paging(cursor,["TESS","MAC Addr.","Calibration","Filter","Azimuth","Altitude","Registered","Site"])
    

def instrument_rename(connection, options):
    cursor = connection.cursor()
    row = {}
    row['newname']  = options.new_name
    row['oldname']  = options.old_name
    row['valid_flag'] = CURRENT
    row['valid_expired'] = EXPIRED
    row['eff_date'] = datetime.datetime.utcnow().replace(microsecond=0) if options.eff_date is None else options.eff_date
    row['exp_date'] = INFINITE_TIME

    # This check is common to both cases
    cursor.execute("SELECT mac_address FROM tess_v WHERE name == :oldname", row)
    oldmac = cursor.fetchone()
    if not oldmac:
        raise IndexError("Cannot rename. Instrument with old name %s does not exist." 
            % (options.old_name,) )
    row['oldmac'] =oldmac[0]

    # This is always performed, regardless clean rename or override
    cursor.execute(
        '''
        UPDATE name_to_mac_t 
        SET valid_until = :eff_date, valid_state = :valid_expired
        WHERE mac_address == :oldmac
        AND name == :oldname
        AND valid_state == :valid_flag
        ''', row)

    cursor.execute("SELECT mac_address FROM tess_v WHERE name == :newname", row)
    newmac = cursor.fetchone()
    if newmac:
        # If instrument with name exists, this is an override
        # And a second row must be updated too
        row['newmac'] =newmac[0]
        cursor.execute(
        '''
        UPDATE name_to_mac_t 
        SET valid_until = :eff_date, valid_state = :valid_expired
        WHERE mac_address == :newmac
        AND name == :newname
        AND valid_state == :valid_flag
        ''', row)

    # Insert a new association, regardless clean rename or override
    cursor.execute(
        '''
        INSERT INTO name_to_mac_t (
            name,
            mac_address,
            valid_since,
            valid_until,
            valid_state
        ) VALUES (
            :newname,
            :oldmac,
            :eff_date,
            :exp_date,
            :valid_flag
        )
        ''', row)
    connection.commit()

    # Now display the changes
    cursor.execute(
        '''
        SELECT name,mac_address,valid_state,valid_since,valid_until
        FROM   name_to_mac_t
        WHERE  name == :newname
        OR     name == :oldname
        ''', row)
    paging(cursor,["TESS","MAC Addr.","State","Name Valid Since","Name Valid Until"])


def instrument_delete(connection, options):
    cursor = connection.cursor()
    row = {}
    
    row['valid_flag'] = CURRENT

    if options.name is not None:
        row['name']  = options.name
        cursor.execute(
            '''
            SELECT mac_address
            FROM   tess_v
            WHERE  name == :name
            ''', row)
        result = cursor.fetchone()
        if not result:
            raise IndexError("Cannot delete. Instrument with name '%s' does not exist." 
                % (options.name,) )
        row['mac'] = result[0]

    else:
        row['mac']  = options.mac
        cursor.execute(
            '''
            SELECT name
            FROM   tess_v
            WHERE  mac_address == :mac
            ''', row)
        result = cursor.fetchone()
        if not result:
            raise IndexError("Cannot delete. Instrument with MAC '%s' does not exist." 
                % (options.mac,) )
        row['name'] = result[0]

    cursor.execute('''
        SELECT COUNT(*) from tess_readings_t
        WHERE tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :mac)
        ''', row)
    result = cursor.fetchone()
    if result[0] > 0:
        raise IndexError("Cannot delete instrument. Existing readings with this instrument '%s' are already stored." % (row['mac'],) )

    # Find out what's being deleted
    print("About to delete")
    cursor.execute(
        '''
        SELECT name,tess_id,mac_address,zero_point,filter,azimuth,altitude,site
        FROM   tess_v
        WHERE  mac_address == :mac
        ''', row)
    paging(cursor,["TESS","Id.","MAC Addr.","Zero Point","Filter","Azimuth","Altitude","Site"])
    
    # Find out if it has accumulated readings
    # This may go away if readings are stored in another database (i.e influxdb)
    cursor.execute(
        '''
        SELECT i.mac_address, count(*) AS readings
        FROM tess_readings_t AS r
        JOIN tess_t          AS i USING (tess_id)
        WHERE i.mac_address == :mac
        ''', row)
    paging(cursor,["TESS","Acumulated Readings"])

    if not options.test:
        cursor.execute('''
            DELETE 
            FROM tess_readings_t
            WHERE tess_id IN (SELECT tess_id FROM tess_t WHERE mac_address == :mac)
            ''', row)
        cursor.execute("DELETE FROM name_to_mac_t WHERE mac_address == :mac", row)
        cursor.execute("DELETE FROM tess_t WHERE mac_address == :mac", row)
        connection.commit()
        print("Instrument and readings deleted")


def instrument_update(connection, options):
    if options.latest:
        instrument_raw_update(connection, options)
    else:
        try:
            datetime.datetime.strptime(options.start_time, TSTAMP_FORMAT)
        except ValueError as e:
            print("Invalid start date YYYY-MM-DDTHH:MM:SS format: => %s" % (options.start_time,) )
        else:
            instrument_controlled_update(connection, options)


def instrument_raw_update(connection, options):
    '''Raw update lastest instrument calibration constant (with 'Current' state)'''
    cursor = connection.cursor()
    row = {}
    row['name']       = options.name
    row['valid_flag'] = CURRENT
    cursor.execute(
        '''
        SELECT name, mac_address
        FROM tess_v 
        WHERE name == :name
        AND valid_state == :valid_flag 
        ''', row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot rename. Existing instrument with name %s does not exist."
         % (options.name,) )
    row['mac']           = result[1]

    # Change only if passed in the command line
    if options.zero_point is not None:
        row['zp'] = options.zero_point
        cursor.execute(
        '''
        UPDATE tess_t SET zero_point = :zp
        WHERE mac_address == :mac AND valid_state == :valid_flag
        ''', row)

    if options.filter is not None:
        row['filter'] = options.filter
        cursor.execute(
        '''
        UPDATE tess_t SET filter = :filter
        WHERE mac_address == :mac AND valid_state == :valid_flag
        ''', row)

    if options.azimuth is not None:
        row['azimuth'] = options.azimuth
        cursor.execute(
        '''
        UPDATE tess_t SET azimuth = :azimuth
        WHERE mac_address == :mac AND valid_state == :valid_flag
        ''', row)

    if options.altitude is not None:
        row['altitude'] = options.altitude
        cursor.execute(
        '''
        UPDATE tess_t SET altitude = :altitude
        WHERE mac_address == :mac AND valid_state == :valid_flag
        ''', row)

    if options.registered is not None:
        row['registered'] = options.registered
        cursor.execute(
        '''
        UPDATE tess_t SET registered = :registered
        WHERE mac_address == :mac AND valid_state == :valid_flag
        ''', row)

    connection.commit()
    print("Operation complete.")
    cursor.execute(
        '''
        SELECT name, zero_point, filter, azimuth, altitude, valid_state, valid_since, valid_until, registered, site
        FROM   tess_v
        WHERE  name == :name AND valid_state == :valid_flag 
        ''', row)
    paging(cursor,["TESS","Zero Point","Filter","Azimuth","Altitude","State","Since","Until", "Registered", "Site"])




def instrument_controlled_update(connection, options):
    '''
    Update lastest instrument calibration constant with control change
    creating a new row with new calibration state and valid interval
    '''
    cursor = connection.cursor()
    row = {}
    row['name']       = options.name
    row['valid_flag'] = CURRENT
    cursor.execute(
        '''
        SELECT mac_address, location_id, valid_since, zero_point, filter, azimuth, altitude, authorised, registered 
        FROM tess_t 
        WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name AND valid_state == :valid_flag)
        AND valid_state == :valid_flag 
        ''', row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot rename. Existing instrument with name %s does not exist." % (options.name,) )
    if result[2] >= options.start_time:
        raise ValueError("Cannot set valid_since (%s) column to an equal or earlier date (%s)" % (result[2], options.start_time) )

    row['mac']           = result[0]
    row['location']      = result[1]
    row['eff_date']      = options.start_time
    row['exp_date']      = INFINITE_TIME
    row['valid_expired'] = EXPIRED
    row['zp']            = result[3] if options.zero_point is None else options.zero_point
    row['filter']        = result[4] if options.filter is None else options.filter
    row['azimuth']       = result[5] if options.azimuth is None else options.azimuth
    row['altitude']      = result[6] if options.altitude is None else options.altitude
    row['authorised']    = result[7]
    row['registered']    = result[8] if options.registered is None else options.registered
    cursor.execute(
        '''
        UPDATE tess_t SET valid_until = :eff_date, valid_state = :valid_expired
        WHERE mac_address == :mac AND valid_state == :valid_flag
        ''', row)

    cursor.execute(
        '''
        INSERT INTO tess_t (
            mac_address, 
            zero_point,
            filter,
            azimuth,
            altitude,
            valid_since,
            valid_until,
            valid_state,
            authorised,
            registered,
            location_id
        ) VALUES (
            :mac,
            :zp,
            :filter,
            :azimuth,
            :altitude,
            :eff_date,
            :exp_date,
            :valid_flag,
            :authorised,
            :registered,
            :location
            )
        ''',  row)
    cursor.execute(
        '''
        INSERT INTO name_to_mac_t (
            name,
            mac_address, 
            valid_since,
            valid_until,
            valid_state
        ) VALUES (
            :name
            :mac,
            :eff_date,
            :exp_date,
            :valid_flag
        )
        ''',  row)
    connection.commit()
    print("Operation complete.")
    
    cursor.execute(
        '''
        SELECT name, zero_point, filter, azimuth, altitude, valid_state, valid_since, valid_until, authorised, registered, site
        FROM   tess_v
        WHERE  name == :name
        ''', row)
    paging(cursor,["TESS","Zero Point","Filter","Azimuth","Altitude","State","Since","Until", "Authorised","Registered","Site"])
