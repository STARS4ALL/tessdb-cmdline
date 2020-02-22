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

from . import __version__

import tabulate


# Python3 catch
try:
    raw_input
except:
    raw_input = input 

# ----------------
# Module constants
# ----------------

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

# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------

def utf8(s):
    if sys.version_info[0] < 3:
        return unicode(s, 'utf8')
    else:
        return (s)

def mkdate(datestr):
    try:
        date = datetime.datetime.strptime(datestr, '%Y-%m-%d').replace(hour=12)
    except ValueError:
        date = datetime.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
    return date

def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser    = argparse.ArgumentParser(prog=name, description="tessdb command line tool " + __version__)
    subparser = parser.add_subparsers(dest='command')

    # --------------------------
    # Create first level parsers
    # --------------------------
    parser_instrument = subparser.add_parser('instrument', help='instrument commands')
    parser_location   = subparser.add_parser('location',   help='location commands')
    parser_readings   = subparser.add_parser('readings',   help='readings commands')

    # ------------------------------------------
    # Create second level parsers for 'location'
    # ------------------------------------------
    # Choices:
    #   tess location list
    #
    subparser = parser_location.add_subparsers(dest='subcommand')

    lcp = subparser.add_parser('create', help='create single location')
    lcp.add_argument('site', metavar='<site>', type=utf8, help='Unique site name')
    lcp.add_argument('-o', '--longitude', type=float, default=0.0,       help='geographical longitude (degrees)')
    lcp.add_argument('-a', '--latitude',  type=float, default=0.0,       help='geographical latitude (degrees)')
    lcp.add_argument('-e', '--elevation', type=float, default=0.0,       help='elevation above sea level(meters)')
    lcp.add_argument('-z', '--zipcode',   type=utf8,  default='Unknown', help='Postal Code')
    lcp.add_argument('-l', '--location',  type=utf8,  default='Unknown', help='Location (village, town, city)')
    lcp.add_argument('-p', '--province',  type=utf8,  default='Unknown', help='Province')
    lcp.add_argument('-c', '--country',   type=utf8,  default='Unknown', help='Country')
    lcp.add_argument('-w', '--owner',     type=utf8,  default='Unknown', help='Contact person')
    lcp.add_argument('-m', '--email',     type=str,   default='Unknown', help='Contact email')
    lcp.add_argument('-g', '--org',       type=utf8,  default='Unknown', help='Organization')
    lcp.add_argument('-t', '--tzone',     type=str,   default='Etc/UTC', help='Olson Timezone')
    lcp.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    llp = subparser.add_parser('list', help='list single location or all locations')
    llp.add_argument('-n', '--name',      type=utf8,  help='specific location name')
    llp.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    llp.add_argument('-x', '--extended', action='store_true',  help='extended listing')
    llp.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    lup = subparser.add_parser('update', help='update single location')
    lup.add_argument('site', metavar='<site>', type=utf8, help='Unique site name')
    lup.add_argument('-o', '--longitude', type=float, help='geographical longitude (degrees)')
    lup.add_argument('-a', '--latitude',  type=float, help='geographical latitude (degrees)')
    lup.add_argument('-e', '--elevation', type=float, help='elevation above sea level(meters)')
    lup.add_argument('-z', '--zipcode',   type=utf8,  help='Postal Code')
    lup.add_argument('-l', '--location',  type=utf8,  help='Location (village, town, city)')
    lup.add_argument('-p', '--province',  type=utf8,  help='Province')
    lup.add_argument('-c', '--country',   type=utf8,  help='Country')
    lup.add_argument('-w', '--owner',     type=utf8,  help='Contact person')
    lup.add_argument('-m', '--email',     type=str,   help='Contact email')
    lup.add_argument('-g', '--org',       type=utf8,  help='Organization')
    lup.add_argument('-t', '--tzone',     type=str,   help='Olson Timezone')
    lup.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')


    lde = subparser.add_parser('delete', help='single location to delete')
    lde.add_argument('name', type=utf8,  help='location name')
    lde.add_argument('-t', '--test', action='store_true',  help='test only, do not delete')
    lde.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    lre = subparser.add_parser('rename', help='rename single location')
    lre.add_argument('old_site',  type=utf8, help='old site name')
    lre.add_argument('new_site',  type=utf8, help='new site name')
    lre.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    lkp = subparser.add_parser('unassigned', help='list all unassigned locations')
    lkp.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    lkp.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ldup = subparser.add_parser('duplicates', help='list all duplicated locations')
    ldup.add_argument('--distance', type=int, default=100, help='Maximun distance in meters')
    ldup.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    ldup.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')


    # ------------------------------------------
    # Create second level parsers for 'readings'
    # ------------------------------------------
    # Choices:
    #   tess readings list
    #   tess readings adjloc <instrument name> -o <old site name> -n <new site name> -s <start date> -e <end date>
    #
    subparser = parser_readings.add_subparsers(dest='subcommand')


    run = subparser.add_parser('unassigned', help='count all unassigned location readings')
    run.add_argument('-c', '--count', type=int, default=200, help='list up to <count> entries')
    run.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    rli = subparser.add_parser('list', help='list readings')
    rliex = rli.add_mutually_exclusive_group(required=False)
    rliex.add_argument('-n', '--name', type=str, help='instrument name')
    rliex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    rli.add_argument('-c', '--count', type=int, default=10, help='list up to <count> entries')
    rli.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    rco = subparser.add_parser('count', help='count readings')
    rcoex = rco.add_mutually_exclusive_group(required=True)
    rcoex.add_argument('-n', '--name', type=str, help='instrument name')
    rcoex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    rco.add_argument('-s', '--start-date', type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_START_DATE, help='start date')
    rco.add_argument('-e', '--end-date',   type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_END_DATE, help='end date')
    rco.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ral = subparser.add_parser('adjloc', help='adjust readings location for a given TESS')
    ralex = ral.add_mutually_exclusive_group(required=True)
    ralex.add_argument('-n', '--name', type=str, help='instrument name')
    ralex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    ral.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    ral.add_argument('-o', '--old-site',   type=utf8, required=True, help='old site name')
    ral.add_argument('-w', '--new-site',   type=utf8, required=True, help='new site name')
    ral.add_argument('-s', '--start-date', type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_START_DATE, help='start date')
    ral.add_argument('-e', '--end-date',   type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_END_DATE, help='end date')
    ral.add_argument('-t', '--test', action='store_true',  help='test only, do not change readings')

    rpu = subparser.add_parser('purge', help='purge readings for a given TESS')
    rpuex = rpu.add_mutually_exclusive_group(required=True)
    rpuex.add_argument('-n', '--name', type=str, help='instrument name')
    rpuex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    rpu.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    rpu.add_argument('-l', '--location',   type=utf8, required=True, help='site name')
    rpu.add_argument('-s', '--start-date', type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_START_DATE, help='start date')
    rpu.add_argument('-e', '--end-date',   type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_END_DATE, help='end date')
    rpu.add_argument('-t', '--test', action='store_true',  help='test only, do not change readings')

    rai = subparser.add_parser('adjins', help='assign readings from <old> to <new> TESS instruments')
    rai.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    rai.add_argument('-o', '--old',   type=utf8, required=True, help='old MAC')
    rai.add_argument('-n', '--new',   type=utf8, required=True, help='new MAC')
    rai.add_argument('-s', '--start-date', type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_START_DATE, help='start date')
    rai.add_argument('-e', '--end-date',   type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_END_DATE, help='end date')
    rai.add_argument('-t', '--test', action='store_true',  help='test only, do not change readings')


    # --------------------------------------------
    # Create second level parsers for 'instrument'
    # --------------------------------------------
    # Choices:
    #   tess instrument list --name <instrument name>
    #   tess instrument assign <instrument name> <location name>
    #   tess instrument create <friendly name> <MAC address> <Calibration Constant>
    #   tess instrument rename <old friendly name> <new friendly name>
    #   tess instrument update <friendly name> --zero-point <new zero point> --filter <new filter> --latest
    #   tess instrument delete <instrument name> 
    #   tess instrument enable <instrument name> 
    #   tess instrument disable <instrument name> 
    #
    subparser = parser_instrument.add_subparsers(dest='subcommand')

    icr = subparser.add_parser('create',   help='create single instrument')
    icr.add_argument('name',   type=str,   help='friendly name')
    icr.add_argument('mac',    type=str,   help='MAC address')
    icr.add_argument('zp',     type=float, help='Zero Point')
    icr.add_argument('filter', type=str,   help='Filter (i.e. UV/IR-cut, BG39, GG495, etc.)')
    icr.add_argument('-a', '--azimuth',    type=float, default=DEFAULT_AZIMUTH, help='Azimuth (degrees). 0.0 = North')
    icr.add_argument('-t', '--altitude',   type=float, default=DEFAULT_ALTITUDE, help='Altitude (degrees). 90.0 = Zenith')
    icr.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ip = subparser.add_parser('list', help='list single instrument or all instruments')
    ipex = ip.add_mutually_exclusive_group(required=False)
    ipex.add_argument('-n', '--name', type=str, help='instrument name')
    ipex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    ip.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    ip.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    ip.add_argument('-l', '--log', action='store_true', default=False, help='show TESS instrument change log')
    ip.add_argument('-x', '--extended', action='store_true', default=False, help='show TESS instrument name changes')

    ihi = subparser.add_parser('history',  help='single instrument history')
    ihiex = ihi.add_mutually_exclusive_group(required=True)
    ihiex.add_argument('-n', '--name', type=str, help='instrument name')
    ihiex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    ihi.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    
    iup = subparser.add_parser('update',   help='update single instrument attributes')
    iupex1 = iup.add_mutually_exclusive_group(required=True)
    iupex1.add_argument('-n', '--name', type=str, help='instrument name')
    iupex1.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    iup.add_argument('-z', '--zero-point', type=float, help='new zero point')
    iup.add_argument('-f', '--filter',     type=str,  help='new filter glass')
    iup.add_argument('-a', '--azimuth',    type=float, help='Azimuth (degrees). 0.0 = North')
    iup.add_argument('-t', '--altitude',   type=float, help='Altitude (degrees). 90.0 = Zenith')
    iup.add_argument('-r', '--registered', type=str, choices=["Manual","Automatic","Unknown"], help='Registration Method: [Unknown,Manual,Automatic]')
    iup.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    iupex2 = iup.add_mutually_exclusive_group()
    now = datetime.datetime.utcnow().strftime(TSTAMP_FORMAT)
    iupex2.add_argument("-s", "--start-time", type=str, default=now, metavar="YYYYMMDDTHHMMSS", help='update start date')
    iupex2.add_argument('-l', '--latest', action='store_true', default=False, help='Latest entry only (no change control)')

    iaz = subparser.add_parser('enable', help='enable storing single instrument samples')
    iazex = iaz.add_mutually_exclusive_group(required=True)
    iazex.add_argument('-n', '--name', type=str, help='instrument name')
    iazex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    iaz.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    iaz.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    
    iuz = subparser.add_parser('disable', help='disable storing single instrument samples')
    iuzex = iuz.add_mutually_exclusive_group(required=True)
    iuzex.add_argument('-n', '--name', type=str, help='instrument name')
    iuzex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    iuz.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    iuz.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ide = subparser.add_parser('delete', help='delete single instrument')
    ideex = ide.add_mutually_exclusive_group(required=True)
    ideex.add_argument('-n', '--name', type=str, help='instrument name')
    ideex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    ide.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    ide.add_argument('-t', '--test', action='store_true',  help='test only, do not delete')

    ire = subparser.add_parser('rename', help='rename instrument friendly name')
    ire.add_argument('old_name',  type=str, help='old friendly name')
    ire.add_argument('new_name',  type=str, help='new friendly name')
    ire.add_argument('-s', '--eff-date', type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>',  help='effective date')
    ire.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ias = subparser.add_parser('assign', help='assign single instrument to location')
    iasex = ias.add_mutually_exclusive_group(required=True)
    iasex.add_argument('-n', '--name', type=str, help='instrument name')
    iasex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    ias.add_argument('-l','--location',   required=True, metavar='<location>', type=utf8,  help='Location name')
    ias.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ik = subparser.add_parser('unassigned', help='list unassigned instruments')
    ik.add_argument('-p', '--page-size', type=int, default=10, help='list page size')
    ik.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ian = subparser.add_parser('anonymous', help='list anonymous instruments without a friendly name')
    ian.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    ings = subparser.add_parser('renamings', help='list all instrument renamings')
    ingsex = ings.add_mutually_exclusive_group(required=True)
    ingsex.add_argument('-s', '--summary', action='store_true', help='summary')
    ingsex.add_argument('-n', '--name', action='store_true', help='detail by instrument name')
    ingsex.add_argument('-m', '--mac',  action='store_true', help='detali by instrument MAC')
    ings.add_argument('-c', '--count', type=int, default=10, help='list up to <count> entries')
    ings.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')

    return parser

def main():
    '''
    Utility entry point
    '''
    try:
        invalid_cache = False
        options = createParser().parse_args(sys.argv[1:])
        connection = open_database(options)
        command = options.command
        subcommand = options.subcommand
        if subcommand in ["rename","enable","disable","update","delete"]:
            invalid_cache = True
        # Call the function dynamically
        func = command + '_' + subcommand
        globals()[func](connection, options)

    except KeyboardInterrupt:
        print('')
    #except Exception as e:
        print("Error => {0}".format( utf8(str(e)) ))
    finally:
        if invalid_cache:
            print("WARNING: Do not forget to issue 'service tessdb reload' afterwards to invalidate tessdb caches")

# ==============
# DATABASE STUFF
# ==============

def open_database(options):
    if not os.path.exists(options.dbase):
        raise IOError("No SQLite3 Database file found in {0}. Exiting ...".format(options.dbase))
    return sqlite3.connect(options.dbase)
 

def paging(cursor, headers, size=10):
    '''
    Pages query output and displays in tabular format
    '''
    ONE_PAGE = 10
    while True:
        result = cursor.fetchmany(ONE_PAGE)
        print(tabulate.tabulate(result, headers=headers, tablefmt='grid'))
        if len(result) < ONE_PAGE:
            break
        size -= ONE_PAGE
        if size > 0:
            raw_input("Press Enter to continue [Ctrl-C to abort] ...")
        else:
            break

# ----------------------
# INSTRUMENT SUBCOMMANDS
# ----------------------

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
            SELECT src.name,dst.name,dst.valid_since
            FROM name_to_mac_t AS src
            JOIN name_to_mac_t AS dst USING (mac_address)
            WHERE src.name != dst.name
            AND   src.valid_state == "Expired"
            ORDER BY CAST(substr(src.name, 6) as decimal) ASC;
            ''', row)
        paging(cursor,["Original TESS Name","Renamed To TESS name.","Renamed at"], size=100)

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


# --------------------
# LOCATION SUBCOMMANDS
# --------------------

def location_list_short(connection, options):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email 
        FROM location_t 
        WHERE location_id > -1 
        ORDER BY location_id ASC
        ''')
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email"], size=100)

def location_list_long(connection, options):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email,organization,zipcode,location,province,country,timezone
        FROM location_t 
        WHERE location_id > -1 
        ORDER BY location_id ASC
        ''')
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email","Organization","ZIP Code","Location","Province","Country","Timezone"], size=100)


def location_single_list_short(connection, options):
    row = {}
    row['name']  = options.name
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email 
        FROM location_t 
        WHERE site = :name
        ''', row)
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email"], size=100)


def location_single_list_long(connection, options):
    row = {}
    row['name']  = options.name
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email,organization,zipcode,location,province,country,timezone
        FROM location_t 
        WHERE site = :name
        ''', row)
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email","Organization","ZIP Code","Location","Province","Country","Timezone"], size=100)


def location_list(connection, options):
    if options.name is not None:
        if options.extended:
            location_single_list_long(connection, options)
        else:
            location_single_list_short(connection, options)
    else:
        if options.extended:
            location_list_long(connection,options)
        else:
            location_list_short(connection,options)


def location_delete(connection, options):
    row = {'name': options.name}
    cursor = connection.cursor()
    # Fetch ithis location has been used
    cursor.execute('''
        SELECT COUNT(*) from tess_readings_t
        WHERE location_id = (SELECT location_id FROM location_t WHERE site == :name)
        ''', row)
    result = cursor.fetchone()
    if result[0] > 0:
        raise IndexError("Cannot delete. Existing readings with this site '%s' are already stored." % (options.name,) )
    cursor.execute(
        '''
        SELECT l.site,l.location_id,l.longitude,l.latitude,l.elevation
        FROM location_t AS l
        WHERE l.site == :name
        ''', row)
    paging(cursor,["Name","Id.","Longitude","Latitude","Elevation"], size=100)
    if not options.test:
        cursor.execute(
        '''
        DELETE
        FROM location_t
        WHERE site == :name
        ''', row)
    connection.commit()




def location_unassigned(connection, options):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT l.site,l.longitude,l.latitude,l.elevation,l.contact_name,l.contact_email 
        FROM location_t        AS l 
        LEFT OUTER JOIN tess_t AS i USING (location_id)
        WHERE i.name IS NULL;
        ''')
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email"], size=100)


def location_duplicates(connection, options):
    cursor = connection.cursor()
    row = {}
    row['distance'] = options.distance
    row['unknown'] = UNKNOWN
    cursor.execute(
        '''
        SELECT src.site, dst.site, ABS(src.latitude - dst.latitude) AS DLat, ABS(src.longitude - dst.longitude) as DLong
        FROM location_t AS src
        JOIN location_t AS dst
        WHERE  src.site      != dst.site
        AND    src.longitude != :unknown
        AND    dst.longitude != :unknown
        AND    src.latitude  != :unknown
        AND    src.longitude != :unknown
        AND DLat  <= (:distance*180.0)/(6371000.0*3.1415926535)
        AND DLong <= (:distance*180.0)/(6371000.0*3.1415926535)
        ''', row)
    paging(cursor,["Site A","Site B","Delta Latitude","Delta Longitude"], size=100)

# Location update is a nightmare if done properly, since we have to generate
# SQL updates tailored to the attributes being given in the command line

def location_create(connection, options):
    cursor = connection.cursor()
    row = {}
    row['site']      = options.site
    row['longitude'] = options.longitude
    row['latitude']  = options.latitude
    row['elevation'] = options.elevation
    row['zipcode']   = options.zipcode
    row['location']  = options.location
    row['province']  = options.province
    row['country']   = options.country
    row['email']     = options.email
    row['owner']     = options.owner
    row['org']       = options.org
    row['tzone']     = options.tzone
    # Fetch existing site
    cursor.execute(
        '''
        SELECT site 
        FROM   location_t 
        WHERE site == :site
        ''', row)
    result = cursor.fetchone()
    if result:
        raise IndexError("Cannot create. Existing site with name %s already exists." % (options.site,) )
    cursor.execute(
        '''
        INSERT INTO location_t (
            site,
            longitude, 
            latitude,
            elevation,
            zipcode,
            location,
            province,
            country,
            contact_email,
            contact_name,
            organization,
            timezone
        ) VALUES (
            :site,
            :longitude,
            :latitude,
            :elevation,
            :zipcode,
            :location,
            :province,
            :country,
            :email,
            :owner,
            :org,
            :tzone
            )
        ''',  row)
    connection.commit()
    # Read just written data
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email,organization,zipcode,location,province,country,timezone
        FROM location_t 
        WHERE site == :site
        ORDER BY location_id ASC
        ''', row)
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email","Organization","ZIP Code","Location","Province","Country","Timezone"], size=5)



def location_update(connection, options):
    cursor = connection.cursor()
    row = {} 
    row['site'] = options.site
   
    # Fetch existing site
    cursor.execute(
        '''
        SELECT site 
        FROM   location_t 
        WHERE site == :site
        ''', row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot update. Site with name %s does not exists." % (options.site,) )
    
    if options.longitude is not None:
        row['longitude'] = options.longitude
        cursor.execute(
        '''
        UPDATE location_t SET longitude = :longitude WHERE site == :site
        ''', row)

    if options.latitude is not None:
        row['latitude']  = options.latitude
        cursor.execute(
        '''
        UPDATE location_t SET latitude = :latitude WHERE site == :site
        ''', row)
        
    if options.elevation is not None:
        row['elevation'] = options.elevation
        cursor.execute(
        '''
        UPDATE location_t SET elevation = :elevation WHERE site == :site
        ''', row)

    if options.zipcode is not None:
        row['zipcode']   = options.zipcode
        cursor.execute(
        '''
        UPDATE location_t SET zipcode = :zipcode WHERE site == :site
        ''', row)

    if options.location is not None:
        row['location']  = options.location
        cursor.execute(
        '''
        UPDATE location_t SET location = :location WHERE site == :site
        ''', row)

    if options.province is not None:
        row['province']  = options.province
        cursor.execute(
        '''
        UPDATE location_t SET province = :province WHERE site == :site
        ''', row)

    if options.country is not None:
        row['country']  = options.country
        cursor.execute(
        '''
        UPDATE location_t SET country = :country WHERE site == :site
        ''', row)

    if options.email is not None:
        row['email']   = options.email
        cursor.execute(
        '''
        UPDATE location_t SET contact_email = :email WHERE site == :site
        ''', row)

    if options.owner is not None:
        row['owner']   = options.owner
        cursor.execute(
        '''
        UPDATE location_t SET contact_name = :owner WHERE site == :site
        ''', row)

    if options.org is not None:
        row['org']   = options.org
        cursor.execute(
        '''
        UPDATE location_t SET organization = :org WHERE site == :site
        ''', row)

    if options.tzone is not None:
        row['tzone']   = options.tzone
        cursor.execute(
        '''
        UPDATE location_t SET timezone = :tzone WHERE site == :site
        ''', row)

    connection.commit()
    # Read just written data
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email,organization,zipcode,location,province,country,timezone
        FROM location_t 
        WHERE site == :site
        ORDER BY location_id ASC
        ''', row)
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email","Organization","ZIP Code","Location","Province","Country","Timezone"], size=5)


def location_rename(connection, options):
    cursor = connection.cursor()
    row = {}
    row['newsite']  = options.new_site
    row['oldsite']  = options.old_site
    cursor.execute("SELECT site FROM location_t WHERE site == :oldsite", row)
    result = cursor.fetchone()
    if not result:
        raise IndexError("Cannot rename. Existing site with old name %s does not exist." 
            % (options.old_site,) )
    
    cursor.execute("SELECT site FROM location_t WHERE site == :newsite", row)
    result = cursor.fetchone()
    if result:
        raise IndexError("Cannot rename. New site %s already exists." % (result[0],) ) 
    cursor.execute("UPDATE location_t SET site = :newsite WHERE site == :oldsite", row)
    connection.commit()
    # Now display it
    cursor.execute(
        '''
        SELECT site,longitude,latitude,elevation,contact_name,contact_email,organization,zipcode,location,province,country,timezone
        FROM location_t 
        WHERE site == :newsite
        ''', row)
    paging(cursor,["Name","Longitude","Latitude","Elevation","Contact","Email","Organization","ZIP Code","Location","Province","Country","Timezone"], size=5)

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
