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

from . import DEFAULT_DBASE, UNKNOWN, INFINITE_TIME, EXPIRED, CURRENT
from . import OUT_OF_SERVICE, MANUAL, DEFAULT_AZIMUTH, DEFAULT_ALTITUDE
from . import TSTAMP_FORMAT, DEFAULT_START_DATE, DEFAULT_END_DATE

from .utils      import open_database

from .instrument import *
from .location   import *
from .readings   import *

# ----------------
# Module constants
# ----------------

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
    parser    = argparse.ArgumentParser(prog=name, description="tessdb command line tool")
    parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
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

    ico = subparser.add_parser('coalesce', help='coalesce redundant instrument ids')
    icoex = ico.add_mutually_exclusive_group(required=True)
    icoex.add_argument('-n', '--name', type=str, help='instrument name')
    icoex.add_argument('-m', '--mac',  type=str, help='instrument MAC')
    icoex.add_argument('-a', '--all', action='store_true', help='all instruments')
    ico.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    ico.add_argument('-t', '--test', action='store_true',  help='test only, do not delete')

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
    except Exception as e:
        print("Error => {0}".format( utf8(str(e)) ))
    finally:
        if invalid_cache:
            print("WARNING: Do not forget to issue 'service tessdb reload' afterwards to invalidate tessdb caches")

