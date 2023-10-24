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
import argparse
import logging
import traceback
import importlib


#--------------
# local imports
# -------------

from . import __version__, DEFAULT_DBASE
from .utils import url

# ----------------
# Module constants
# ----------------

LOG_CHOICES = ('critical', 'error', 'warn', 'info', 'debug')

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger('root')

# ----------
# Exceptions
# ----------


# ------------------------
# Module utility functions
# ------------------------

def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.ERROR
    else:
        level = logging.INFO
    
    log.setLevel(level)
    # Log formatter
    #fmt = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(message)s')
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    # create console handler and set level to debug
    if options.console:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        ch.setLevel(level)
        log.addHandler(ch)
    # Create a file handler suitable for logrotate usage
    if options.log_file:
        #fh = logging.handlers.WatchedFileHandler(options.log_file)
        fh = logging.handlers.TimedRotatingFileHandler(options.log_file, when='midnight', interval=1, backupCount=365)
        fh.setFormatter(fmt)
        fh.setLevel(level)
        log.addHandler(fh)

def validfile(path):
    if not os.path.isfile(path):
        raise IOError(f"Not valid or existing file: {path}")
    return path

def validdir(path):
    if not os.path.isdir(path):
        raise IOError(f"Not valid or existing directory: {path}")
    return path

           
# -----------------------
# Module global functions
# -----------------------


def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser    = argparse.ArgumentParser(prog=name, description='Location utilities for TESS-W')

    # Global options
    parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
    parser.add_argument('-x', '--exceptions', action='store_true',  help='print exception traceback when exiting.')
    parser.add_argument('-c', '--console', action='store_true',  help='log to console.')
    parser.add_argument('-l', '--log-file', type=str, default=None, action='store', metavar='<file path>', help='log to file')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='store_true', help='Verbose logging output.')
    group.add_argument('-q', '--quiet',   action='store_true', help='Quiet logging output.')


    # --------------------------
    # Create first level parsers
    # --------------------------

    subparser = parser.add_subparsers(dest='command')

    parser_location  = subparser.add_parser('location', help='location commands')
    parser_mongodb = subparser.add_parser('mongodb', help='MongoDB commands')
    parser_tessdb  = subparser.add_parser('tessdb', help='TessDB commands')
    parser_crossdb = subparser.add_parser('crossdb', help='Cross database check commands')
    parser_zptess  = subparser.add_parser('zptess', help='zptess commands')
    parser_ida  = subparser.add_parser('idadb', help='idadb commands')

    # -------------------------------------
    # Create second level parsers for 'ida'
    # -------------------------------------

    subparser = parser_ida.add_subparsers(dest='subcommand')
    
    ida = subparser.add_parser('generate',  help="Generate cross IDA/tessdb CSV comparison")
    ida.add_argument('-f', '--file', type=str, required=True, help='Output CSV File')
    idaex1 = ida.add_mutually_exclusive_group(required=True)
    idaex1.add_argument('--common', action='store_true', help='Common MACs')
    idaex1.add_argument('--ida', action='store_true', help='MACs in IDA CSV file not in TESSDB')
    idaex1.add_argument('--tessdb', action='store_true', help='MACs in TESSDB not in IDA CSV')

    # ------------------------------------------
    # Create second level parsers for 'zptess'
    # ------------------------------------------

    subparser = parser_zptess.add_subparsers(dest='subcommand')
    
    zpt = subparser.add_parser('generate',  help="Generate cross zptess/tessdb CSV comparison")
    zpt.add_argument('-f', '--file', type=str, required=True, help='Output CSV File')
    zpex1 = zpt.add_mutually_exclusive_group(required=True)
    zpex1.add_argument('--common', action='store_true', help='Common MACs')
    zpex1.add_argument('--zptess', action='store_true', help='MACs in ZPTESS not in TESSDB')
    zpex1.add_argument('--tessdb', action='store_true', help='MACs in TESSDB not in ZPTESS')

    zpex1 = zpt.add_mutually_exclusive_group(required=True)
    zpex1.add_argument('-c', '--current', action='store_true', help='Current ZP')
    zpex1.add_argument('-i', '--historic', action='store_true', help='Historic ZP entries')


    # ------------------------------------------
    # Create second level parsers for 'location'
    # ------------------------------------------

    subparser = parser_location.add_subparsers(dest='subcommand')
    
    locg = subparser.add_parser('generate',  help="Generate tessdb location creation script")
    locg.add_argument('-d', '--dbase', type=validfile, default=DEFAULT_DBASE, help='SQLite database full file path')
    locg.add_argument('-i', '--input-file', type=validfile, required=True, help='Input CSV file')
    locg.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    # -----------------------------------------
    # Create second level parsers for 'mongodb'
    # -----------------------------------------

    subparser = parser_mongodb.add_subparsers(dest='subcommand')

    mgloc = subparser.add_parser('location',  help="MongoDB location metadata operations")
    mgloc.add_argument('-f', '--file', type=str, required=True, help='Input (for update) / Output (for list) CSV file')
    mgloc.add_argument('-n', '--names', type=str, nargs='+', default=None, required=False, help='Optional names filter')
    mgloc.add_argument('--delimiter', type=str,  default=';', help='Optional column delimiter for CSV I/O (semicolon by default)')
    mgex1 = mgloc.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-l', '--list', action='store_true', help='List MongoDB location data')
    mgex1.add_argument('-m', '--nominatim', action='store_true', help='List MongoDB location + Nominatim metadata')
    mgex1.add_argument('-u', '--update', action='store_true', help='Update MongoDB location metadata')
    mgex1.add_argument('-s', '--sim-update', action='store_true', help='(simulated) Update MongoDB location metadata')
  

    mgphot = subparser.add_parser('photometer',  help="MongoDB photometer metadata operations")
    mgphot.add_argument('-f', '--file', type=str, required=True, help='Input (for update) / Output (for list) CSV file')
    mgphot.add_argument('-n', '--names', type=str, nargs='+', default=None, required=False, help='Optional names filter')
    mgphot.add_argument('--delimiter', type=str,  default=';', help='Optional column delimiter for CSV I/O (semicolon by default)')
    mgex1 = mgphot.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-l', '--list', action='store_true', help='List MongoDB photometer data')
    mgex1.add_argument('-u', '--update', action='store_true', help='Update MongoDB photometer metadata')
    mgex1.add_argument('-c', '--create', action='store_true', help='Create MongoDB photometer metadata')
    mgex1.add_argument('-x', '--sim-create', action='store_true', help='(simulated) Create MongoDB photometer metadata')
    mgex1.add_argument('-s', '--sim-update', action='store_true', help='(simulated) Update MongoDB photometer metadata')
    mgphot.add_argument('-m', '--mac', type=str, default=None, required=False, help='(Optional) old MAC, needed only to change MAC')
    
    mgorg = subparser.add_parser('organization',  help="MongoDB organiaztion metadata operations")
    mgorg.add_argument('-f', '--file', type=str, required=True, help='Input (for update) / Output (for list) CSV file')
    mgorg.add_argument('-n', '--names', type=str, nargs='+', default=None, required=False, help='Optional names filter')
    mgorg.add_argument('--delimiter', type=str,  default=';', help='Optional column delimiter for CSV I/O (semicolon by default)')
    mgex1 = mgorg.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-l', '--list', action='store_true', help='List MongoDB organization metadata')
    mgex1.add_argument('-u', '--update', action='store_true', help='Update MongoDB organization metadata')
    mgex1.add_argument('-s', '--sim-update', action='store_true', help='(simulated) Update MongoDB organization metadata')

    mgcon = subparser.add_parser('contact',  help="MongoDB contact metadata operations")
    mgcon.add_argument('-f', '--file', type=str, required=True, help='Input (for update) / Output (for list) CSV file')
    mgcon.add_argument('-n', '--names', type=str, nargs='+', default=None, required=False, help='Optional names filter')
    mgcon.add_argument('--delimiter', type=str,  default=';', help='Optional column delimiter for CSV I/O (semicolon by default)')
    mgex1 = mgcon.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-l', '--list', action='store_true', help='List MongoDB contact metadata')
    mgex1.add_argument('-u', '--update', action='store_true', help='Update MongoDB contact metadata')
    mgex1.add_argument('-s', '--sim-update', action='store_true', help='(simulated) Update MongoDB contact metadata')

    mgall = subparser.add_parser('all',  help="MongoDB all metadata operations")
    mgall.add_argument('-f', '--file', type=str, required=True, help='Input (for update) / Output (for list) CSV file')
    mgall.add_argument('-n', '--names', type=str, nargs='+', default=None, required=False, help='Optional names filter')
    mgall.add_argument('--delimiter', type=str,  default=';', help='Optional column delimiter for CSV I/O (semicolon by default)')
    mgex1 = mgall.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-l', '--list', action='store_true', help='List all MongoDB metadata')
    mgex1.add_argument('-d', '--diff-file', type=validfile, help='Diff between Mongo and a backup input CSV file. Generates 4 files.')
    mgex1.add_argument('-u', '--update', action='store_true', help='Update all MongoDB metadata')
    mgex1.add_argument('-c', '--create', action='store_true', help='Create MongoDB photometer metadata')
    mgex1.add_argument('-s', '--sim-update', action='store_true', help='(simulated) Update MongoDB all metadata')
    mgex1.add_argument('-x', '--sim-create', action='store_true', help='(simulated) Create MongoDB photometer metadata')
    
    mgphck = subparser.add_parser('check',  help="Various MongoDB metadata checks")
    mgphck.add_argument('--delimiter', type=str,  default=';', help='Optional column delimiter for CSV I/O (semicolon by default)')
    mgex1 = mgphck.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-n', '--names', action='store_true', help='Check for duplicate photometer names')
    mgex1.add_argument('-m', '--macs', action='store_true', help='Check for duplicate MACs')
    mgex1.add_argument('-a', '--mac-format', action='store_true', help='Check for properly formatted MACs')
    mgex1.add_argument('-p', '--places', action='store_true', help='Check same places, different coordinates')
    mgex1.add_argument('-c', '--coords', action='store_true', help='Check same coordinates, different places')
    mgex1.add_argument('-b', '--nearby', type=float, default=0, help='Check for nearby places, distance in meters')
    mgex1.add_argument('-u', '--utc', action='store_true', help='Check for Etc/UTC* timezone')
    mgex1.add_argument('-i', '--filter', action='store_true', help='Check for "UV/IR-cut" string in filters')
    mgex1.add_argument('-z', '--zero-point', action='store_true', help='Check for defined zero points')


    # -----------------------------------------
    # Create second level parsers for 'tessdb'
    # -----------------------------------------

    subparser = parser_tessdb.add_subparsers(dest='subcommand')

    tdloc = subparser.add_parser('locations',  help="TessDB locations metadata check")
    tdloc.add_argument('-d', '--dbase', type=validfile, required=True, help='TessDB database file path')
    tdloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    tdphot = subparser.add_parser('photometer',  help="TessDB photometers metadata check")
    tdphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')


    tdcheck = subparser.add_parser('check',  help="Various MongoDB metadata checks")
    tdex1 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex1.add_argument('-p', '--places', action='store_true', help='Check same places, different coordinates')
    tdex1.add_argument('-c', '--coords', action='store_true', help='Check same coordinates, different places')
    tdex1.add_argument('-d', '--dupl', action='store_true', help='Check same coordinates, duplicated places')
    tdex1.add_argument('-b', '--nearby', type=float, default=0, help='Check for nearby places, distance in meters')
    tdex1.add_argument('-m', '--macs', action='store_true', help='Check for proper MACS in tess_t')
    tdex1.add_argument('-z', '--fake-zero-points', action='store_true', help='Check for proper MACS in tess_t')

    # -----------------------------------------
    # Create second level parsers for 'crossdb'
    # -----------------------------------------

    subparser = parser_crossdb.add_subparsers(dest='subcommand')
    
    xdbloc = subparser.add_parser('locations',  help="Cross DB locations metadata check")
    xdbloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    grp = xdbloc.add_mutually_exclusive_group(required=True)
    grp.add_argument('-m', '--mongo', action='store_true', help='MongoDB exclusive locations')
    grp.add_argument('-t', '--tess', action='store_true',  help='TessDB exclusive locations')
    grp.add_argument('-c', '--common', action='store_true',  help='TessDB exclusive locations')

    xdbphot = subparser.add_parser('photometers',  help="Cross DB photometers metadata operations")
    xdbphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    grp = xdbphot.add_mutually_exclusive_group(required=True)
    grp.add_argument('-s', '--sim-update-mac', action='store_true', help='Simulated update Mongo DB MAC with TESS-DB MAC value')
    grp.add_argument('-m', '--update-mac', action='store_true',  help='Update Mongo DB MAC with TESS-DB MAC value')
    grp.add_argument('-x', '--sim-update-zp', action='store_true', help='Simulated update Mongo DB ZP with TESS-DB ZP value')
    grp.add_argument('-z', '--update-zp', action='store_true',  help='Update Mongo DB ZP with TESS-DB ZP value')

    xdbcoord = subparser.add_parser('coordinates',  help="Cross DB photometers metadata check")
    xdbcoord.add_argument('-f', '--file', type=str, required=True, help='CSV file to generate differences')
    xdbcoord.add_argument('--lower', type=float, default=0.0, help='Lower limit in meters')
    xdbcoord.add_argument('--upper', type=float, default=1000.0, help='Upper limit in meters')

    mgphck = subparser.add_parser('check',  help="Various MongoDB metadata checks")
    mgex1 = mgphck.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-m', '--mac', action='store_true', help="Check for common photometer's MACs")
    mgex1.add_argument('-z', '--zero-point', action='store_true', help="Check for common photometer's Zero Points")

    return parser


# ================ #
# MAIN ENTRY POINT #
# ================ #


def main():
    '''
    Utility entry point
    '''
    options = argparse.Namespace()
    exit_code = 0
    try:
        options = createParser().parse_args(sys.argv[1:], namespace=options)
        configureLogging(options)
        name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
        log.info(f"============== {name} {__version__} ==============")
        package = f"{name}"
        command  = f"{options.command}"
        subcommand = f"{options.subcommand}"
        try: 
            command = importlib.import_module(command, package=package)
        except ModuleNotFoundError: # when debugging module in git source tree ...
            command  = f".{options.command}"
            command = importlib.import_module(command, package=package)
        getattr(command, subcommand)(options)
    except KeyboardInterrupt as e:
        log.critical("[%s] Interrupted by user ", __name__)
        exit_code = 1
    except Exception as e:
        if(options.exceptions):
            traceback.print_exc()
        log.critical("[%s] Fatal error => %s", __name__, str(e) )
        exit_code = 1
    finally:
        pass
    sys.exit(exit_code)

main()
