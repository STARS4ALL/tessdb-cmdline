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
    
    # ------------------------------------------
    # Create second level parsers for 'location'
    # ------------------------------------------

    subparser = parser_location.add_subparsers(dest='subcommand')
    locg = subparser.add_parser('generate',  help="Generate location creation script")
    locg.add_argument('-d', '--dbase', type=validfile, default=DEFAULT_DBASE, help='SQLite database full file path')
    locg.add_argument('-i', '--input-file', type=validfile, required=True, help='Input CSV file')
    locg.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    # -----------------------------------------
    # Create second level parsers for 'mongodb'
    # -----------------------------------------

    subparser = parser_mongodb.add_subparsers(dest='subcommand')
    mgloc = subparser.add_parser('locations',  help="MongoDB locations metadata check")
    mgloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    mgphot = subparser.add_parser('photometers',  help="MongoDB photometers metadata check")
    mgphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')


    mgloc = subparser.add_parser('coordinates',  help="Show same places with different coordinates")
    mgloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')


    mgupd = subparser.add_parser('update',  help="MongoDB locations metadata check")
    mgupd.add_argument('-i', '--input-file', type=validfile, required=True, help='Input CSV')
    mgex1 = mgupd.add_mutually_exclusive_group(required=True)
    mgex1.add_argument('-n', '--name', type=str, help='Only one name in CSV')
    mgex1.add_argument('-a', '--all',  action='store_true', help='All photometers in CSV')
    mgex2 = mgupd.add_mutually_exclusive_group(required=True)
    mgex2.add_argument('-l', '--loc',  action='store_true', help='Location info')
    mgex2.add_argument('-t', '--tess', action='store_true', help='Tess info')
    mgex2.add_argument('-m', '--img',  action='store_true', help='Image info')
    mgex2.add_argument('-o', '--org',  action='store_true', help='Organization info')
   

    # -----------------------------------------
    # Create second level parsers for 'tessdb'
    # -----------------------------------------

    subparser = parser_tessdb.add_subparsers(dest='subcommand')
    tdloc = subparser.add_parser('locations',  help="TessDB locations metadata check")
    tdloc.add_argument('-d', '--dbase', type=validfile, required=True, help='TessDB database file path')
    tdloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    tdphot = subparser.add_parser('photometers',  help="TessDB photometers metadata check")
    tdphot.add_argument('-d', '--dbase', type=validfile, required=True, help='TessDB database file path')
    tdphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    # -----------------------------------------
    # Create second level parsers for 'crossdb'
    # -----------------------------------------

    subparser = parser_crossdb.add_subparsers(dest='subcommand')
    xdbloc = subparser.add_parser('locations',  help="Cross DB locations metadata check")
    xdbloc.add_argument('-d', '--dbase', type=validfile, required=True, help='TessDB database file path')
    xdbloc.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    grp = xdbloc.add_mutually_exclusive_group(required=True)
    grp.add_argument('-m', '--mongo', action='store_true', help='MongoDB exclusive locations')
    grp.add_argument('-t', '--tess', action='store_true',  help='TessDB exclusive locations')
    grp.add_argument('-c', '--common', action='store_true',  help='TessDB exclusive locations')

    xdbphot = subparser.add_parser('photometers',  help="Cross DB photometers metadata check")
    xdbphot.add_argument('-d', '--dbase', type=validfile, required=True, help='TessDB database file path')
    xdbphot.add_argument('-u', '--url', type=url, required=True, help='API URL for MongoDB queries')
    xdbphot.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    grp = xdbphot.add_mutually_exclusive_group(required=True)
    grp.add_argument('-m', '--mongo', action='store_true', help='MongoDB exclusive locations')
    grp.add_argument('-t', '--tess', action='store_true',  help='TessDB exclusive locations')
    grp.add_argument('-c', '--common', action='store_true',  help='TessDB exclusive locations')

    xdbcoord = subparser.add_parser('coordinates',  help="Cross DB photometers metadata check")
    xdbcoord.add_argument('-d', '--dbase', type=validfile, required=True, help='TessDB database file path')
    xdbcoord.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')
    xdbcoord.add_argument('--lower', type=float, default=0.0, help='Lower limit in meters')
    xdbcoord.add_argument('--upper', type=float, default=1000.0, help='Upper limit in meters')


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
