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
        level = logging.WARN
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
    parser_mongo  = subparser.add_parser('mongo', help='MongoDB commands')
    
    # ------------------------------------------
    # Create second level parsers for 'location'
    # ------------------------------------------

    subparser = parser_location.add_subparsers(dest='subcommand')
    locg = subparser.add_parser('generate',  help="Generate location creation script")
    locg.add_argument('-d', '--dbase', type=validfile, default=DEFAULT_DBASE, help='SQLite database full file path')
    locg.add_argument('-i', '--input-file', type=validfile, required=True, help='Input CSV file')
    locg.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

    # ---------------------------------------
    # Create second level parsers for 'mongo'
    # ---------------------------------------

    subparser = parser_mongo.add_subparsers(dest='subcommand')
    mgcommon = subparser.add_parser('intersect',  help="common coordinates between MongoDB and tessDB")
    mgcommon.add_argument('-d', '--dbase', type=validfile, default=DEFAULT_DBASE, help='SQLite tessDB database full file path')
    mgcommon.add_argument('-u', '--url', type=str, required=True, help='API URL for MongoDB queries')
    mgcommon.add_argument('-o', '--output-prefix', type=str, required=True, help='Output file prefix for the different files to generate')

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
