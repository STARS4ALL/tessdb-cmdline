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

from . import __version__
from .utils import url

# ----------------
# Module constants
# ----------------

LOG_CHOICES = ('critical', 'error', 'warn', 'info', 'debug')

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger()
package = __name__.split(".")[0]

# ----------
# Exceptions
# ----------


# ------------------------
# Module utility functions
# ------------------------

def configure_logging(options):
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
        ch.setLevel(logging.DEBUG) # All logging handles to the MAX level
        log.addHandler(ch)
    # Create a file handler suitable for logrotate usage
    if options.log_file:
        #fh = logging.handlers.WatchedFileHandler(options.log_file)
        fh = logging.handlers.TimedRotatingFileHandler(options.log_file, when='midnight', interval=1, backupCount=365)
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG) # All logging handles to the MAX level
        log.addHandler(fh)

    if options.modules:
        modules = options.modules.split(',')
        print(modules)
        for module in modules:
            logging.getLogger(module).setLevel(logging.DEBUG)


def valid_file(path):
    if not os.path.isfile(path):
        raise IOError(f"Not valid or existing file: {path}")
    return path

def valid_dir(path):
    if not os.path.isdir(path):
        raise IOError(f"Not valid or existing directory: {path}")
    return path

           
# -----------------------
# Module global functions
# -----------------------


def create_parser():
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
    parser.add_argument('-m', '--modules', type=str, default=None, action='store', help='comma separated list of modules to activate debug level upon')

    # --------------------------
    # Create first level parsers
    # --------------------------

    subparser = parser.add_subparsers(dest='command')

    parser_ida  = subparser.add_parser('idadb', help='idadb commands')
    parser_forms  = subparser.add_parser('forms', help='Google forms commands')

    # --------------------------------------
    # Create second level parsers for 'forms'
    # ---------------------------------------

    subparser = parser_forms.add_subparsers(dest='subcommand')
    forms = subparser.add_parser('check',  help="Generate cross IDA/tessdb CSV comparison")
    forms.add_argument('-f', '--file', type=str, required=True, help='Input Google Forms CSV File')
    forms = forms.add_mutually_exclusive_group(required=True)
    forms.add_argument('--common', action='store_true', help='Common Photometers in Google Forms and in MongoDB')
    forms.add_argument('--google', action='store_true', help='Photometers in Google Forms not in MongoDB')

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

    return parser


# ================ #
# MAIN ENTRY POINT #
# ================ #


def main():
    """
    Utility entry point
    """
    options = argparse.Namespace()
    exit_code = 0
    try:
        options = create_parser().parse_args(sys.argv[1:], namespace=options)
        configure_logging(options)
        log.info(f"============== {__name__} {__version__} ==============")
        command = f"{options.command}"
        subcommand = f"{options.subcommand}"
        try:
            log.debug("loading command from module %s in package %s", command, package)
            command = importlib.import_module(command, package=package)
        except ModuleNotFoundError:  # when debugging module in git source tree ...
            command = f".{options.command}"
            log.debug("retrying loading command from module %s in package %s", command, package)
            command = importlib.import_module(command, package=package)
        getattr(command, subcommand)(options)
    except AttributeError:
            log.critical("[%s] No subcommand was given ", __name__)
            traceback.print_exc()
            exit_code = 1
    except KeyboardInterrupt:
        log.critical("[%s] Interrupted by user ", __name__)
    except Exception as e:
        log.critical("[%s] Fatal error => %s", __name__, str(e))
        traceback.print_exc()
    finally:
        pass
    sys.exit(exit_code)
