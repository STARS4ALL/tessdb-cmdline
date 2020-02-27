
# GENERATE IDA-FORMAT OBSERVATIONS FILE

# ----------------------------------------------------------------------
# Copyright (c) 2017 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import glob
import re
import logging
import sys
import argparse
import sqlite3
import os
import os.path
import copy

import datetime



#--------------
# other imports
# -------------


#--------------
# local imports
# -------------

from . import __version__, TSTAMP_FORMAT


# ----------------
# Module constants
# ----------------

DEFAULT_DBASE  = "/var/dbase/tess.db"
DEFAULT_LOGDIR = "/var/log"


EVENTS = (
    {
        'name'    : 'started',
        'pattern' : r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[+-]\d{4} \[.+\] starting tessdb (\d{1,2}\.\d{1,2}\.\d{1,2}) on Python \d{1}\.\d{1} using (Twisted \d{1,2}\.\d{1,2}\.\d{1,2})',       
    },
    {
        'name'    : 'stopped',
        'pattern' : r'^^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[+-]\d{4} \[-\] Main loop terminated.',       
    },
    
)
EVENTS_PATTERNS = [ re.compile(event['pattern']) for event in EVENTS ]


# ---------------------
# Module global classes
# ---------------------


# -----------------------
# Module global functions
# -----------------------

def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser = argparse.ArgumentParser(prog=name, description="TESS Event log file parser " + __version__)
    parser.add_argument('-d', '--dbase',   default=DEFAULT_DBASE, help='SQLite database full file path')
    parser.add_argument('-t', '--testing', action='store_true', help='Testing environment.')
    parser.add_argument('-a', '--automatic', action='store_true', help='Launched automatically (i..e cron job).')
    parser.add_argument('--dry-run', action='store_true', help='Do not insert into the database.')
    group2 = parser.add_mutually_exclusive_group()
    group2.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
    group2.add_argument('-q', '--quiet',   action='store_true', help='Quiet output.')
    return parser

# -------------------
# AUXILIARY FUNCTIONS
# -------------------

       

def get_context(options):
    return {
        'source'     : os.path.split(os.path.dirname(sys.argv[0]))[-1],
        'environment': "testing"  if options.testing   else "production",
        'method'     : "cron job" if options.automatic else "manual",
    }


def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=level)


def open_database(dbase_path):
    if not os.path.exists(dbase_path):
       raise IOError("No SQLite3 Database file found at {0}. Exiting ...".format(dbase_path))
    logging.info("Opening database {0}".format(dbase_path))
    return sqlite3.connect(dbase_path)


def create_table(connection):
    logging.debug("creating event_log_t table")
    cursor = connection.cursor()
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS event_log_t
        (
            tstamp        TEXT NOT NULL,    -- ISO timestamp
            event         TEXT NOT NULL,    -- Event key ('started,'stopped', etc)
            environment   TEXT NOT NULL,    -- Database where it takes place: 'production', 'testing'
            source        TEXT NOT NULL,    -- Who produces the event, 'script', 'tessdb'
            scope         TEXT NOT NULL,    -- Which scope: 'global', 'stars1', etc.
            method        TEXT NOT NULL,    -- How it was produced: 'cron job', 'manual'
            comment       TEXT,              -- i.e. "tessdb 2.5.3, Twisted 10.10.0"
            PRIMARY KEY(tstamp, event, environment)
        );
        ''')
    connection.commit()


def match_event(line):
    '''Returns matched command descriptor or None'''
    for regexp in EVENTS_PATTERNS:
        matchobj = regexp.search(line)
        if matchobj:
            logging.debug("matched {0}".format(EVENTS[EVENTS_PATTERNS.index(regexp)]['name']))
            return EVENTS[EVENTS_PATTERNS.index(regexp)], matchobj
    return None, None


def process_line(line, context, accum):
    global event_list
    event, matchobj = match_event(line)
   
    context['tstamp']  = None
    context['scope']   = None
    context['comment'] = None
    if not event:
        return
    context['event'] = event['name']
    if event['name'] == 'started':
        context['tstamp'] = matchobj.group(1)
        context['scope']   = "global"
        context['comment'] = "tessdb " + matchobj.group(2) + ', ' + matchobj.group(3)
    elif event['name'] == 'stopped':
        context['tstamp'] = matchobj.group(1)
        context['scope']   = "global"
        context['comment'] = None
    else:
        pass
    logging.debug(context)
    accum.append(copy.deepcopy(context))

def write_to_database(accum, connection):
    starts = [item for item in accum if item['event'] == 'started']
    stops  = [item for item in accum if item['event'] == 'stopped']
    logging.info("writting to database a list of {0} events, {1} 'started' and {2} 'stopped'".format(len(accum), len(starts), len(stops)))
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM event_log_t");
    n1 = cursor.fetchone()[0]
    for event in accum:
        cursor.execute(
            '''
            INSERT OR IGNORE INTO event_log_t (
                tstamp,
                event,
                environment,
                source,
                scope,
                method,
                comment
            ) VALUES (
                :tstamp,
                :event,
                :environment,
                :source,
                :scope,
                :method,
                :comment
            );
            ''', event)
    cursor.execute("SELECT COUNT(*) FROM event_log_t");
    n2 = cursor.fetchone()[0]
    logging.info("effectively written to database: {0} events".format(n2-n1))
    connection.commit()


# -------------
# MAIN FUNCTION
# -------------


def main():
    '''
    Main entry point
    '''
    try:
        options = createParser().parse_args(sys.argv[1:])
        configureLogging(options)
        context = get_context(options)
        connection = open_database(options.dbase)
        create_table(connection)
        event_list = []
        for logfile_path in sorted(glob.glob("/var/log/tessdb*")):
            with open(logfile_path,'r') as fd:
                logging.debug("processing file {0}".format(logfile_path))
                for line in fd:
                    process_line(line, context, event_list)
        if not options.dry_run and len(event_list):
            write_to_database(event_list, connection)
    except KeyboardInterrupt:
        logging.exception('{0}: Interrupted by user ^C'.format(name))
    except Exception as e:
        logging.exception("Error => {0}".format(e))