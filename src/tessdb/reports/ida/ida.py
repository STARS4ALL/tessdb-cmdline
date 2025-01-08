
# GENERATE IDA-FORMAT OBSERVATIONS FILE

# ----------------------------------------------------------------------
# Copyright (c) 2017 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import os
import datetime
import logging
import functools

from dateutil.relativedelta import relativedelta

#--------------
# other imports
# -------------

import pytz
import decouple

from lica.cli import execute
from lica.sqlite import open_database
from lica.validators import vmonth, vfile, vdir
from lica.jinja2 import render_from

#--------------
# local imports
# -------------

from ..._version import __version__


from .. import MONTH_FORMAT, TSTAMP_FORMAT
from . import readings, metadata

# ----------------
# Module constants
# ----------------

DEFAULT_DBASE = "/var/dbase/tess.db"
DEFAULT_DIR   = "/var/dbase/reports/IDA"
IDA_TEMPLATE = 'IDA-template-4c.j2'

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

package = __name__.split('.')[0]

# ---------------------
# Module global classes
# ---------------------

class MonthIterator(object):

    def __init__(self, start_month, end_month):
        self.__month = start_month
        self.__end = end_month + relativedelta(months = +1)

    def __iter__(self):
        '''Make this this class an iterable'''
        return self

    def __next__(self):
        '''Make this this class an iterator'''
        m = self.__month
        if self.__month >= self.__end:
            raise StopIteration  
        self.__month += relativedelta(months = +1)
        return m

    def next(self):
        '''Puython 2.7 compatibility'''
        return self.__next__()


# -------------------
# AUXILIARY FUNCTIONS
# -------------------

def now_month():
    return datetime.datetime.now(datetime.timezone.utc).replace(day=1,hour=0,minute=0,second=0,microsecond=0)


def result_generator(name, cursor, arraysize=5000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        log.debug("[%s]: Fetched %d readings", name, len(results))
        for result in results:
            yield result

def createMonthList(args):
    if args.latest_month:
        start_month  = now_month()
        end_month   = start_month
    elif args.previous_month:
        start_month  = now_month() + relativedelta(months = -1)
        end_month    = start_month
    elif args.for_month:
        start_month = args.for_month
        end_month   = start_month
    else:
        # cant compare naive dateinfo with toimeaware dateinfo
        dt = args.from_month
        start_month  = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, tzinfo=datetime.timezone.utc)
        end_month    = now_month()
    return MonthIterator(start_month, end_month)



def render_readings_line(dbreading, timezone):
    tzobj = pytz.timezone(timezone)
    dt = datetime.datetime.strptime(dbreading[0], TSTAMP_FORMAT).replace(tzinfo=pytz.utc)
    local_dt = dt.astimezone(tzobj).strftime(TSTAMP_FORMAT)
    aList = dbreading[:1] + (local_dt,) + dbreading[1:]
    aList = tuple(str(item) for item in aList)
    return ";".join(aList)


# Specialize Jinja2 rendering function with the parent package where to find packages
render = functools.partial(render_from, package)


def create_directories(instrument_name, out_dir, year=None):
    sub_dir = os.path.join(out_dir, instrument_name)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    if not os.path.exists(sub_dir):
        os.makedirs(sub_dir)
   
# -------------------
# IDA FILE Generation
# -------------------

def write_IDA_header_file(header, file_path):
    '''Writes the IDA header file after contained in result'''
    header = header.decode('utf-8')
    with open(file_path, 'w') as outfile:
        outfile.write(header)

def write_IDA_body_file(name, month, cursor, timezone, file_path):
    with open(file_path, 'a') as outfile:
        for reading in result_generator(name, cursor):
            body_line = render_readings_line(reading, timezone)
            outfile.write(body_line)
            outfile.write('\n')

# -------------
# MAIN FUNCTION
# -------------

def write_IDA_file(name, month, location_id, connection, output_base_dir, is_single):
    
    # Render one IDA file per location 
    # in case the TESS nstrument has changed location during the given month
    context = {}
    create_directories(name, output_base_dir)
    log.debug("[%s]: Fetching location metadata from the database", name)
    context['location']   = metadata.location(location_id, connection)
    log.debug("[%s]: Fetching instrument metadata from the database", name)
    context['instrument'] = metadata.instrument(name, month, location_id, connection)
    log.debug("[%s]: Fetching observer metadata from the database", name)
    context['observer']   = metadata.observer(month, connection)
    timezone = context['location']['timezone']
    header = render(IDA_TEMPLATE, context).encode('utf-8')
    suffix = '_' + str(location_id) if not is_single else ''
    file_name = name + "_" + month.strftime(MONTH_FORMAT) + suffix + ".dat"
    file_path = os.path.join(output_base_dir, name, file_name)
    log.debug("[%s]: Fetching readings from the database, location_id %d", name, location_id)
    cursor = readings.fetch(name, month, location_id, connection)
    log.info("[%s]: saving on to file %s", name, file_name)
    write_IDA_header_file(header, file_path)
    write_IDA_body_file(name, month, cursor, timezone, file_path)

# -----------------------
# AUXILIARY MAIN FUNCTION
# -----------------------

def ida(args):
    '''
    Main entry point
    '''
    output_base_dir = decouple.config('IDA_BASE_DIR') if args.out_dir is None else args.out_dir
    connection, db_path = open_database(args.dbase, env_var='TESSDB_URL')
    log.info("database opened on %s", db_path)
    name = args.name        
    month_list = createMonthList(args)
    for month in month_list:
        date = month.strftime(MONTH_FORMAT) # For printing purposes
        log.debug("[%s]: Counting available data on month %s", name, date)
        per_location_list = readings.available(name, month, connection)
        nlocations = len(per_location_list)
        log.debug("[%s]: %d readings in month %s", name, nlocations, date)
        if nlocations > 0:
            is_single = nlocations == 1
            for count, location_id, site in per_location_list:
                site = site.decode('utf-8') if type(site) is bytes else site 
                write_IDA_file(name, month, location_id, connection, output_base_dir, is_single)
                log.info("[%s]: Generating %s monthly IDA file with %d samples for location '%s'", name, date, count, site)
        else:
            log.info("[%s]: No data for month %s: skipping subdirs creation and IDA file generation", name, date)
    

# ===================================
# MAIN ENTRY POINT SPECIFIC ARGUMENTS
# ===================================

def add_args(parser):
    parser.add_argument('name', metavar='<name>', help='TESS instrument name')
    parser.add_argument('-d', '--dbase',   type=vfile, default=None, help='SQLite database full file path')
    parser.add_argument('-o', '--out_dir', type=vdir, default=None, help='Output directory to dump record')
    group1 = parser.add_mutually_exclusive_group(required=True)
    group1.add_argument('-m', '--for-month',  type=vmonth, metavar='<YYYY-MM>', help='Given year & month. Defaults to current.')
    group1.add_argument('-f', '--from-month', type=vmonth, metavar='<YYYY-MM>', help='Starting year & month')
    group1.add_argument('-l', '--latest-month', action='store_true', help='Latest month only.')
    group1.add_argument('-p', '--previous-month', action='store_true', help='previous month only.')

# ================
# MAIN ENTRY POINT
# ================

def main():
    execute(main_func=ida, 
        add_args_func=add_args, 
        name=__name__, 
        version=__version__,
        description ="Export TESS data to monthly IDA files"
    )