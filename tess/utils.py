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
import sqlite3
import os
import os.path
import datetime

# Python3 catch
try:
    raw_input
except:
    raw_input = input 

#--------------
# other imports
# -------------

import tabulate


#--------------
# local imports
# -------------

# ----------------
# Module constants
# ----------------

# ----------------
# package constants
# ----------------


# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------


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

