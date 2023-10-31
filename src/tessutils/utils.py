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

# ---------------------
# Third party libraries
# ---------------------

import validators
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

def url(string):
    if not validators.url(string):
        raise ValueError("Invalid URL: %s" % string)
    return string

def formatted_mac(mac):
    ''''Corrects TESS-W MAC strings to be properly formatted'''
    return ':'.join(f"{int(x,16):02X}" for x in mac.split(':'))

def is_tess_mac(mac):
    '''TESS-W MAC address do not contain with padding 0s'''
    mac_list = mac.split(':')
    result = True
    for x in mac_list:
        try:
            int(x,16)
        except:
            result = False
            break
    return result and len(mac_list) == 6

def is_mac(mac):
    '''Strict MAC address check'''
    return is_tess_mac(mac) and len(mac) == 17

def tessify_mac(mac):
    '''This is needed for SQL comparison in tessdb'''
    return ':'.join(f"{int(x,16):X}" for x in mac.split(':'))
    

# ==============
# DATABASE STUFF
# ==============

def open_database(path):
    if not os.path.exists(path):
        raise IOError("No SQLite3 Database file found in {0}. Exiting ...".format(path))
    return sqlite3.connect(path)
 

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

