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

#--------------
# local imports
# -------------

from . import DEFAULT_DBASE, UNKNOWN, INFINITE_TIME, EXPIRED, CURRENT
from . import OUT_OF_SERVICE, MANUAL, DEFAULT_AZIMUTH, DEFAULT_ALTITUDE
from . import TSTAMP_FORMAT, DEFAULT_START_DATE, DEFAULT_END_DATE

from .utils import paging

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

