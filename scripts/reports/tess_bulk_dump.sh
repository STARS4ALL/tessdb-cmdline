#!/bin/bash
# This script dumps every reading from every TESS given in an instrument list file.

# ------------------------------------------------------------------------------
#                             AUXILIARY FUNCTIONS
# ------------------------------------------------------------------------------

query_names() {
dbase=$1
sqlite3 ${dbase} <<EOF
SELECT name 
FROM tess_v 
WHERE name like 'stars%' 
AND valid_state = 'Current' 
ORDER by name ASC;
EOF
}

# ------------------------------------------------------------------------------- #

bulk_dump_by_instrument() {
instrument_name=$1
sqlite3 -csv -header ${dbase} <<EOF
.separator ;
SELECT (d.julian_day + t.day_fraction) AS julian_day, (d.sql_date || 'T' || t.time || 'Z') AS timestamp, r.sequence_number, l.site, "${instrument_name}", i.mac_address, r.frequency, r.magnitude, i.zero_point, i.filter, r.sky_temperature, r.ambient_temperature, r.signal_strength
FROM tess_readings_t AS r
JOIN tess_t          AS i USING (tess_id)
JOIN location_t      AS l USING (location_id)
JOIN date_t          AS d USING (date_id)
JOIN time_t          AS t USING (time_id)
WHERE i.mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == "${instrument_name}")
ORDER BY r.date_id ASC, r.time_id ASC;
EOF
}

# ------------------------------------------------------------------------------- #

DEFAULT_DATABASE="/var/dbase/tess.db"
DEFAULT_REPORTS_DIR="/var/dbase/reports"

# Arguments from the command line & default values

# Either the default or the rotated tess.db-* database
dbase="${1:-$DEFAULT_DATABASE}"
# wildcard expansion ...
dbase="$(ls -1 $dbase)"

out_dir="${2:-$DEFAULT_REPORTS_DIR}"

# get the name from the script name without extensions
name=$(basename ${0%.sh})

if  [[ ! -f $dbase || ! -r $dbase ]]; then
        echo "Database file $dbase does not exists or is not readable."
        echo "Exiting"
        exit 1
fi

if  [[ ! -d $out_dir  ]]; then
        echo "Output directory $out_dir does not exists."
        echo "Exiting"
        exit 1
fi

dbname=$(basename $dbase)
oper_dbname=$(basename $DEFAULT_DATABASE)

if [[ "$dbname" = "$oper_dbname" ]]; then
    operational_dbase="yes"
else
    operational_dbase="no"
fi

# Stops background database I/O when using the operational database
if  [[ operational_dbase="yes" ]]; then
        echo "Pausing tessdb service."
    	/usr/local/bin/tessdb_pause 
		/bin/sleep 2
else
	echo "Using backup database, no need to pause tessdb service."
fi

photometers=$(query_names ${dbase})
# Loops over the instruments file and dumping data
for instrument in $photometers; do
        echo "Generating compresed CSV for TESS $instrument"
        bulk_dump_by_instrument ${instrument} ${dbase} | gzip > ${out_dir}/${instrument}.csv.gz
done

# Resume background database I/O
if  [[ operational_dbase="yes" ]]; then
        echo "Resuming tessdb service."
    	/usr/local/bin/tessdb_resume 
else
	echo "Using backup database, no need to resume tessdb service."
fi


