#!/bin/bash
# today's sunrise/sunset for all locations

# ------------------------------------------------------------------------------
#                             AUXILIARY FUNCTIONS
# ------------------------------------------------------------------------------

query_sunrise_data() {
dbase=$1
sqlite3 ${dbase} <<EOF
.mode line
SELECT name, site, sunrise, sunset
FROM tess_v   
WHERE valid_state = 'Current'
ORDER BY name ASC;
EOF
}
# ------------------------------------------------------------------------------


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
suffix=$(/bin/date +%Y%m%dT%H%M00)

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
if  [[ $operational_dbase = "yes" ]]; then
        echo "Pausing tessdb service."
    	/usr/local/bin/tessdb_pause 
		/bin/sleep 2
else
	echo "Using backup database, no need to pause tessdb service."
fi

query_sunrise_data ${dbase} > ${out_dir}/${name}.${suffix}.txt

# Resume background database I/O
if  [[ $operational_dbase = "yes" ]]; then
        echo "Resuming tessdb service."
    	/usr/local/bin/tessdb_resume 
else
	echo "Using backup database, no need to resume tessdb service."
fi
