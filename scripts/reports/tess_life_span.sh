#!/bin/bash
# Summary report by instrument

# ------------------------------------------------------------------------------
#                             AUXILIARY FUNCTIONS
# ------------------------------------------------------------------------------


report_by_tess() {
dbase=$1
sqlite3 -csv -header ${dbase} <<EOF 
.separator ;
SELECT m.name as tess, i.mac_address as mac, min(d.sql_date) as start, max(d.sql_date) AS end, count(*) AS readings
FROM name_to_mac_t AS m, tess_readings_t AS r
JOIN tess_t     AS i USING (tess_id)
JOIN date_t     AS d USING (date_id)
WHERE m.mac_address = i.mac_address 
GROUP BY m.name
ORDER BY CAST(substr(m.name, 6) as decimal) ASC;
EOF
}

# ------------------------------------------------------------------------------

# may be we need it ..
TODAY=$(date +%Y%m%d)

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

report_by_tess ${dbase} > ${out_dir}/${name}.csv

# Resume background database I/O
if  [[ operational_dbase="yes" ]]; then
        echo "Resuming tessdb service."
    	/usr/local/bin/tessdb_resume 
else
	echo "Using backup database, no need to resume tessdb service."
fi

