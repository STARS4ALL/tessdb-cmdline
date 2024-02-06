#!/bin/bash
# This script dumps latest month readings from every TESS given in an instrument list file.

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
ORDER by CAST(substr(name, 6) as decimal) ASC;
EOF
}

# ------------------------------------------------------------------------------- #


DEFAULT_DATABASE="/var/dbase/tess.db"
DEFAULT_REPORTS_DIR="/var/dbase/reports/IDA"
DEFAULT_MONTH="latest"

# get the name from the script name without extensions
name=$(basename ${0%.sh})

# Either the default or the rotated tess.db-* database
dbase="${1:-$DEFAULT_DATABASE}"
# wildcard expansion ...
dbase="$(ls -1 $dbase)"

# Output directory is created if not exists inside the inner script
out_dir="${2:-$DEFAULT_REPORTS_DIR}"

# Month: either "latest", "previous" or "YYYY-MM"
month="${3:-$DEFAULT_MONTH}"


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
if  [[ $operational_dbase = "yes" ]]; then
        echo "Pausing tessdb service."
    	/usr/local/bin/tessdb_pause 
		/bin/sleep 2
else
	echo "Using backup database, no need to pause tessdb service."
fi

photometers=$(query_names ${dbase})
# Loops over the instruments file and dumping data
for instrument in $photometers; do
    if [[ "$month" = "latest" ]];
    then
        echo "Generating latest month IDA file for TESS $instrument under ${out_dir}/${instrument}"
        /usr/local/bin/tess_ida ${instrument} -l -d ${dbase}  -o ${out_dir}
    elif [[ "$month" = "previous" ]];
    then
        echo "Generating previous month IDA file for TESS $instrument under ${out_dir}/${instrument}"
        /usr/local/bin/tess_ida ${instrument} -p -d ${dbase}  -o ${out_dir}
    else
        echo "Generating IDA file for TESS $instrument for month $month under ${out_dir}/${instrument}"
        /usr/local/bin/tess_ida ${instrument} -m $month -d ${dbase}  -o ${out_dir}
    fi
done


# Resume background database I/O
if  [[ $operational_dbase = "yes" ]]; then
        echo "Resuming tessdb service."
    	/usr/local/bin/tessdb_resume 
else
	echo "Using backup database, no need to resume tessdb service."
fi
