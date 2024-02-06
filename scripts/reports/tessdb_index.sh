#!/bin/bash
# This script creates indices for reports.

DEFAULT_DATABASE="/var/dbase/tess.db"

# Arguments from the command line & default values

# Either the default or the rotated tess.db-* database
dbases="${1:-$DEFAULT_DATABASE}"
# wildcard expansion for the logrotared backups ...
dbases="$(ls -1 $dbases)"


# ------------------------------------------------------------------------------
#                             AUXILIARY FUNCTIONS
# ------------------------------------------------------------------------------

create_indices() {
dbase=$1
sqlite3 ${dbase} <<EOF
-- Create a covering index for locations
CREATE INDEX IF NOT EXISTS tess_readings_i ON tess_readings_t(tess_id, date_id, time_id, location_id);
EOF
}

# ------------------------------------------------------------------------------- #

for dbase in $dbases; do
    if  [[ ! -f $dbase || ! -r $dbase ]]; then
        echo "Database file $dbase does not exists or is not readable."
        echo "Exiting"
        exit 1
    fi
done

for dbase in $dbases; do
    echo "Generating indexes for queries in database ${dbase}"
    create_indices ${dbase}
done
