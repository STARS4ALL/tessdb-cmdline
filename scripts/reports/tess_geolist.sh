#!/bin/bash
# This script dumps latest month readings from every TESS given in an instrument list file.

# ------------------------------------------------------------------------------
#                             AUXILIARY FUNCTIONS
# ------------------------------------------------------------------------------

help() {
    name=$(basename ${0%.sh})
    echo "Usage:"
    echo "$name -d <database path> -o <dst dir> -l <log file path>"
    echo "Defaults to:"
    echo "name -d $DEFAULT_DATABASE -o $DEFAULT_DST_DIR -l $DEFAULT_LOG_FILE"
}

# ------------------------------------------------------------------------------- #

DEFAULT_DATABASE="/var/dbase/tess.db"
DEFAULT_DST_DIR="/var/dbase/reports/IDA"
DEFAULT_LOG_FILE=/var/log/tess_geolist.log

TESS_GEOLIST=/usr/local/bin/tess-geolist
TEE=$(which tee)

dbase="${DEFAULT_DATABASE}"
out_dir="${DEFAULT_DST_DIR}"
log_file="${DEFAULT_LOG_FILE}"

while getopts ":hd:o:m:l:" opt; do
    case ${opt} in
    d)
        dbase="${OPTARG}"
        ;;
    o)
        out_dir="${OPTARG}"
        ;;
    l)
        log_file="${OPTARG}"
        ;;
    h)
        help
        exit 0
        ;;
    :)
        echo "Option -${OPTARG} requires an argument."
        exit 1
        ;;
    ?)
        echo "Invalid option: -${OPTARG}."
        exit 1
        ;;
  esac
done
shift "$((OPTIND-1))"

echo "[INFO] Generating TESS geographical list under ${out_dir}" | ${TEE} -a ${log_file}
${TESS_GEOLIST} --console --log-file ${log_file} -d ${dbase} -o ${out_dir}

