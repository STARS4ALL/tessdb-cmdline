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

TEE=$(which tee)
NICE=$(which nice)
IONICE=$(which ionice)

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


if  [[ ! -f $dbase || ! -r $dbase ]]; then
        echo "Database file $dbase does not exists or is not readable." | ${TEE} -a ${log_file}
        echo "Exiting" | ${TEE} -a ${log_file}
        exit 1
fi


export VIRTUAL_ENV=/home/rfg/stars4all
echo "${NICE} -n 19 ${IONICE} -c3 ${VIRTUAL_ENV}/bin/tess-geolist -d ${dbase} -o ${out_dir} -l ${log_file} $@"
${NICE} -n 19 ${IONICE} -c3 ${VIRTUAL_ENV}/bin/tess-geolist -d ${dbase} -o ${out_dir} -l ${log_file} "$@"
