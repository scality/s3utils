#!/bin/bash

if [ -z ${1} ] || [ -z ${2} ]; then
  echo "Usage: $0 <zenko-name> <bucket-name> [-y]"
  exit 1
fi

set -euo pipefail
# args
RELEASE=${1}
BUCKET=${2}
CONFIRM=${3:-false}
# Sensitive collections to skip
SKIP=(PENSIEVE __infostore __metastore __userbucket)
PRIMARY="$(mongo --host ${MONGODB_REPLICASET} --quiet --eval="printjson(rs.isMaster().primary)" | sed 's/^"\(.*\)".*/\1/')"
DB='metadata'

confirm() {
  if [ ${CONFIRM} == '-y' ]; then
    return 0
  else 
    local yes=(y yes Yes YES)
    cat <<-EOF
	Warning: This this action can lead to unrecoverable data orphans and should only be used to reset out of band operations
	Confirm emptying '${BUCKET}' in Zenko '${RELEASE}' and all contained metadata? (y/n)
	EOF
    read answer
    for y in "${yes[@]}"; do
      if [ "${y}" == "${answer}" ]; then
        return 0
      fi
    done
    echo "Operation canceled"
    return 1
  fi
}

skip_bucket(){
  for skip in "${SKIP[@]}"; do
    if [ ${skip} == ${BUCKET} ]; then
        echo "Error: Will not drop collection '${BUCKET}'"
        exit 1
    fi
  done
}

skip_bucket
confirm
mongo ${PRIMARY} --quiet --eval="
  db = connect('${PRIMARY}/${DB}');
  collection = db.getCollection('${BUCKET}')
  if (collection.drop() == true){
    print('Successfully dropped collection ${BUCKET}');
  } else {
    print('Collection ${BUCKET} not found');
    quit(1);
  }"
