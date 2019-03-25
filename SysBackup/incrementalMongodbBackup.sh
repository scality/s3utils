#!/bin/bash

function initStaticParams
{
   MONGODB_SERVER=$MONGODB_HOST
   MONOGDB_PORT=$MONGODB_PORT
   MONGODB_USER=NA
   MONGODB_PWD=NA
   OUTPUT_DIRECTORY=$BACKUP_DATA_PATH/mongodb-backup
   LOG_PATH=$OUTPUT_DIRECTORY/logs
   LOG_FILE="$LOG_PATH/backup.log"

   LOG_MESSAGE_ERROR=1
   LOG_MESSAGE_WARN=2
   LOG_MESSAGE_INFO=3
   LOG_MESSAGE_DEBUG=4
   LOG_LEVEL=$LOG_MESSAGE_DEBUG
   SCRIPT=`readlink -f ${BASH_SOURCE[0]}`
   ABSOLUTE_SCRIPT_PATH=$(cd `dirname "$SCRIPT"` && pwd)
}



function log
{
   MESSAGE_LEVEL=$1
   shift
   MESSAGE="$@"

   if [ $MESSAGE_LEVEL -le $LOG_LEVEL ]; then
      echo "`date +'%Y-%m-%dT%H:%M:%S.%3N'` $MESSAGE" >> $LOG_FILE
   fi
}

initStaticParams

mkdir -p $OUTPUT_DIRECTORY
mkdir -p $LOG_PATH

log $LOG_MESSAGE_INFO "[INFO] starting incremental backup of oplog"


LAST_OPLOG_DUMP=`ls -t ${OUTPUT_DIRECTORY}/*.bson  2> /dev/null | head -1`

if [ "$LAST_OPLOG_DUMP" != "" ]; then
   log $LOG_MESSAGE_DEBUG "[DEBUG] last incremental oplog backup is $LAST_OPLOG_DUMP"
   LAST_OPLOG_ENTRY=`bsondump ${LAST_OPLOG_DUMP} 2>> $LOG_FILE | grep ts | tail -1`
   if [ "$LAST_OPLOG_ENTRY" == "" ]; then
      log $LOG_MESSAGE_ERROR "[ERROR] evaluating last backuped oplog entry with bsondump failed"
      exit 1
   else
      TIMESTAMP_LAST_OPLOG_ENTRY=`echo $LAST_OPLOG_ENTRY | jq '.ts[].t'`
      INC_NUMBER_LAST_OPLOG_ENTRY=`echo $LAST_OPLOG_ENTRY | jq '.ts[].i'`
      START_TIMESTAMP="Timestamp( ${TIMESTAMP_LAST_OPLOG_ENTRY}, ${INC_NUMBER_LAST_OPLOG_ENTRY} )"
      log $LOG_MESSAGE_DEBUG "[DEBUG] dumping everything newer than $START_TIMESTAMP"
   fi
   log $LOG_MESSAGE_DEBUG "[DEBUG] last backuped oplog entry: $LAST_OPLOG_ENTRY"
else
   log $LOG_MESSAGE_WARN "[WARN] no backuped oplog available. creating initial backup"
fi

if [ "$LAST_OPLOG_ENTRY" != "" ]; then
   mongodump -h $MONGODB_SERVER --authenticationDatabase=admin -d local -c oplog.rs --query "{ \"ts\" : { \"\$gt\" : $START_TIMESTAMP } }" -o - > ${OUTPUT_DIRECTORY}/${TIMESTAMP_LAST_OPLOG_ENTRY}_${INC_NUMBER_LAST_OPLOG_ENTRY}_oplog.bson 2>> $LOG_FILE
   RET_CODE=$?
else
   TIMESTAMP_LAST_OPLOG_ENTRY=0000000000
   INC_NUMBER_LAST_OPLOG_ENTRY=0
   mongodump -h $MONGODB_SERVER --authenticationDatabase=admin -d local -c oplog.rs -o - > ${OUTPUT_DIRECTORY}/${TIMESTAMP_LAST_OPLOG_ENTRY}_${INC_NUMBER_LAST_OPLOG_ENTRY}_oplog.bson 2>> $LOG_FILE
   RET_CODE=$?
fi

if [ $RET_CODE -gt 0 ]; then
   log $LOG_MESSAGE_ERROR "[ERROR] incremental backup of oplog with mongodump failed with return code $RET_CODE"
fi

FILESIZE=`stat --printf="%s" ${OUTPUT_DIRECTORY}/${TIMESTAMP_LAST_OPLOG_ENTRY}_${INC_NUMBER_LAST_OPLOG_ENTRY}_oplog.bson`

if [ $FILESIZE -eq 0 ]; then
   log $LOG_MESSAGE_WARN "[WARN] no documents have been dumped with incremental backup (no changes in mongodb since last backup?). Deleting ${OUTPUT_DIRECTORY}/${TIMESTAMP_LAST_OPLOG_ENTRY}_${INC_NUMBER_LAST_OPLOG_ENTRY}_oplog.bson"
   rm -f ${OUTPUT_DIRECTORY}/${TIMESTAMP_LAST_OPLOG_ENTRY}_${INC_NUMBER_LAST_OPLOG_ENTRY}_oplog.bson
else
   log $LOG_MESSAGE_INFO "[INFO] finished incremental backup of oplog to ${OUTPUT_DIRECTORY}/${TIMESTAMP_LAST_OPLOG_ENTRY}_${INC_NUMBER_LAST_OPLOG_ENTRY}_oplog.bson"
fi
