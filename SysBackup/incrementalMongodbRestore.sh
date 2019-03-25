#!/bin/bash

# OPLOG_LIMIT: only include oplog entries before the provided Timestamp
# The timestamp is a unix timestamp 
# If your desaster happened for example on 2019-03-15 11:40 and you want to restore until 11:39
# your timestamp is 'date -d "2019-03-15 11:39:00" +%s'

FULL_DUMP_DIRECTORY=$1
OPLOGS_DIRECTORY=$2
OPLOG_LIMIT=$3

if [ "$FULL_DUMP_DIRECTORY" == "" ]; then
   echo "usage: restoreOpLogs.sh [NAME_OF_DIRECTORY_OF_FULL_DUMP] [DIRECTORY_TOP_OPLOGS]"
   exit 1
fi

if [ "$OPLOGS_DIRECTORY" == "" ]; then
   echo "usage: restoreOpLogs.sh [NAME_OF_DIRECTORY_OF_FULL_DUMP] [DIRECTORY_TOP_OPLOGS]"
   exit 1
fi

if [ "$OPLOG_LIMIT" == "" ]; then
   echo "usage: restoreOpLogs.sh [NAME_OF_DIRECTORY_OF_FULL_DUMP] [DIRECTORY_TOP_OPLOGS] [OPLOG_LIMIT]"
   exit 1
fi



FULL_DUMP_TIMESTAMP=`echo $FULL_DUMP_DIRECTORY | cut -d "_" -f 2 | cut -d "/" -f 1`
LAST_OPLOG=""
ALREADY_APPLIED_OPLOG=0

mkdir -p /tmp/emptyDirForOpRestore

for OPLOG in `ls $OPLOGS_DIRECTORY/*.bson`; do
   OPLOG_TIMESTAMP=`echo $OPLOG | rev | cut -d "/" -f 1 | rev | cut -d "_" -f 1`
   if [ $OPLOG_TIMESTAMP -gt $FULL_DUMP_TIMESTAMP ]; then
      if [ $ALREADY_APPLIED_OPLOG -eq 0 ]; then
         ALREADY_APPLIED_OPLOG=1
         echo "applying oplog $LAST_OPLOG"
	 mongorestore --authenticationDatabase=admin --oplogFile $LAST_OPLOG --oplogReplay --dir /tmp/emptyDirForOpRestore --oplogLimit=$OPLOG_LIMIT
         echo "applying oplog $OPLOG"
	 mongorestore --authenticationDatabase=admin --oplogFile $OPLOG --oplogReplay --dir /tmp/emptyDirForOpRestore --oplogLimit=$OPLOG_LIMIT
      else
         echo "applying oplog $OPLOG"
	 mongorestore --authenticationDatabase=admin --oplogFile $OPLOG --oplogReplay --dir /tmp/emptyDirForOpRestore --oplogLimit=$OPLOG_LIMIT
      fi
   else
      LAST_OPLOG=$OPLOG 
   fi
done

if [ $ALREADY_APPLIED_OPLOG -eq 0 ]; then
   if [ "$LAST_OPLOG" != "" ]; then
         echo "applying oplog $LAST_OPLOG"
	 mongorestore --authenticationDatabase=admin --oplogFile $LAST_OPLOG --oplogReplay --dir $OPLOGS_DIRECTORY --oplogLimit=$OPLOG_LIMIT
   fi
fi
