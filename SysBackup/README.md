SysBackup
================

This is a system for backup and restore for two components of Zenko for 
disaster recovery purposes:
* Metadata in MongoDB
* Zookeeper queues

Currently the backup process is automated. The process/script for backup will 
be run as a cronjob inside the container: zenko-backup.

MongoDB metadata backup
-----------------------

The tool used to dump mongoDB documents and collections is `mongodump`. The
backup will be a combination of a fullbackup and multiple incremental backups.
Everything will be automated inside the container zenko-backup using a cronjob.
The cronjob will take a full backup everyday using the below command:

```shell
mongodump --host $MONGODB_HOST --port $MONGODB_PORT --oplog --gzip -o \
    $BACKUP_DATA_PATH/mongodb-backup/`date \+\%Y\%m\%d_\%s`
```

This command will assume that MONGODB_HOST, MONGODB_PORT and BACKUP_DATA_PATH
environment variables are defined in the help chart.

For incremental backup the script `incrementalMongodbBackup.sh` will be used.
This is a simple bash script to analyze the full backups or previous
incremental backups and create backups post the previous oplog entry. It will 
avoid overlapping entries. This script will be run every 5 minutes.

Currently local storage attached to the pod will be used for backups. As the
backups are compressed using the `--gzip` option, the backups will roughly take
8-10% of the actual mongoDB metadata size.

<!-- TODO: Add tool like rclone to upload backups to cloud. -->

MongoDB metadata restore
-----------------------
<!-- TODO: write the process for restoring to a new zenko cluster. -->

The tool used to restore mongoDB documents and collection is `mongorestore`.
The restore process is a combination of a full restore of the full backup made
and a script to restore all the incremental backups. Currently the restore
process is manual. To restore the full backup the following command should be
used:

```shell
mongorestore  --gzip --host <mongoDB host --port 27017 <path-to-fullbackup> --oplogReplay --verbose
```

The helper script `incrementalMongodbRestore.sh` restores all incremental
backups. The script assumes that the full backup has been restored using
`mongorestore` command as stated previously. The script requires 3 variables
with the execution:
* FULL_DUMP_DIRECTORY: directory path to the full backup
* OPLOGS_DIRECTORY: Directory path to incremental backup
* OPLOG_LIMIT: Defines the time till which you want the backup for

The script needs the names of the backup files to be the same as when 
backed up.


<!-- TODO: write the process of zookeeper backups -->
