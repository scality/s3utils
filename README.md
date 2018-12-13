# s3utils
S3 Connector and Zenko Utilities

Run the docker container as
```
docker run --net=host -e 'ACCESS_KEY=accessKey' -e 'SECRET_KEY=secretkey' -e 'ENDPOINT=http://127.0.0.1:8000' -e 'SITE_NAME=crrSiteName' zenko/s3utils node scriptName bucket1[,bucket2...]
```

Optionally, the environment variable "WORKERS" may be set to specify
how many parallel workers should run, otherwise a default of 10
workers will be used.

## Trigger CRR on objects that were put before replication was enabled on the bucket

1. Enable versioning and setup replication on the bucket
2. Run script as
```
node crrExistingObjects.js testbucket1,testbucket2
```

## Trigger CRR on *all* objects of a bucket

This mode includes the objects that have already been replicated or
that have a replication status attached.

For disaster recovery notably, to re-sync a backup bucket to the
primary site, it may be useful to reprocess all objects regardless of
the existence of a current replication status (e.g. "REPLICA").

Follow the above steps for using "crrExistingObjects" script, and
specify an extra environment variable `-e "PROCESS_ALL=true"` to force
the script to reset the replication status of all objects in the
bucket to "pending", which will force a replication for all objects.

# Empty a versioned bucket

This script deletes all versions of objects in the bucket including delete markers,
and aborts any ongoing multipart uploads to prepare the bucket for deletion.

**Note: This will delete data associated with the objects and it's not recoverable**
```
node cleanupBuckets.js testbucket1,testbucket2
```

# List objects that failed replication

This script can print the list of objects that failed replication to stdout by
taking a comma-separated list of buckets. Run the command as

````
node listFailedObjects testbucket1,testbucket2
```
