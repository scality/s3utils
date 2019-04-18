# s3utils
S3 Connector and Zenko Utilities

Run the docker container as
```
docker run --net=host -e 'ACCESS_KEY=accessKey' -e 'SECRET_KEY=secretKey' -e 'ENDPOINT=http://127.0.0.1:8000' -e 'SITE_NAME=crrSiteName' zenko/s3utils node scriptName bucket1[,bucket2...]
```

## Trigger CRR on existing objects

1. Enable versioning and setup replication on the bucket
2. Run script as
```
node crrExistingObjects.js testbucket1,testbucket2
```

### Extra environment variables

Additionally, the following extra environment variables can be passed
to the script to modify its behavior:

#### TARGET_REPLICATION_STATUS

Comma-separated list of replication status to target for CRR
requeueing. The recognized statuses are:

* **NEW**: no replication status is attached to the object, this is
   the state of objects that have been written without any CRR policy
   attached to the bucket that would have triggered CRR on them

* **PENDING**: the object replication status is PENDING

* **COMPLETED**: the object replication status is COMPLETED

* **FAILED**: the object replication status is FAILED

* **REPLICA**: the object replication status is REPLICA (objects that
   were put to a target site via CRR have this status)

The default script behavior is to affect objects that have no
replication status attached (so equivalent to
`TARGET_REPLICATION_STATUS=NEW`).

Examples:

`TARGET_REPLICATION_STATUS=PENDING,COMPLETED`

Requeue objects that either have a replication status of PENDING or
COMPLETED for CRR, do not requeue the others

`TARGET_REPLICATION_STATUS=NEW,PENDING,COMPLETED,FAILED`

Trigger CRR on all original source objects (not replicas) in a bucket.

`TARGET_REPLICATION_STATUS=REPLICA`

For disaster recovery notably, to re-sync a backup bucket to the
primary site, it may be useful to reprocess REPLICA objects.

#### WORKERS

Specify how many parallel workers should run to update object
metadata. The default is 10 parallel workers.

#### MAX_UPDATES

Specify a maximum number of metadata updates to execute before
stopping the script.

If the script reaches this limit, it will output a log line containing
the KeyMarker and VersionIdMarker to pass to the next invocation, as
environment variables `KEY_MARKER` and `VERSION_ID_MARKER`, and the
updated bucket list without the already completed buckets. At the next
invocation of the script, those two environment variables should be
set and the updated bucket list passed on the command-line to resume
from where the script stopped.

The default is unlimited (will process the complete listing of buckets
passed on the command-line).

**Beware that if too many objects are queued by the script and cannot
 be processed by backbeat quickly enough, it might lead to Kafka
 dropping the oldest entries**, and the associated objects will stay
 in **PENDING** state permanently without being replicated. When the
 number of objects is large, it can be a good idea to limit the batch
 size, and wait for CRR to complete between invocations.

Example:

`MAX_UPDATES=10000`

Limit the number of updates to 10000 objects, which will requeue a
maximum of 10000 objects to replicate before the script stops.

#### KEY_MARKER

Set to resume from where an earlier invocation stopped (see
[MAX_UPDATES](#MAX_UPDATES))

#### VERSION_ID_MARKER

Set to resume from where an earlier invocation stopped (see
[MAX_UPDATES](#MAX_UPDATES))

### Example use-cases

#### CRR existing objects after setting a replication policy for the first time

For this use-case, it's not necessary to pass any extra environment
variable as the default behavior is to process objects without a
replication status attached.

Though to avoid requeuing too many entries at once you could pass:

```
export MAX_UPDATES=10000
```

#### Re-queue objects stuck in PENDING state

In case where replication entries would have been dropped by kafka,
and objects are stuck in PENDING state without being replicated, you
could pass the following extra environment variables to reprocess
them:

```
export TARGET_REPLICATION_STATUS=PENDING
export MAX_UPDATES=10000
```

**Warning**: this might cause replication to happen twice on some
objects that were already in the kafka queue. To avoid this, it can be
advised to purge the Kafka queue "backbeat-replication" before
launching the script.

#### Replicate again entries that have failed a previous replication

If entries have failed permanently to replicate and got a FAILED
replication status, and we lost track of them in the failed CRR API,
it's still possible to re-attempt replication later with the following
extra environment variables:

```
export TARGET_REPLICATION_STATUS=FAILED
export MAX_UPDATES=10000
```

#### Re-sync a primary site completely to a new DR site

In order to re-sync the objects again to a new DR site, for example
when the original DR site was lost, it could be done by forcing a new
replication of all original objects, with the following environment
variables, after having set the proper replication configuration to
the DR site bucket:

```
export TARGET_REPLICATION_STATUS=NEW,PENDING,COMPLETED,FAILED
export MAX_UPDATES=10000
```

#### Re-sync a DR site back to the primary site

In order to re-sync the objects from the DR site back to the primary
site, when objects have been lost from the primary site, it would be
possible by re-syncing the objects that have a REPLICA status with the
following environment variables, after having set the proper
replication configuration from the DR bucket to the primary bucket:

```
export TARGET_REPLICATION_STATUS=REPLICA
export MAX_UPDATES=10000
```

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
