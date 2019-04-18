# s3utils
S3 Connector and Zenko Utilities

Run the Docker container as
```
docker run --net=host -e 'ACCESS_KEY=accessKey' -e 'SECRET_KEY=secretKey' -e 'ENDPOINT=http://127.0.0.1:8000' zenko/s3utils node scriptName bucket1[,bucket2...]
```

## Trigger CRR on existing objects

1. Enable versioning and set up replication on the bucket.
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

* **NEW**: No replication status is attached to the object. This is
   the state of objects written without any CRR policy attached to
   the bucket that would have triggered CRR on them.

* **PENDING**: The object replication status is PENDING.

* **COMPLETED**: The object replication status is COMPLETED.

* **FAILED**: The object replication status is FAILED.

* **REPLICA**: The object replication status is REPLICA (objects that
   were put to a target site via CRR have this status).

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

For disaster recovery notably, it may be useful to reprocess REPLICA 
objects to re-sync a backup bucket to the primary site.

#### WORKERS

Specify how many parallel workers should run to update object
metadata. The default is 10 parallel workers.

#### MAX_UPDATES

Specify a maximum number of metadata updates to execute before
stopping the script.

If the script reaches this limit, it outputs a log line containing
the KeyMarker and VersionIdMarker to pass to the next invocation (as
environment variables `KEY_MARKER` and `VERSION_ID_MARKER`) and the
updated bucket list without the already completed buckets. At the next
invocation of the script, those two environment variables must be
set and the updated bucket list passed on the command line to resume
where the script stopped.

The default is unlimited (will process the complete listing of buckets
passed on the command line).

**If the script queues too many objects and Backbeat cannot
 process them quickly enough, Kafka may drop the oldest entries**,
 and the associated objects will stay in the **PENDING** state
 permanently without being replicated. When the number of objects
 is large, it is a good idea to limit the batch size and wait 
 for CRR to complete between invocations.

Example:

`MAX_UPDATES=10000`

This limits the number of updates to 10,000 objects, which requeues
a maximum of 10,000 objects to replicate before the script stops.

#### KEY_MARKER

Set to resume from where an earlier invocation stopped (see
[MAX_UPDATES](#MAX_UPDATES)).

#### VERSION_ID_MARKER

Set to resume from where an earlier invocation stopped (see
[MAX_UPDATES](#MAX_UPDATES)).

### Example use cases

#### CRR existing objects after setting a replication policy for the first time

For this use case, it's not necessary to pass any extra environment
variable, because the default behavior is to process objects without a
replication status attached.

To avoid requeuing too many entries at once, pass this value:

```
export MAX_UPDATES=10000
```

#### Re-queue objects stuck in PENDING state

If Kafka has dropped replication entries, leaving objects stuck in a
PENDING state without being replicated, pass the following extra
environment variables to reprocess them:

```
export TARGET_REPLICATION_STATUS=PENDING
export MAX_UPDATES=10000
```

**Warning**: This may cause replication of objects already in the 
Kafka queue to repeat. To avoid this, purge the "backbeat-replication"
Kafka queue before launching the script.

#### Replicate entries that failed a previous replication

If entries have permanently failed to replicate with a FAILED
replication status and were lost in the failed CRR API, it's still 
possible to re-attempt replication later with the following
extra environment variables:

```
export TARGET_REPLICATION_STATUS=FAILED
export MAX_UPDATES=10000
```

#### Re-sync a primary site completely to a new DR site

To re-sync objects to a new DR site (for example, when the original
DR site is lost) force a new replication of all original objects 
with the following environment variables (after setting the proper
replication configuration to the DR site bucket):

```
export TARGET_REPLICATION_STATUS=NEW,PENDING,COMPLETED,FAILED
export MAX_UPDATES=10000
```

#### Re-sync a DR site back to the primary site

When objects have been lost from the primary site you can re-sync
objects from the DR site to the primary site by re-syncing the
objects that have a REPLICA status with the following environment
variables (after setting the proper replication configuration
from the DR bucket to the primary bucket):

```
export TARGET_REPLICATION_STATUS=REPLICA
export MAX_UPDATES=10000
```

# Empty a versioned bucket

This script deletes all versions of objects in the bucket, including delete markers,
and aborts any ongoing multipart uploads to prepare the bucket for deletion.

**Note: This deletes the data associated with objects and is not recoverable**
```
node cleanupBuckets.js testbucket1,testbucket2
```

# List objects that failed replication

This script prints the list of objects that failed replication to stdout,
following a comma-separated list of buckets. Run the command as

````
node listFailedObjects testbucket1,testbucket2
```
