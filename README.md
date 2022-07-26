# s3utils
S3 Connector and Zenko Utilities

Run the Docker container as
```
docker run --net=host -e 'ACCESS_KEY=accessKey' -e 'SECRET_KEY=secretKey' -e 'ENDPOINT=http://127.0.0.1:8000' -e 'REPLICATION_GROUP_ID=RG001'
zenko/s3utils node scriptName bucket1[,bucket2...]
```

## Trigger CRR on existing objects

1. Enable versioning and set up replication on the bucket.
2. Run script as
```
node crrExistingObjects.js testbucket1,testbucket2
```

### Mandatory environment variables,

* **REPLICATION_GROUP_ID**

### Optional environment variables,

* **DEBUG**: set to 1 to output debug level information.

### Extra environment variables

Additionally, the following extra environment variables can be passed
to the script to modify its behavior:

#### TARGET_REPLICATION_STATUS

Comma-separated list of replication statuses to target for CRR
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
COMPLETED for CRR, do not requeue the others.

`TARGET_REPLICATION_STATUS=NEW,PENDING,COMPLETED,FAILED`

Trigger CRR on all original source objects (not replicas) in a bucket.

`TARGET_REPLICATION_STATUS=REPLICA`

For disaster recovery notably, it may be useful to reprocess REPLICA
objects to re-sync a backup bucket to the primary site.

#### TARGET_PREFIX

Optional prefix of keys to scan

Example:

`TARGET_PREFIX=foo/`

Only scan keys beginning with "foo/"


#### STORAGE_TYPE

Comma-separated list of the destination storage location types. This is used
only if a replication destination is a public cloud.

The recognized statuses are:

* **aws_s3**: The destination storage location type is Amazon S3.

* **azure**: The destination storage location type is Microsoft Azure Blob
   Storage.

* **gcp**: The destination storage location type is Google Cloud Storage.

Examples:

`STORAGE_TYPE=aws_s3`

The destination storage location type is AWS.

`STORAGE_TYPE=aws_s3,azure,gcp`

The destination storage type is a one-to-many configuration replicating to AWS,
Azure, and GCP destination storage locations.

#### WORKERS

Specify how many parallel workers should run to update object
metadata. The default is 10 parallel workers.

Example:

`WORKERS=50`

#### MAX_UPDATES, MAX_SCANNED

Specify a maximum number of metadata updates (MAX_UPDATES) or scanned
entries (MAX_SCANNED) to execute before stopping the script.

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

Examples:

`MAX_UPDATES=10000`

This limits the number of updates to 10,000 objects, which requeues
a maximum of 10,000 objects to replicate before the script stops.

`MAX_SCANNED=10000`

This limits the number of scanned objects to 10,000 objects before
the script stops.

#### KEY_MARKER

Set to resume from where an earlier invocation stopped (see
[MAX_UPDATES, MAX_SCANNED](#MAX_UPDATES)).

Example:

`KEY_MARKER="some/key"`

#### VERSION_ID_MARKER

Set to resume from where an earlier invocation stopped (see
[MAX_UPDATES](#MAX_UPDATES)).

Example:

`VERSION_ID_MARKER="123456789"`


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
Kafka queue to repeat. To avoid this, set the backbeat consumer
offsets of "backbeat-replication" Kafka topic to the latest topic
offsets before launching the script, to skip over the existing
consumer log.

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

```
node listFailedObjects testbucket1,testbucket2
```

# Verify existence of sproxyd keys

This script verifies that all sproxyd keys referenced by objects in S3
buckets exist on the RING. It can help to identify objects affected by
the S3C-1959 bug.

The script can also be used to generate block digests from the listing
results as seen by the leader, for the purpose of finding
discrepancies between raft members, that can be caused by bugs like
S3C-5739.

## Usage

```
node verifyBucketSproxydKeys.js
```

## Mandatory environment variables:

* **BUCKETD_HOSTPORT**: ip:port of bucketd endpoint

* **SPROXYD_HOSTPORT**: ip:port of sproxyd endpoint

* One of:

  * **BUCKETS**: comma-separated list of buckets to scan
  * Example : `BUCKETS=bucket1,bucket2,bucket3`

* or:

  * **RAFT_SESSIONS**: comma-separated list of raft sessions to scan
  * Example :
    ```
    docker run --net=host \
    -e 'BUCKETD_HOSTPORT=127.0.0.1:9000' \
    -e 'SPROXYD_HOSTPORT=127.0.0.1:8181' \
    -e 'RAFT_SESSIONS=1' \
    registry.scality.com/s3utils/s3utils:1.12.5 \
    node verifyBucketSproxydKeys.js
    ```

* or:

  * **KEYS_FROM_STDIN**: reads objects to scan from stdin if this
    environment variable is set, where the input is a stream of JSON
    objects, each of the form:
    `{"bucket":"bucketname","key":"objectkey\u0000objectversion"}` -
    **note**: if `\u0000objectversion` is not present, it checks the master key
  * Example :
    ```
    cat raft_session_1_output.txt | docker run -i --net=host --rm \
    -e 'SPROXYD_HOSTPORT=127.0.0.1:8181' \
    -e 'BUCKETD_HOSTPORT=127.0.0.1:9000' \
    -e 'KEYS_FROM_STDIN=1' \
    registry.scality.com/s3utils/s3utils:1.12.5 \
    node verifyBucketSproxydKeys.js | tee ring_scan_raft_session_1.txt
    ```

## Optional environment variables:

* **WORKERS**: concurrency value for sproxyd requests (default 100)

* **FROM_URL**: URL from which to resume scanning ("s3://bucket[/key]")

* **VERBOSE**: set to 1 for more verbose output (shows one line for every sproxyd key)

* **LOG_PROGRESS_INTERVAL**: interval in seconds between progress update log lines (default 10)

* **LISTING_LIMIT**: number of keys to list per listing request (default 1000)

* **MPU_ONLY**: only scan objects uploaded with multipart upload method

* **NO_MISSING_KEY_CHECK**: do not check for existence of sproxyd
    keys, for a performance benefit - other checks like duplicate keys
    are still done

* **LISTING_DIGESTS_OUTPUT_DIR**: output listing digests into the
    specified directory (in the LevelDB format)

* **LISTING_DIGESTS_BLOCK_SIZE**: number of keys in each listing
    digest block (default 1000)

## Output

The output of the script consists of JSON log lines. The most
important ones are described below.

### Info

```
progress update
```

This log message is reported at regular intervals, every 10 seconds by
default unless LOG_PROGRESS_INTERVAL environment variable is
defined. It shows statistics about the scan in progress.

Logged fields:

* **scanned**: number of objects scanned

* **skipped**: number of objects skipped (in case MPU_ONLY is set)

* **haveMissingKeys**: number of object versions found with at least
    one missing sproxyd key

* **haveDupKeys**: number of object versions found with at least one
    sproxyd key shared with another object version

* **url**: current URL `s3://bucket[/object]` for the current listing
    iteration, can be used to resume a scan from this point passing
    FROM_URL environment variable.

Example:

```
{"name":"s3utils:verifyBucketSproxydKeys","time":1585700528238,"skipped":0,"scanned":115833,
"haveMissingKeys":2,"haveDupKeys":0,"url":"s3://some-bucket/object1234","level":"info",
"message":"progress update","hostname":"e923ec732b42","pid":67}
```

### Issue reports

```
sproxyd check reported missing key
```

This log message is reported when a missing sproxyd key has been found
in an object. The message may appear more than once for the same
object in case multiple sproxyd keys are missing.

Logged fields:

* **objectUrl**: URL of the affected object version:
    `s3://bucket/object[%00UrlEncodedVersionId]`

* **sproxydKey**: sproxyd key that is missing

```
duplicate sproxyd key found
```

This message is reported when two versions (possibly the same) are
sharing an identical, duplicated sproxyd key.

It represents a data loss risk, since data loss would occur when
deleting one of the versions with a duplicate key, causing the other
version to refer to deleted data in its location array.

Logged fields:

* **objectUrl**: URL of the first affected object version with an
    sproxyd key shared with the second affected object:
    `s3://bucket/object[%00UrlEncodedVersionId]`

* **objectUrl2**: URL of the second affected object version with an
    sproxyd key shared with the first affected object:
    `s3://bucket/object[%00UrlEncodedVersionId]`

* **sproxydKey**: sproxyd key that is duplicated between the two
    object versions


# Compare follower's databases against leader view

The **CompareRaftMembers/followerDiff** tool compares Metadata leveldb
databases on the repd follower on which it is run against the leader's
view, and outputs the differences to the file path given as the
DIFF_OUTPUT_FILE environment variable.

In this file, it outputs each key that differs as line-separated JSON
entries, where each entry can be one of:

- `[{ key, value }, null]`: this key is present on this follower but
  not on the leader

- `[null, { key, value }]`: this key is not present on this follower
  but is present on the leader

- `[{ key, value: "{value1}" }, { key, value: "{value2}" }]`: this key
  has a different value between this follower and the leader: "value1"
  is the value seen on the follower and "value2" the value seen on the
  leader.

It is possible and recommended to speed-up the comparison by providing
a pre-computed digests database via the LISTING_DIGESTS_INPUT_DIR
environment variable, so that ranges of keys that match the digests
database do not have to be checked by querying the leader. The
pre-computed digests database can be generated via a run of
"verifyBucketSproxydKeys" script, providing it the
LISTING_DIGESTS_OUTPUT_DIR environment variable.

## Usage

```
node followerDiff.js
```

## Mandatory environment variables

* **BUCKETD_HOSTPORT**: ip:port of bucketd endpoint

* **DATABASES**: space-separated list of databases to scan

* **DIFF_OUTPUT_FILE**: file path where diff output will be stored

## Optional environment variables

* **LISTING_DIGESTS_INPUT_DIR**: read listing digests from the
    specified LevelDB database

* **PARALLEL_SCANS**: number of databases to scan in parallel (default 4)

## Scan Procedure

In order to scan a set of raft sessions to detect discrepancies
between raft members, you may use the following procedure involving
the two scan tools
[verifyBucketSproxydKeys](#verify-existence-of-sproxyd-keys) and
[followerDiff](#compare-followers-databases-against-leader-view).

The general idea is to compare each follower's view in turn against
the current leader's view, without disturbing the leader in order to
avoid downtime.

To speed up the comparisons, a digests database is first generated
from the current leader's view of all raft sessions, which is then
used by individual comparisons on followers to skim quickly over what
hasn't changed without sending requests to the leader.

- **Step Pre-check** is to be run on the supervisor

- **Steps Prep-1 to Prep-3** are to be run once, on a host having
  access to bucketd as well as having direct SSH access to stateful
  hosts (e.g. the supervisor)

- **Step Scan-1 to Scan-4** are to be run repeatedly for each active
  stateful host, so five times on most systems

- **Step Results-1** is to be run once at the end of the scan, to
  gather results

Note: If running on RH8 or Rocky Linux 8, you may adapt the given
docker commands with `crictl` instead.

### Pre-check: followers must be up-to-date

On each raft session, check that all followers are up-to-date with the leader.

You may use the following command on the supervisor to check this:

```
./ansible-playbook -i env/s3config/inventory tooling-playbooks/check-status-metadata.yml
```

Results are gathered in `/tmp/results.txt`: for all raft sessions,
make sure that all `committed` values across active `md[x]-cluster[y]`
members are within less than 100 entries to each other. Ignore values
of `wsb[x]-cluster[y]` members.

### Step Prep-1: Generate the digests database

Start by scanning all raft sessions using `verifyBucketSproxydKeys`,
to generate a digests database in order to have a more efficient scan
of followers later on, that does not require an entire listing of the
leader again.

Provide the environment variables **LISTING_DIGESTS_OUTPUT_DIR** and
enable **NO_MISSING_KEY_CHECK** to speed up the listing.

If the command is restarted, e.g. after an error or manual abortion,
make sure to delete the `${DIGESTS_PATH}` directory before running the
command again to generate a clean and efficient database.

Example:

```
DIGESTS_PATH=~/followerDiff-digests-$(date -I);
docker run \
--net=host \
--rm \
-e 'BUCKETD_HOSTPORT=127.0.0.1:9000' \
-e 'RAFT_SESSIONS=1,2,3,4,5,6,7,8' \
-e 'LISTING_DIGESTS_OUTPUT_DIR=/digests' \
-v "${DIGESTS_PATH}:/digests" \
-e 'NO_MISSING_KEY_CHECK=1' \
registry.scality.com/s3utils/s3utils:1.13.1 \
node verifyBucketSproxydKeys.js \
| tee -a verifyBucketSproxydKeys.log
```

### Step Prep-2: Copy the digests database to each active metadata stateful server

Example:

```
USER=root
for HOST in storage-1 storage-2 storage-3 storage-4 storage-5; do
    scp -r ${DIGESTS_PATH} ${USER}@${HOST}:followerDiff-digests
done
```

### Step Prep-3: Save the list of raft sessions to scan on each stateful server

The following script computes the list of raft sessions to scan for
each stateful server, and outputs the result as a series of ssh
commands to execute to save the list on each active stateful. It
requires the 'jq' command to be installed where it is executed.

It starts with all listed raft sessions for each active stateful, but
removes the raft sessions on each active stateful on which it is the
leader.

You may save the following script to a file before executing it with "sh".

```
#!/bin/sh

### Config

BUCKETD_ENDPOINT=localhost:9000
USER=root
RS_LIST="1 2 3 4 5 6 7 8"


### Script

JQ_SCRIPT_EACH='
.leader.host as $leader
| .connected[]
| select(.host != $leader)
| { host, $rs }'

JQ_SCRIPT_ALL='
group_by(.host)
| .[]
| .[0].host as $host
| "echo \(map(.rs) | join(" ")) > /tmp/rs-to-scan"
| @sh "ssh -n \($user)@\($host) \(.)"'

for RS in $RS_LIST; do
    curl -s http://${BUCKETD_ENDPOINT}/_/raft_sessions/${RS}/info \
    | jq --arg rs ${RS} "${JQ_SCRIPT_EACH}";
done | jq -rs --arg user ${USER} "${JQ_SCRIPT_ALL}"
```

Executing this script gives on the standard output, the list of ssh
commands to run to save the list of raft sessions to scan in a
temporary file on each active stateful member, for example:

```
ssh -n 'root'@'storage-1' 'echo 3 4 5 7 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-2' 'echo 1 2 3 5 6 7 8 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-3' 'echo 1 2 3 4 5 6 8 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-4' 'echo 1 2 3 4 5 6 7 8 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-5' 'echo 1 2 4 6 7 8 > /tmp/rs-to-scan'
```

Check that what you get looks like the above, then execute those
commands on the shell.

Example, assuming you saved the above script as `output-rs-to-scan-commands.sh`:

```
# sh output-rs-to-scan-commands.sh
ssh -n 'root'@'storage-1' 'echo 3 4 5 7 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-2' 'echo 1 2 3 5 6 7 8 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-3' 'echo 1 2 3 4 5 6 8 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-4' 'echo 1 2 3 4 5 6 7 8 > /tmp/rs-to-scan'
ssh -n 'root'@'storage-5' 'echo 1 2 4 6 7 8 > /tmp/rs-to-scan'
# sh output-rs-to-scan-commands.sh | bash -x
```

**The following steps are to be run for each active metadata stateful node.**

### Step Scan-1: SSH to next stateful host to scan

Example:

```
ssh root@storage-1
```

### Step Scan-2: Stop all follower repd processes

In order to be able to scan the databases, follower repd processes
must be stopped with the following command:

```
for RS in $(cat /tmp/rs-to-scan); do
    docker exec -u root scality-metadata-bucket-repd \
    supervisorctl -c /conf/supervisor/supervisord-repd.conf stop repd:repd_${RS}
done
```

In case errors occur, restart the command.

### Step Scan-3: Scan local databases

Run the "CompareRaftMembers/followerDiff" tool to generate a
line-separated JSON output file showing all differences found between
the leader and the local metadata databases, for raft sessions where
the repd process is a follower.

Example command:

```
mkdir -p followerDiff-results && \
docker run --net=host --rm \
-e 'BUCKETD_HOSTPORT=localhost:9000' \
-v "/scality/ssd01/s3/scality-metadata-databases-bucket:/databases" \
-e "DATABASES_GLOB=$(cat /tmp/rs-to-scan | tr -d '\n' | xargs -d' ' -IRS echo /databases/RS/0/*)" \
-v "${PWD}/followerDiff-digests:/digests" \
-e "LISTING_DIGESTS_INPUT_DIR=/digests" \
-v "${PWD}/followerDiff-results:/followerDiff-results" \
-e "DIFF_OUTPUT_FILE=/followerDiff-results/followerDiff-results.jsonl" \
registry.scality.com/s3utils/s3utils:1.13.1 \
bash -c 'DATABASES=$(echo $DATABASES_GLOB) node CompareRaftMembers/followerDiff' \
| tee -a followerDiff.log
```

**Note**: the tool will refuse to override an existing diff output
  file. If you need to re-run the command, first delete the output
  file or give another path as DIFF_OUTPUT_FILE.

### Step Scan-4: Restart all stopped repd processes

```
for RS in $(cat /tmp/rs-to-scan); do
    docker exec -u root scality-metadata-bucket-repd \
    supervisorctl -c /conf/supervisor/supervisord-repd.conf start repd:repd_${RS}
done
```

In case errors occur, restart the command.

**If there are more active stateful hosts to scan, continue with
[Step Scan-1](#step-scan-1-ssh-to-next-stateful-host-to-scan) on the next
active stateful host.**

### Step Results-1: Gather results

Finally, once all scans have been done on all active stateful hosts,
gather all diff results for later analysis.

For example:

```
for HOST in storage-1 storage-2 storage-3 storage-4 storage-5; do
    scp -r ${USER}@${HOST}:followerDiff-results ./followerDiff-results.${HOST}
done
```

At this point, each results file contains a newline-separated set of
JSON entries describing each difference found on each of the servers,
for the databases used by repd processes acting as followers in the
Raft protocol. This output is meant to be passed on to the [repair
script](#repair-objects-affected-by-raft-divergence) to actually
repair the divergences. More details on the output format can be found
in the [tool's description
summary](#compare-followers-databases-against-leader-view).

### Caveats

- The more up-to-date the digests database is, the more efficient the
  subsequent stateful host scans are. This means that it's better not
  to wait too long between the digests database generation and each
  host scan, in order to reduce the number of updates that occurred
  in-between, therefore limiting the number of new listings that have
  to be done on the leader.

- It is possible to generate the digests database in multiple steps if
  need be, however in such case it's best not to re-scan the same
  range of keys multiple times outputting to the same digests
  database, the risk being to render that range of digests much less
  efficient to optimize the comparisons, or even useless. If an update
  of the digests database is wanted, it's best to re-create a new one
  from scratch (e.g. by deleting the old digests directory first).

- The current version of the script does not work with Metadata bucket
  format v1, only with v0. If the need arises, it could be made to
  work with v1 with some more work.

- The current version of the script does not support internal TLS
  feature


# Repair objects affected by raft divergence

The repair script provided here attempts to automatically repair the
object metadata affected by divergences between leaders and followers,
to fix consistency while making sure the repaired object is
readable. It does not necessarily mean that the repaired version will
be the legitimate version from the application point of view, but the
tool strives to be non-destructive and best-effort.

This script checks each entry from stdin, corresponding to a line from
the output of followerDiff.js, and repairs the object on metadata if
deemed safe to do so with a readable version, and if there's no
ambiguity on what's the best way to repair.

## Usage

```
    node CompareRaftMembers/repairObjects.js
```

## Mandatory environment variables

* **BUCKETD_HOSTPORT**: ip:port of bucketd endpoint

* **SPROXYD_HOSTPORT**: ip:port of sproxyd endpoint

## Optional environment variables

* **VERBOSE**: set to 1 for more verbose output (shows one line for
every sproxyd key checked)

* **DRY_RUN**: set to 1 to log statuses without attempting to repair anything

## Logs

The script outputs logs in JSON. Each object repair analysis or
attempt outputs a single log line, for example:

```
{"name":"s3utils:CompareRaftMembers:repairObjects","time":1658796563031,"bucket":"s3c-5862-nv-0072","key":"test-2022-07-05T21-06-16-960Z/190","repairStatus":"AutoRepair","repairSource":"Leader","repairMaster":false,"level":"info","message":"repaired object metadata successfully","hostname":"jonathan-storage-1","pid":1}
```

In the above log, the repair status is `AutoRepair`, which means the
script attempted to repair automatically the object metadata, and it
succeeded.

The status can be one of:

* **AutoRepair**: the object can be repaired automatically
  (`repairSource` described the source metadata for the repair, either
  `Leader` or `Follower`).

* **ManualRepair**: the object can be repaired but an operator needs
  to decide on which version to repair from. This is normally when
  both versions are readable and the script is unable to take an
  automatic decision. There is currently no support to use
  `objectRepair` to help with those cases, but it can be done via the
  bucketd API.

* **NotRepairable**: the object metadata is either absent or
  unreadable from both leader and follower (the message gives extra
  detail), so the script did not attempt any repair.

* **UpdatedByClient**: this status is given when the repair script
  detects that the current metadata does not match one of the leader
  of follower's entry in the current diff entry being processed. This
  normally means that the application has overwritten the object with
  new contents since the scan ran. It can then safely be ignored, and
  the repair script did not attempt to repair anything here.

Additionally, logs may contain such lines, when a check failed on a
particular sproxyd key that belongs to one instance of the metadata
(whether on leader or follower):

```
{"name":"s3utils:CompareRaftMembers:repairObjects","time":1658796563000,"bucketdUrl":"http://localhost:9000/default/bucket/s3c-5862-nv-0072/test-2022-07-05T21-06-16-960Z%2F190","sproxydKey":"A553B230F05B0B55C2D090233B280559E730D320","level":"error","message":"sproxyd check reported missing key","hostname":"jonathan-storage-1","pid":1}
```

Those are expected when metadata divergence exists, they can give some
complementary information for diagnosis.

At the end of the run, a single line shows the final count per status
(which is also shown during the run every 10 seconds). For example
here, we know that there were 2 automatic repairs performed, and one
requiring a manual check and repair (which entry can be found from the
logs, looking for "ManualRepair" status):

```
{"name":"s3utils:CompareRaftMembers:repairObjects","time":1658796563064,"countByStatus":{"AutoRepair":2,"AutoRepairError":0,"ManualRepair":1,"NotRepairable":0,"UpdatedByClient":0},"level":"info","message":"completed repair","hostname":"jonathan-storage-1","pid":1}
```

## Repair Procedure

To repair a set of objects for inconsistencies, first make sure that
you ran the [scan procedure](#scan-procedure) and got results ready
from `followerDiff-results.[host]` directories.

### Example Repair Command

Assuming the hosts are named storage-1 through storage-5, this command
can be used to launch the repair in dry-run mode first (it's a good
practice to get a sense of what the script will attempt to do before
executing it for real, looking at the logs first):

```
cat followerDiff-results.storage-{1..5}/followerDiff-results.jsonl | \
docker run -i --net=host --rm \
  -e "BUCKETD_HOSTPORT=localhost:9000" \
  -e "SPROXYD_HOSTPORT=localhost:8181" \
  -e "DRY_RUN=1" \
  registry.scality.com/s3utils/s3utils:1.13.3 \
  node CompareRaftMembers/repairObjects | tee -a repairObjects.log
```

If results make sense, it can be executed without the "DRY_RUN"
option to execute the possible automatic repairs, if any:

```
cat followerDiff-results.storage-{1..5}/followerDiff-results.jsonl | \
docker run -i --net=host --rm \
  -e "BUCKETD_HOSTPORT=localhost:9000" \
  -e "SPROXYD_HOSTPORT=localhost:8181" \
  registry.scality.com/s3utils/s3utils:1.13.3 \
  node CompareRaftMembers/repairObjects | tee -a repairObjects.log
```

# Remove delete markers

The removeDeleteMarkers.js script removes delete markers from one or
more versioning-suspended bucket(s).

## Usage

```
    node removeDeleteMarkers.js bucket1[,bucket2...]
```

## Mandatory environment variables

* **ENDPOINT**: S3 endpoint

* **ACCESS_KEY**: S3 account access key

* **SECRET_KEY**: S3 account secret key

## Optional environment variables:

* **TARGET_PREFIX**: only process a specific prefix in the bucket(s)

* **WORKERS**: concurrency value for listing / batch delete requests (default 10)

* **LOG_PROGRESS_INTERVAL**: interval in seconds between progress update log lines (default 10)

* **LISTING_LIMIT**: number of keys to list per listing request (default 1000)

* **KEY_MARKER**: resume processing from a specific key

* **VERSION_ID_MARKER**: resume processing from a specific version ID

## Output

The output of the script consists of JSON log lines.

### Info

One log line is output for each delete marker deleted, e.g.:

```
{"name":"s3utils::removeDeleteMarkers","time":1586304708269,"bucket":"some-bucket","objectKey":"some-key","versionId":"3938343133363935343035383633393939393936524730303120203533342e3432363436322e393035353030","level":"info","message":"delete marker deleted","hostname":"c3f84aa473a2","pid":88}
```

The script also logs a progress update, every 10 seconds by default:

```
{"name":"s3utils::removeDeleteMarkers","time":1586304694304,"objectsListed":257000,"deleteMarkersDeleted":0,"deleteMarkersErrors":0,"bucketInProgress":"some-bucket","keyMarker":"some-key-marker","versionIdMarker":"3938343133373030373134393734393939393938524730303120203533342e3430323230362e373139333039","level":"info","message":"progress update","hostname":"c3f84aa473a2","pid":88}
```

# Repair duplicate sproxyd keys

This script repairs object versions that share sproxyd keys with
another version, particularly due to bug S3C-2731.

The repair action consists of copying the data location that is
duplicated to a new sproxyd key (or set of keys for MPU), and updating
the metadata to reflect the new location, resulting in two valid
versions with distinct data, though identical in content.

The script does not remove any version even if the duplicate was due
to an internal retry in the metadata layer, because either version
might be referenced by S3 clients in some cases.

## Usage

```
node repairDuplicateVersions.js
```

## Standard Input

The standard input must be fed with the JSON logs output by the
verifyBucketSproxydKeys.js s3utils script. This script only processes
the log entries containing the message "duplicate sproxyd key found"
and ignores other entries.

## Mandatory environment variables

* **BUCKETD_HOSTPORT**: ip:port of bucketd endpoint

* **SPROXYD_HOSTPORT**: ip:port of sproxyd endpoint

## Example

```
cat /tmp/verifyBucketSproxydKeys.log | docker run -i zenko/s3utils:latest bash -c 'BUCKETD_HOSTPORT=127.0.0.1:9000 SPROXYD_HOSTPORT=127.0.0.1:8181 node repairDuplicateVersions.js' > /tmp/repairDuplicateVersions.log
```

## Caveat

While the script is running, there is a possibility, although slim,
that it updates metadata for an object that is being updated at the
exact same time by a client application, either through "PutObjectAcl"
or "PutObjectTagging" operations which can modify existing
versions. In such case, there is a risk that either:
* the application update will be lost
* or the script will not repair the item properly

If this looks like a potential risk to a customer running the script,
we suggest to disable those operations on the clients during the time
the script is running (or alternatively, disable all writes), to avoid
any such risk.

# Cleanup Noncurrent Versions

This script removes noncurrent versions and current/noncurrent delete
markers, either all such objects or older than a specified
last-modified date.

## Usage

```
node cleanupNoncurrentVersions.js bucket1[,bucket2...]
```

## Mandatory environment variables

* **S3_ENDPOINT**: S3 endpoint URL

* **ACCESS_KEY**: S3 account/user access key

* **SECRET_KEY**: S3 account/user secret key

## Optional environment variables

* **TARGET_PREFIX**: cleanup only inside this key prefix in each bucket

* **MAX_LISTED**: maximum number of keys listed before exiting (default unlimited)

* **MAX_DELETES**: maximum number of keys to delete before exiting (default unlimited)

* **MARKER**: marker from which to resume the cleanup, logged at the end
of a previous invocation of the script, uses the format:

  ```
  MARKER := encodeURI(bucketName)
    "|" encodeURI(key)
    "|" encodeURI(versionId)
  ```

* **OLDER_THAN**: cleanup only objects which last modified date is older
than this, as an ISO date or a number of days, e.g.:

  * setting to "2021-01-09T00:00:00Z" limits the cleanup to objects
  created or modified before Jan 9th 2021

  * setting to "30 days" limits the cleanup to objects created more
  than 30 days ago

* **ONLY_DELETED**: if set to "1" or "true" or "yes", only remove
otherwise eligible noncurrent versions if the object's current version
is a delete marker (also removes the delete marker)

* **DELETED_BEFORE**: cleanup only objects whose current version is a delete
    marker older than this date, e.g. setting to "2021-01-09T00:00:00Z"
    limits the cleanup to objects deleted before Jan 9th 2021.
    Implies `ONLY_DELETED = true`

* **HTTPS_CA_PATH**: path to a CA certificate bundle used to
authentify the S3 endpoint

* **HTTPS_NO_VERIFY**: set to 1 to disable S3 endpoint certificate check

## Example

```
docker run --net=host -ti zenko/s3utils:latest bash -c 'S3_ENDPOINT=https://s3.customer.com ACCESS_KEY=123456 SECRET_KEY=789ABC OLDER_THAN="Jan 14 2021" node cleanupNoncurrentVersions.js target-bucket-1,target-bucket-2' > /tmp/cleanupNoncurrentVersions.log
```

# Count number and total size of current and noncurrent versions in a bucket

The bucketVersionsStats.js script counts the total number of objects
and their cumulative size, and shows them separately for current and
noncurrent versions.

## Usage

```
    node bucketVersionsStats.js
```

## Mandatory environment variables

* **ENDPOINT**: S3 endpoint

* **ACCESS_KEY**: S3 account access key

* **SECRET_KEY**: S3 account secret key

* **BUCKET**: S3 bucket name

## Optional environment variables:

* **TARGET_PREFIX**: only process a specific prefix in the bucket

* **LISTING_LIMIT**: number of keys to list per listing request (default 1000)

* **LOG_PROGRESS_INTERVAL**: interval in seconds between progress update log lines (default 10)

* **KEY_MARKER**: start counting from a specific key

* **VERSION_ID_MARKER**: start counting from a specific version ID

## Output

The output of the script consists of JSON log lines.

The script logs a progress update, every 10 seconds by default, and a
final summary at the end of execution with the total counts for the
bucket.

The total counts may be possibly limited by the **TARGET_PREFIX**,
**KEY_MARKER** and **VERSION_ID_MARKER** optional environment variables.

- **stats** contains the output statistics:

  - **current** refers to current versions of objects

  - **noncurrent** refers to non-current versions of objects

  - **total** is the sum of **current** and **noncurrent**

- in **stats.current** and **stats.noncurrent**:

  - **count** is the number of objects pertaining to the section

  - **size** is the cumulative size in bytes of the objects pertaining
  to the section

### Example Progress Update

Note: the JSON output is prettified here for readability, but the
script outputs this status on a single line.

```
{
  "name": "s3utils::bucketVersionsStats",
  "time": 1638823524419,
  "bucket": "example-bucket",
  "stats": {
    "total": {
      "count": 16000,
      "size": 27205000
    },
    "current": {
      "count": 14996,
      "size": 26165000
    },
    "noncurrent": {
      "count": 1004,
      "size": 1040000
    }
  },
  "keyMarker": "test-2021-12-06T20-09-39-712Z/1732",
  "versionIdMarker": "3938333631313738363132353838393939393939524730303120203131302e353533312e3135373439",
  "level": "info",
  "message": "progress update",
  "hostname": "lab-store-0",
  "pid": 717
}
```

### Example Final Summary

Note: the JSON output is prettified here for readability, but the
script outputs this status on a single line.

```
{
  "name": "s3utils::bucketVersionsStats",
  "time": 1638823531629,
  "bucket": "example-bucket",
  "stats": {
    "total": {
      "count": 54267,
      "size": 65472000
    },
    "current": {
      "count": 53263,
      "size": 64432000
    },
    "noncurrent": {
      "count": 1004,
      "size": 1040000
    }
  },
  "level": "info",
  "message": "final summary",
  "hostname": "lab-store-0",
  "pid": 717
}
```
