# s3utils
S3 Connector and Zenko Utilities

Run the Docker container as (replace `<tag>` with the tag of s3utils image to be used)
```
docker run --net=host -e 'ACCESS_KEY=accessKey' -e 'SECRET_KEY=secretKey' -e 'ENDPOINT=http://127.0.0.1:8000' -e 'REPLICATION_GROUP_ID=RG001'
registry.scality.com/s3utils/s3utils:<tag> node scriptName bucket1[,bucket2...]
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

This script verifies that :

1. all sproxyd keys referenced by objects in S3 buckets exist on the RING
2. sproxyd keys are unique across versions of the same object
3. object metadata is not an empty JSON object '{}'

It can help to identify objects affected by the S3C-1959 bug (1), or
one of S3C-2731, S3C-3778 (2), S3C-5987 (3).

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
    and missing metadata are still done

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

* **haveEmptyMetadata**: number of objects found with an empty metadata
    blob i.e. `{}`

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

```
object with empty metadata found
```

This message is reported when an object that has an empty metadata
blob as its value is found.

Those objects may have been created during an upgrade scenario where
connectors were upgraded to a S3C version >= 7.4.10.2, but metadata
stateful nodes were not yet upgraded, resulting in the bug described
by S3C-5987.

Such objects can normally be safely deleted because they are generated
only when an "AbortMultipartUpload" operation is executed on the
object, meaning that the object was the target of an MPU in progress
that was aborted, and it was the last state of this object (otherwise
it would have been updated by non-empty metadata).

**Note**: even on versioned buckets, the affected keys are always
  non-versioned.

Logged fields:

* **objectUrl**: URL of the affected object: `s3://bucket/object`

**How to delete all such objects**:

Assuming `verifyBucketSproxydKeys.log` contains the output of the
previous run of the verify script, this shell command can be used to
delete the objects with empty metadata:

```
JQ_SCRIPT='
select(.message == "object with empty metadata found") |
.objectUrl |
sub("s3://";"http://localhost:9000/default/bucket/")'
jq -r "${JQ_SCRIPT}" verifyBucketSproxydKeys.log | while read url; do
    [ "$(curl -s "${url}")" = '{}' ] && {
        echo "deleting ${url}";
        curl -XDELETE "${url}" || echo "failed to delete ${url}";
    } || true
done
```


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

* **EXCLUDE_FROM_CSEQS**: mapping of raft sessions to filter on, where
  keys are raft session IDs and values are the cseq value for that
  raft session. Filtering will be based on all oplog records more
  recent than the given "cseq" for the raft session. Input diff
  entries not belonging to one of the declared raft sessions are
  discarded from the output. The value must be in the following JSON
  format:

  * `{"rsId":cseq[,"rsId":cseq...]}`

  * Example: `{"1":1234,"4":4567,"6":6789}`

  * This configuration would cause diff entries of which bucket/key
    appear in one of the following to be discarded from the output:

    * oplog of raft session 1 after cseq=1234

    * or oplog of raft session 4 after cseq=4567

    * or oplog of raft session 6 after cseq=6789

    * or any other raft session's oplog at any cseq

## Scan Procedure with Stable Leader

**Note**: you may use this procedure when leaders are stable,
  i.e. leader changes are rare in normal circumstances. When leaders
  are unstable, use the [unstable
  leader](#scan-procedure-with-unstable-leader) procedure instead.

In order to scan a set of raft sessions to detect discrepancies
between raft members, you may use the following procedure involving
the two scan tools
[verifyBucketSproxydKeys](#verify-existence-of-sproxyd-keys) and
[followerDiff](#compare-followers-databases-against-leader-view).

Make sure that no leader change occurs from the beginning to the end
of running this procedure.

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

### Step Prep-1: Generate the digests database on one metadata server

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
registry.scality.com/s3utils/s3utils:1.13.23 \
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

### Step Scan-2: Gather current cseq of each raft session to scan

This step gathers the current cseq value of the follower repds for all
raft sessions to be scanned. The result will be passed on to the
followerDiff command in order to remove false positives due to live
changes while the script is running.

```
for RS in $(cat /tmp/rs-to-scan); do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_ADMIN_PORT=$(jq .adminPort ${REPD_CONF})
            echo -n '{"'${RS}'":'
            curl -s "http://localhost:${REPD_ADMIN_PORT}/_/raft/state"
            echo '}'
        fi
    done
done \
| jq -s 'reduce .[] as $item ({}; . + ($item | map_values(.committed)))' \
| tee /tmp/rs-cseqs.json
```

This command should show a result resembling this, mapping raft
session numbers to latest cseq values, and storing it in
`/tmp/rs-cseqs.json`:

```
{
  "3": 17074,
  "4": 11121,
  "5": 15666,
  "7": 169677
}
```

### Step Scan-3: Stop all follower repd processes

In order to be able to scan the databases, follower repd processes
must be stopped with the following command:

```
for RS in $(cat /tmp/rs-to-scan); do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_CONF_FILE=$(basename ${REPD_CONF})
            REPD_NAME=${REPD_CONF_FILE%%.json}
            docker exec -u root scality-metadata-bucket-repd \
            supervisorctl -c /conf/supervisor/supervisord-repd.conf stop repd:${REPD_NAME}
        fi
    done
done
```

In case errors occur, restart the command.

### Step Scan-4: Scan local databases

Run the "CompareRaftMembers/followerDiff" tool to generate a
line-separated JSON output file showing all differences found between
the leader and the local metadata databases, for raft sessions where
the repd process is a follower.

Note that the container must mount all metadata databases mountpoints
in order to have access to all databases.

Example command:
```
DATABASE_VOLUME_MOUNTS=$(docker inspect scality-metadata-bucket-repd \
| jq -r '.[0].Mounts | map(select(.Source | contains("scality-metadata-databases-bucket")) | "-v \(.Source):\(.Destination)") | .[]')
DATABASE_MASTER_MOUNTPOINT=$(docker inspect scality-metadata-bucket-repd \
| jq -r '.[0].Mounts | map(select(.Source | contains("scality-metadata-databases-bucket") and (contains("/ssd01/") or contains("/ssd1/"))) | .Destination) | .[]')

mkdir -p scan-results
docker run --net=host --rm \
  -e 'BUCKETD_HOSTPORT=localhost:9000' \
  ${DATABASE_VOLUME_MOUNTS} \
  -e "DATABASES_GLOB=$(cat /tmp/rs-to-scan | tr -d '\n' | xargs -d' ' -IRS echo ${DATABASE_MASTER_MOUNTPOINT}/RS/0/*)" \
  -v "${PWD}/followerDiff-digests:/digests" \
  -e "LISTING_DIGESTS_INPUT_DIR=/digests" \
  -v "${PWD}/scan-results:/scan-results" \
  -e "DIFF_OUTPUT_FILE=/scan-results/scan-results.json" \
  -e "EXCLUDE_FROM_CSEQS=$(cat /tmp/rs-cseqs.json)" \
  registry.scality.com/s3utils/s3utils:1.13.23 \
  bash -c 'DATABASES=$(echo $DATABASES_GLOB) node CompareRaftMembers/followerDiff' \
| tee -a followerDiff.log

```


**Note**: the tool will refuse to override an existing diff output
  file. If you need to re-run the command, first delete the output
  file or give another path as DIFF_OUTPUT_FILE.

### Step Scan-5: Restart all stopped repd processes

```
for RS in $(cat /tmp/rs-to-scan); do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_CONF_FILE=$(basename ${REPD_CONF})
            REPD_NAME=${REPD_CONF_FILE%%.json}
            docker exec -u root scality-metadata-bucket-repd \
            supervisorctl -c /conf/supervisor/supervisord-repd.conf start repd:${REPD_NAME}
        fi
    done
done
```

In case errors occur, restart the command.

**If there are more active stateful hosts to scan, continue with
[Step Scan-1](#step-scan-1-ssh-to-next-stateful-host-to-scan) on the next
active stateful host.**

### Step Results-1: Gather results

Finally, once all scans have been done on all active stateful hosts,
gather all diff results for later analysis or [repair](#repair-procedure).

For example:

```
for HOST in storage-1 storage-2 storage-3 storage-4 storage-5; do
    scp -r ${USER}@${HOST}:scan-results ./scan-results.${HOST}
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


# Compare two followers database sets

The **CompareRaftMembers/compareFollowerDbs** tool compares two sets
of Metadata leveldb databases, belonging to two different Metadata
nodes, and outputs the differences to the file path given as the
DIFF_OUTPUT_FILE environment variable.

In this file, it outputs each key that differs as line-separated JSON
entries, where each entry can be one of:

- `[{ key, value }, null]`: this key is present on the follower #1 but
  not on follower #2

- `[null, { key, value }]`: this key is not present on follower #1
  but is present on follower #2

- `[{ key, value: "{value1}" }, { key, value: "{value2}" }]`: this key
  has a different value between follower #1 and follower #2: "value1"
  is the value seen on follower #1 and "value2" the value seen on
  follower #2.

## Usage

```
node compareFollowerDbs.js
```

## Mandatory environment variables

* **DATABASES1**: space-separated list of databases of follower #1 to
  compare against follower #2

* **DATABASES2**: space-separated list of databases of follower #2 to
  compare against follower #1

* **DIFF_OUTPUT_FILE**: file path where diff output will be stored

## Optional environment variables

* **PARALLEL_SCANS**: number of databases to scan in parallel (default 4)

* **EXCLUDE_FROM_CSEQS**: mapping of raft sessions to filter on, where
  keys are raft session IDs and values are the cseq value for that
  raft session. Filtering will be based on all oplog records more
  recent than the given "cseq" for the raft session. Input diff
  entries not belonging to one of the declared raft sessions are
  discarded from the output. The value must be in the following JSON
  format:

  * `{"rsId":cseq[,"rsId":cseq...]}`

  * Example: `{"1":1234,"4":4567,"6":6789}`

  * This configuration would cause diff entries of which bucket/key
    appear in one of the following to be discarded from the output:

    * oplog of raft session 1 after cseq=1234

    * or oplog of raft session 4 after cseq=4567

    * or oplog of raft session 6 after cseq=6789

    * or any other raft session's oplog at any cseq

* **BUCKETD_HOSTPORT**: ip:port of bucketd endpoint, needed when
  EXCLUDE_FROM_CSEQS is set (in order to read the Raft oplog)

## Scan Procedure with Unstable Leader

**Note**: you may use this procedure when leaders are unstable,
i.e. leader changes are frequent or susceptible to happen during the
duration of the scan procedure. When leaders are stable, it can be
more practical and efficient to use the [stable
leader](#scan-procedure-with-stable-leader) procedure instead.

In order to scan a set of raft sessions to detect discrepancies
between raft members, you may use the following procedure involving
the scan tool [compareFollowerDbs](#compare-two-followers-database-sets).

The general idea is to compare sets of databases belonging to two
different followers at a time.

**Note**: Each repd node (including the current leader) has to be
stopped at some point to copy or read its databases set, then
restarted. Stopping the leader can have a small temporary impact on
the traffic.


### Define global configuration values

- Choose one node on the system that will be the source from which we
  will copy the set of leveldb databases to a filesystem location that
  can be shared with other nodes later on. It can be any one of the
  Metadata active nodes.

  We will call this node **SourceNode** in the rest of this procedure,
  and its hostname stored in the environment variable ``SOURCE_NODE``
  in the provided commands.

- Choose a list of Raft Sessions to scan

  Decide on which raft sessions will be scanned during this run of the
  procedure. It may be all data Raft Sessions (usually 8 with IDs 1
  through 8), or a subset.

  We will store this set as a space-separated list of raft session
  IDs, for example `1 2 3 4 5 6 7 8`, in the environment variable
  `RAFT_SESSIONS_TO_SCAN` in the provided commands.

- Choose a unique shared filesystem location that:

  - has enough storage space to host Metadata databases of the raft
    sessions being scanned (from a single node)

  - will be network-accessible from each Metadata node (i.e. contents
    can be `rsync` from/to Metadata hosts)

  We will save this remote rsync location as
  `REMOTE_DATABASES_HOST_PATH` environment variable in the provided
  commands, for example `root@node-with-storage:/path/to/source-node-dbs`

- Choose a filesystem location on each stateful node that has enough
  storage space to host Metadata databases of the raft sessions being
  scanned (from a single node) to receive a copy of the remote
  databases from **SourceNode**. It can be for example a location on
  the RING spinner disks. We will save this path to the environment
  variable `LOCAL_DATABASES_PATH`, for example
  `/scality/disk1/source-node-dbs`.

- `env_host_data` Ansible variable value will be provided as
  `ENV_HOST_DATA` environment variable in the provided commands.

The following steps will be run as follows:

- **Step Pre-check** is to be run on the supervisor

- **Steps Prep-1 to Prep-5** are to be run on **SourceNode** only

- **Step Scan-1 to Scan-5** are to be run repeatedly for each active
  stateful host other than **SourceNode**, so four times on most
  systems

- **Step Results-1** is to be run once at the end of the scan, to
  gather results

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

### Step Prep-1: Gather current cseq of each raft session to scan

This step gathers the current cseq value of the **SourceNode** repd
for all raft sessions to be scanned. The result will be passed on to
the `compareFollowerDbs` command in order to remove false positives
due to live changes while the procedure is executed.

```
for RS in ${RAFT_SESSIONS_TO_SCAN}; do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_ADMIN_PORT=$(jq .adminPort ${REPD_CONF})
            echo -n '{"'${RS}'":'
            curl -s "http://localhost:${REPD_ADMIN_PORT}/_/raft/state"
            echo '}'
        fi
    done
done \
| jq -s 'reduce .[] as $item ({}; . + ($item | map_values(.committed)))' \
| tee /tmp/rs-cseqs.json
```

This command should show a result resembling this, mapping raft
session numbers to latest cseq values, and storing it in
`/tmp/rs-cseqs.json`:

```
{
  "1": 17074,
  "2": 11121,
  "3": 15666,
  "4": 169677
}
```

### Step Prep-2: Copy cseqs file to other nodes

Copy the file just created on **SourceNode** to each other node that
will be scanned.

Assuming **SourceNode** is `storage-1` and there are 5 nodes to scan,
this command would copy the file to each of the other 4 nodes:

```
for NODE in storage-2 storage-3 storage-4 storage-5; do
    scp /tmp/rs-cseqs.json root@${NODE}:/tmp/
done
```

### Step Prep-3: Stop repd processes

In order to be able to copy the databases, repd processes for the
corresponding raft sessions to scan must be stopped.

It can be done with the following script:

```
for RS in ${RAFT_SESSIONS_TO_SCAN}; do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_CONF_FILE=$(basename ${REPD_CONF})
            REPD_NAME=${REPD_CONF_FILE%%.json}
            docker exec -u root scality-metadata-bucket-repd \
            supervisorctl -c /conf/supervisor/supervisord-repd.conf stop repd:${REPD_NAME}
        fi
    done
done
```

In case errors occur, restart the command.

### Step Prep-4: Copy databases to the remote location

Copy the set of LevelDB databases hosting Metadata for the raft
sessions in `RAFT_SESSIONS_TO_SCAN`, to the remote filesystem location
defined as `REMOTE_DATABASES_HOST_PATH`.

Assuming **SourceNode** has SSH access to the remote database host, it
can be done with the following script:

```
for RS in ${RAFT_SESSIONS_TO_SCAN}; do
    echo "copying databases for raft session ${RS} to ${REMOTE_DATABASES_HOST_PATH}"
    rsync -a ${ENV_HOST_DATA}/scality-metadata-databases-bucket/${RS} ${REMOTE_DATABASES_HOST_PATH}/
done
```

In case errors occur, restart the command.


### Step Prep-5: Restart repd processes

It can be done with the following script:

```
for RS in ${RAFT_SESSIONS_TO_SCAN}; do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_CONF_FILE=$(basename ${REPD_CONF})
            REPD_NAME=${REPD_CONF_FILE%%.json}
            docker exec -u root scality-metadata-bucket-repd \
            supervisorctl -c /conf/supervisor/supervisord-repd.conf start repd:${REPD_NAME}
        fi
    done
done
```

In case errors occur, restart the command.

**Important**: Run the following Scan steps on each stateful node
  except **SourceNode**.

### Step Scan-1: SSH to next stateful host to scan

SSH to the next stateful host (excluding **SourceNode**) where the
next scan will run to compare against **SourceNode**.

Example:

```
ssh root@storage-2
```

### Step Scan-2: Copy remote databases locally

Copy the set of LevelDB databases previously saved from **SourceNode**
to the remote location locally.

It can be done with the following script:

```
rsync -a ${REMOTE_DATABASES_HOST_PATH}/ ${LOCAL_DATABASES_PATH}/
```

### Step Scan-3: Stop repd processes

In order to be able to scan the databases, repd processes must be
stopped with the following command:

```
for RS in ${RAFT_SESSIONS_TO_SCAN}; do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_CONF_FILE=$(basename ${REPD_CONF})
            REPD_NAME=${REPD_CONF_FILE%%.json}
            docker exec -u root scality-metadata-bucket-repd \
            supervisorctl -c /conf/supervisor/supervisord-repd.conf stop repd:${REPD_NAME}
        fi
    done
done
```

In case errors occur, restart the command.


### Step Scan-4: Scan local databases

Run the "CompareRaftMembers/compareFollowerDbs" tool to generate a
line-separated JSON output file showing all differences found between
this node's databases and the SourceNode's databases, for the chosen
list of raft sessions to scan.

Note that the container must mount all metadata databases mountpoints
in order to have access to all local databases, as well as mount
`LOCAL_DATABASES_PATH`.

Example command:
```
MY_DATABASE_VOLUME_MOUNTS=$(docker inspect scality-metadata-bucket-repd \
| jq -r '.[0].Mounts | map(select(.Source | contains("scality-metadata-databases-bucket")) | "-v \(.Source):\(.Destination)") | .[]')
DATABASE_MASTER_MOUNTPOINT=$(docker inspect scality-metadata-bucket-repd \
| jq -r '.[0].Mounts | map(select(.Source | contains("scality-metadata-databases-bucket") and (contains("/ssd01/") or contains("/ssd1/"))) | .Destination) | .[]')

mkdir -p scan-results
docker run --net=host --rm \
  -e 'BUCKETD_HOSTPORT=localhost:9000' \
  ${MY_DATABASE_VOLUME_MOUNTS} \
  -v ${LOCAL_DATABASES_PATH}:/databases2 \
  -e "DATABASES1_GLOB=$(echo ${RAFT_SESSIONS_TO_SCAN} | tr -d '\n' | xargs -d' ' -IRS echo ${DATABASE_MASTER_MOUNTPOINT}/RS/0/*)" \
  -e "DATABASES2_GLOB=$(echo ${RAFT_SESSIONS_TO_SCAN} | tr -d '\n' | xargs -d' ' -IRS echo /databases2/RS/0/*)" \
  -v "${PWD}/scan-results:/scan-results" \
  -e "DIFF_OUTPUT_FILE=/scan-results/scan-results.json" \
  -e "EXCLUDE_FROM_CSEQS=$(cat /tmp/rs-cseqs.json)" \
  registry.scality.com/s3utils/s3utils:1.13.23 \
  bash -c 'DATABASES1=$(echo $DATABASES1_GLOB) DATABASES2=$(echo $DATABASES2_GLOB) node CompareRaftMembers/compareFollowerDbs' \
| tee -a compareFollowerDbs.log

```

**Note**: the tool will refuse to override an existing diff output
  file. If you need to re-run the command, first delete the output
  file or give another path as DIFF_OUTPUT_FILE.

### Step Scan-5: Restart stopped repd processes

```
for RS in ${RAFT_SESSIONS_TO_SCAN}; do
    for REPD_CONF in ${ENV_HOST_DATA}/scality-metadata-bucket/conf/repd_*.json; do
        REPD_RS=$(jq .raftSessionId ${REPD_CONF})
        if [ $RS = $REPD_RS ]; then
            REPD_CONF_FILE=$(basename ${REPD_CONF})
            REPD_NAME=${REPD_CONF_FILE%%.json}
            docker exec -u root scality-metadata-bucket-repd \
            supervisorctl -c /conf/supervisor/supervisord-repd.conf start repd:${REPD_NAME}
        fi
    done
done
```

In case errors occur, restart the command.

**If there are more active stateful hosts to scan, continue with
[Step Scan-1](#step-scan-1-ssh-to-next-stateful-host-to-scan-1) on the next
active stateful host.**

### Step Results-1: Gather results

Finally, once all scans have been done on all active stateful hosts
except **SourceNode**, gather all diff results for later analysis
or [repair](#repair-procedure).

For example:

```
for HOST in storage-2 storage-3 storage-4 storage-5; do
    scp -r ${USER}@${HOST}:scan-results ./scan-results.${HOST}
done
```

At this point, each results file contains a newline-separated set of
JSON entries describing each difference found on each of the servers
against **SourceNode**, for the databases used by each repd process on
each node. This output is meant to be passed on to the [repair
script](#repair-objects-affected-by-raft-divergence) to actually
repair the divergences. More details on the output format can be found
in the [tool's description
summary](#compare-followers-databases-against-leader-view).

### Caveats

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
  or follower's entry in the current diff entry being processed. This
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

To repair a set of objects with inconsistencies, first make sure that
you ran one of the scan procedures (with [stable
leader](#scan-procedure-with-stable-leader) or [unstable
leader](#scan-procedure-with-unstable-leader)) and got results ready
from `scan-results.[host]` directories.

### Example Repair Command

Assuming the hosts are named storage-1 through storage-5, this command
can be used to launch the repair in dry-run mode first (it's a good
practice to get a sense of what the script will attempt to do before
executing it for real, looking at the logs first):

```
cat scan-results.storage-{1..5}/scan-results.jsonl | \
docker run -i --net=host --rm \
  -e "BUCKETD_HOSTPORT=localhost:9000" \
  -e "SPROXYD_HOSTPORT=localhost:8181" \
  -e "DRY_RUN=1" \
  registry.scality.com/s3utils/s3utils:1.13.23 \
  node CompareRaftMembers/repairObjects | tee -a repairObjects.log
```

If results make sense, it can be executed without the "DRY_RUN"
option to execute the possible automatic repairs, if any:

```
cat scan-results.storage-{1..5}/scan-results.jsonl | \
docker run -i --net=host --rm \
  -e "BUCKETD_HOSTPORT=localhost:9000" \
  -e "SPROXYD_HOSTPORT=localhost:8181" \
  registry.scality.com/s3utils/s3utils:1.13.23 \
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

* **OBJECT_REPAIR_BUCKETD_HOSTPORT**: ip:port of bucketd endpoint

* **OBJECT_REPAIR_SPROXYD_HOSTPORT**: ip:port of sproxyd endpoint

## Example

```
cat /tmp/verifyBucketSproxydKeys.log | docker run -i zenko/s3utils:latest bash -c 'OBJECT_REPAIR_BUCKETD_HOSTPORT=127.0.0.1:9000 OBJECT_REPAIR_SPROXYD_HOSTPORT=127.0.0.1:8181 node repairDuplicateVersions.js' > /tmp/repairDuplicateVersions.log
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


# Repair object metadata with duplicate 'versionId' field

This script repairs object versions that have a duplicate versionId
field in the master key and a missing version key, in particular due
to bug S3C-7861 (CRR with incompatible S3C versions between source and
target).

The repair action consists of fetching the source metadata from the
master key, fixing the JSON versionId field to be the first versionId
present in the binary blob (assumed to be the original one, hence
correct), then copying it to both the master key and a new version
key.

## Usage

```
node repairDuplicateVersionIds.js
```

## Standard Input

The standard input must be fed with the JSON logs output by the
verifyBucketSproxydKeys.js s3utils script. This script only processes
the log entries containing the message 'object master metadata with
duplicate "versionId" field found' and ignores other entries.

## Mandatory environment variables

* **OBJECT_REPAIR_BUCKETD_HOSTPORT**: ip:port of bucketd endpoint

## Example

```
cat /tmp/verifyBucketSproxydKeys.log | docker run -i registry.scality.com/s3utils/s3utils:1.13.24 bash -c 'OBJECT_REPAIR_BUCKETD_HOSTPORT=127.0.0.1:9000 node repairDuplicateVersionIds.js' > /tmp/repairDuplicateVersionIds.log
```

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

* **EXCLUDE_REPLICATING_VERSIONS**: if is set to '1,' 'true,' or 'yes,'
    prevent the deletion of replicating versions

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

# Verify Replication

The verify replication script checks/compares the replication source with the replication destination. The script produces logs about missing objects on destination and destination objects whose size doesnt match the source size. The script verifies only current version of objects. It relies on standard s3 api's for the source and the destination.

## Usage

```shell
node VerifyReplication/index.js
```

## Mandatory environment variables

* **SRC_ENDPOINT**: replication source s3 endpoint

* **SRC_BUCKET**: replication source bucket

* **SRC_ACCESS_KEY**: replication source s3 account access key

* **SRC_SECRET_KEY**: replication source s3 account secret key

* **DST_BUCKET**: replication destination bucket

* **DST_ACCESS_KEY**: replication destination s3 account access key

* **DST_SECRET_KEY**: replication destination s3 account secret key

## Optional environment variables:

* **SRC_BUCKET_PREFIXES**: comma separated list of prefixes, listing will be limited to these prefixes

* **SRC_DELIMITER**: delimiter used with prefixes in source (default '/')

* **LISTING_LIMIT**: number of keys to list per listing request (default 1000)

* **LISTING_WORKERS**: number of concurrent workers to handle listing (default 10)

* **LOG_PROGRESS_INTERVAL**: interval in seconds between progress update log lines (default 10)

* **BUCKET_MATCH**: set to 1 if bucket match is enabled with replication, when not enabled objects will be replicated with keyname pattern `sourceBucket/key`

* **COMPARE_OBJECT_SIZE**: set to 1 to compare source and destination object sizes

* **DST_ENDPOINT**: replication destination s3 endpoint (only for storage type `aws_s3`)

* **DST_STORAGE_TYPE**: destination storage type, currently supports only `aws_s3`

* **DST_REGION**: destination s3 region (only for storage type `aws_s3`, default `us-east-1`)

* **DST_MD_REQUEST_WORKERS**: number of concurrent workers to handle destination metadata requests (default 50)

* **HTTPS_CA_PATH**: path to a CA certificate bundle used to authentify the source S3 endpoint

* **HTTPS_NO_VERIFY**: set to 1 to disable source S3 endpoint certificate check

* **SHOW_CLIENT_LOGS_IF_AVAILABLE**: set to 1 to show storage client logs if available (enable only for debugging as it may produce a lot of log entries), this may be disregarded if the client (depending on storage type) does not support it

* **SKIP_OLDER_THAN**: skip replication verification of objects whose last modified date is older than this, set this as an ISO date or a number of days e.g.,

  * setting to "2022-11-30T00:00:00Z" skips the verification of objects created or modified before Nov 30th 2022
  * setting to "30 days" skips the verification of objects created or modified more than 30 days ago


## Output

The output of the script consists of JSON log lines.

### Info

One log line is output for each missing object,

```json
{
  "name":"s3utils:verifyReplication",
  "time":1670279729106,
  "key":"rna-4/rna33",
  "size":0,"srcLastModified":"2022-12-01T12:27:11.469Z",
  "level":"info",
  "message":"object missing in destination",
  "hostname":"scality.local",
  "pid":24601
}
```

and for each size mismatched object,

```json
{
  "name":"s3utils:verifyReplication",
  "time":1670279723474,
  "key":"rna-2/34",
  "srcSize":1,
  "dstSize":0,
  "srcLastModified":"2022-12-01T11:31:27.528Z",
  "dstLastModified":"2022-12-01T11:33:42.000Z",
  "level":"info",
  "message":"object size does not match in destination",
  "hostname":"scality.local",
  "pid":24601
}
```

and for each failed metadata retrieval (after retries),

```json
{
  "name":"s3utils:verifyReplication",
  "time":1670887351694,
  "error":{
    "message":"socket hang up",
    "code":"TimeoutError",
    "time":"2022-12-12T23:22:31.694Z",
    "region":"us-east-1",
    "hostname":"localhost",
    "retryable":true
    },
  "bucket":"src-bucket",
  "key":"pref1/key404",
  "level":"error",
  "message":"error getting metadata",
  "hostname":"scality.local",
  "pid":11398
}
```

The script also logs a progress update (a summary), every 10 seconds by default,

```json
{
  "name":"s3utils:verifyReplication",
  "time":1670280035601,
  "srcListedCount":492,
  "dstProcessedCount":492,
  "skippedByDate": 123,
  "missingInDstCount":123,
  "sizeMismatchCount":123,
  "replicatedCount":120,
  "dstFailedMdRetrievalsCount":3,
  "dstBucket":"dst-bucket",
  "srcBucket":"src-bucket",
  "prefixFilters":["pref1","pref2"],
  "skipOlderThan": "2022-11-30T00:00:00Z", 
  "level":"info",
  "message":"completed replication verification",
  "hostname":"scality.local",
  "pid":24780
}
```

Description of the properties in the summary/progress update log,

- **srcListedCount** - total number of objects listed from source
- **dstProcessedCount** - total number of objects checked in destination (includes missing, mismatched & failed)
- **skippedByDate** - total number of objects skipped verification because of a date filter `SKIP_OLDER_THAN`
- **missingInDstCount** - total number of objects missing in destination
- **sizeMismatchCount** - total number of objects with mismtached size in destination
- **dstFailedMdRetrievalsCount** - total number of failed object metadata retrievals (after retries) from destination
- **replicatedCount** - total number of successful replications
- **srcBucket** - source bucket
- **dstBucket** - destination bucket

## Example

```shell
docker run \
  --net=host \
  -e 'SRC_ENDPOINT=http://source.s3.com:8000' \
  -e 'SRC_BUCKET=src-bucket' \
  -e 'SRC_ACCESS_KEY=src_access_key' \
  -e 'SRC_SECRET_KEY=src_secret_key' \
  -e 'DST_ENDPOINT=http://destination.s3.com:8000' \
  -e 'DST_ACCESS_KEY=dst_access_key' \
  -e 'DST_SECRET_KEY=dst_secret_key' \
  -e 'DST_BUCKET=dst-bucket' \
  -e 'BUCKET_MATCH=1' \
  -e 'COMPARE_OBJECT_SIZE=1' \
  -e 'SRC_BUCKET_PREFIXES=pref1,pref2' \
  -e 'SKIP_OLDER_THAN="2022-11-30T00:00:00Z"' \
  registry.scality.com/s3utils/s3utils:latest \
  node VerifyReplication/index.js
```

# UtapiV2 Service Report

Generates a service level usage report from UtapiV2 in HTML or JSON format.

## Usage

Commands must be executed on a stateful server that hosts the target UtapiV2 deployment.

```
docker run --net=host --entrypoint python3 -v /scality/ssd01/s3/scality-utapi/conf:/conf:ro -v $PWD:/output zenko/s3utils utapi/service-report.py --output /output
```

## Flags

```
  -c CONFIG, --config CONFIG
                        Specify an alternate config file (default: /conf/config.json)
  -r MAX_RETRIES, --max-retries MAX_RETRIES
                        Max retries before failing a request to an external service (default: 2)
  -p PARALLEL_QUERIES, --parallel-queries PARALLEL_QUERIES
                        Max number of parallel queries to warp 10 (default: 5)
  -j, --json            Output raw reports in json format (default: False)
  -o OUTPUT, --output OUTPUT
                        Write report to this directory (default: Current Directory)
  --debug               Enable debug level logging (default: False)
  --dry-run             Don't do any computation. Only validate and print the configuration. (default: False)
```

# UtapiV2 Service level metrics sidecar

REST API to provide service level reports for UtapiV2

## Usage

```shell
docker run -d \
  --network=host \
  -e SIDECAR_API_KEY=dev_key_change_me \
  -e SIDECAR_SCALE_FACTOR=1.4 \
  -e SIDECAR_WARP10_NODE="md1-cluster1:4802@127.0.0.1:4802" \
  scality/s3utils service-level-sidecar/index.js

curl -X POST -H "Authorization: Bearer dev_key_change_me" localhost:24742/api/report | jq
{
  "account": [
    {
      "arn": "arn:aws:iam::081797933446:/test_1669749866/",
      "name": "test_1669749866",
      "obj_count": 25,
      "bytes_stored": 25,
      "bytes_stored_total": 35
    },
    {
      "arn": "arn:aws:iam::024022147664:/test_1669748782/",
      "name": "test_1669748782",
      "obj_count": 25,
      "bytes_stored": 25,
      "bytes_stored_total": 35
    }
  ],
  "bucket": {
    "arn:aws:iam::081797933446:/test_1669749866/": [
      {
        "name": "test3",
        "obj_count": 25,
        "bytes_stored": 25,
        "bytes_stored_total": 35
      }
    ],
    "arn:aws:iam::024022147664:/test_1669748782/": [
      {
        "name": "test",
        "obj_count": 15,
        "bytes_stored": 15,
        "bytes_stored_total": 21
      },
      {
        "name": "test2",
        "obj_count": 10,
        "bytes_stored": 10,
        "bytes_stored_total": 14
      }
    ]
  },
  "service": {
    "obj_count": 50,
    "bytes_stored": 50,
    "bytes_stored_total": 70
  }
}
```

### Authentication

By default a random API key is generated and logged to stdout at startup.

```json
{"name":"s3utils::utapi-service-sidecar","time":1669677797644,"key":"c489cc0f5bec1c757be7aecd7cdd61dc9db5bfbc4e4f7bad","level":"info","message":"using random API key","hostname":"f2988d47e450","pid":1}
```

A custom static key can be set using the `SIDECAR_API_KEY` environment variable.

```shell
docker run -d \
  --network=host \
  -e SIDECAR_API_KEY=dev_key_change_me \
  scality/s3utils service-level-sidecar/index.js
```

API requests to `/api/report` require this API token to be present in the `Authorization` header.

```shell
curl -X POST -H "Authorization: Bearer dev_key_change_me" localhost:24742/api/report
```

### Scale Factor

Scale factor is provided as a floating point number that represents the percentage to scale reported byte values.
It is used to account for the overhead of erasure coding on the storage backend.
For example, a value of `1.4` will output byte numbers 140% of the calculated value ie 100 bytes will be reported as 140 bytes.
If this produces a non-integer result it is rounded up to the next whole number.

It can be set using the `SIDECAR_SCALE_FACTOR` environment variable.

```shell
docker run -d \
  --network=host \
  -e SIDECAR_SCALE_FACTOR=1.4 \
  scality/s3utils service-level-sidecar/index.js
```

### TLS

TLS can be enabled for the api server as well as internal connections to vault and bucketd.
To enable TLS for the API server specify the path to the TLS certificate and key using
the `SIDECAR_TLS_KEY_PATH` and `SIDECAR_TLS_CERT_PATH` environment variables.
A CA can optionally be set using `SIDECAR_TLS_CA_PATH`.
Certificates should be mounted into the container using a docker volume.

```shell
docker run -d \
  --network=host \
  -v $PWD/certs:/certs \
  -e SIDECAR_TLS_CERT_PATH=/certs/cert.pem \
  -e SIDECAR_TLS_KEY_PATH=/certs/key.pem \
  -e SIDECAR_TLS_CA_PATH=/certs/ca.pem \ // CA path is optional
  scality/s3utils service-level-sidecar/index.js
```

To enable TLS for internal connections to vault or bucketd set `SIDECAR_VAULT_ENABLE_TLS=true` or `SIDECAR_BUCKETD_ENABLE_TLS=true` respectively.

```shell
docker run -d \
  --network=host \
  -e SIDECAR_VAULT_ENABLE_TLS=true \
  -e SIDECAR_BUCKETD_ENABLE_TLS=true \
  scality/s3utils service-level-sidecar/index.js
```

### Backend addresses
#### Vault

Vault is configured using `SIDECAR_VAULT_ENDPOINT`

```shell
docker run -d \
  --network=host \
  -e SIDECAR_VAULT_ENDPOINT=127.0.0.1:8500 \
  scality/s3utils service-level-sidecar/index.js
```

#### Bucketd

Bucketd is configured using `SIDECAR_BUCKETD_BOOTSTRAP`

```shell
docker run -d \
  --network=host \
  -e SIDECAR_BUCKETD_BOOTSTRAP=127.0.0.1:9000 \
  scality/s3utils service-level-sidecar/index.js
```

#### Warp10

The Warp 10 address is configured using `SIDECAR_WARP10_NODE`.
The Warp 10 `nodeId` must be included (normally matches ansible inventory name plus port ie `md1-cluster1:4802`).
The format is `<nodeId>@<host>:<port>`.

```shell
docker run -d \
  --network=host \
  -e SIDECAR_WARP10_NODE="md1-cluster1:4802@127.0.0.1:4802" \
  scality/s3utils service-level-sidecar/index.js
```

### Other Settings

- Log level can be set using `SIDECAR_LOG_LEVEL` (defaults to `info`)
- The concurrency used to query backend APIs can be set using `SIDECAR_CONCURRENCY_LIMIT` (defaults to 10)
- The maximum retries allowed when querying backend APIs can be set using `SIDECAR_RETRY_LIMIT` (defaults to 5)
- The IP address to bind can be set using `SIDECAR_HOST` (default `0.0.0.0`)
- The port to bind can be set using `SIDECAR_PORT` (default `24742`)
