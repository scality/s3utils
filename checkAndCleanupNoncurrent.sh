#!/bin/bash

set -e

function usage {
    echo $@ >&2
    cat >&2 <<EOF
Usage:
    checkAndCleanupNoncurrent.sh

Example:
    docker run -i --net=host --rm zenko/s3utils:latest /usr/bin/env \\
    S3_ENDPOINT=http://10.97.84.244:8000 \\
    ACCESS_KEY=ABCDEFGHIJKLMNOPQRST \\
    SECRET_KEY=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN \\
    BUCKETD_HOSTPORT=10.102.18.103:9000 \\
    SPROXYD_HOSTPORT=10.109.250.118:8181 \\
    BUCKETS=bucket1,bucket2 \\
    ./checkAndCleanupNoncurrent.sh

Mandatory Environment Variables:
    S3_ENDPOINT: S3 endpoint URL
    ACCESS_KEY: S3 account/user access key
    SECRET_KEY: S3 account/user secret key
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint
    BUCKETS: comma-separated list of buckets to scan, repair and cleanup

Optional Environment Variables:
    WORKERS: maximum concurrency for metadata checks (default 100)
    VERBOSE: set to 1 for more verbose output of verify script
    HTTPS_CA_PATH: path to a CA certificate bundle used to authentify
    the S3 endpoint
    HTTPS_NO_VERIFY: set to 1 to disable S3 endpoint certificate check
EOF
    exit 1
}

if [ -z "${S3_ENDPOINT}" ]; then
    usage 'missing S3_ENDPOINT variable'
fi

if [ -z "${ACCESS_KEY}" ]; then
    usage 'missing ACCESS_KEY variable'
fi

if [ -z "${SECRET_KEY}" ]; then
    usage 'missing SECRET_KEY variable'
fi

if [ -z "${BUCKETD_HOSTPORT}" ]; then
    usage 'missing BUCKETD_HOSTPORT variable'
fi

if [ -z "${SPROXYD_HOSTPORT}" ]; then
    usage 'missing SPROXYD_HOSTPORT variable'
fi

if [ -z "${BUCKETS}" ]; then
    usage 'missing BUCKETS variable'
fi

START_TIME=`date`
START_TS=`date +%s`

echo '=== STEP 1: CHECK + REPAIR OF DUPLICATE SPROXYD KEYS ===' >&2
echo '=== STEP 1: START TIME' `date` '===' >&2

# We can't use "wait" in bash to wait for process substitution >(...),
# but adding "| cat" has the same effect of waiting until the
# subcommand inside >(...) completes.
NO_MISSING_KEY_CHECK=1 node verifyBucketSproxydKeys.js \
    | tee >(node repairDuplicateVersions.js) | cat

echo >&2
echo '=== STEP 2: CLEANUP NONCURRENT VERSIONS AND DELETE MARKERS ===' >&2
echo '=== STEP 2: START TIME' `date` '===' >&2

OLDER_THAN="${START_TIME}" node cleanupNoncurrentVersions.js ${BUCKETS}

END_TS=`date +%s`
ELAPSED=$((END_TS - START_TS))

echo >&2
echo '=== FINISHED ===' >&2
echo "=== TOTAL RUN TIME: ${ELAPSED} seconds ===" >&2
