/* eslint-disable no-console */
const async = require('async');

const werelogs = require('werelogs');

const httpRequest = require('./utils/async/httpRequest');
const listVersions = require('./utils/async/bucketd/listVersions');

const DEFAULT_LISTING_PAGE_SIZE = 1000;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;
const RETRY_PARAMS = { times: 100, interval: 5000 };

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
    BUCKETS, RAFT_SESSIONS,
} = process.env;

const VERBOSE = process.env.VERBOSE === '1';
const TRACE = process.env.TRACE === '1';

let logLevel;
if (TRACE) {
    logLevel = 'trace';
} else if (VERBOSE) {
    logLevel = 'debug';
} else {
    logLevel = 'info';
}
werelogs.configure({ level: logLevel, dump: 'error' });

const LISTING_PAGE_SIZE = (
    process.env.LISTING_PAGE_SIZE
        && Number.parseInt(process.env.LISTING_PAGE_SIZE, 10))
      || DEFAULT_LISTING_PAGE_SIZE;

const LOG_PROGRESS_INTERVAL = (
    process.env.LOG_PROGRESS_INTERVAL
        && Number.parseInt(process.env.LOG_PROGRESS_INTERVAL, 10))
      || DEFAULT_LOG_PROGRESS_INTERVAL;

const USAGE = `
cleanupMpuOrphans.js

This script cleans up orphaned multipart upload (MPU) data and
metadata from S3 buckets.

Context:
    There can be S3 metadata keys from internal MPU shadow buckets,
    possibly associated with orphaned RING objects, that are left
    behind in some rare cases of duplicate and concurrent "complete
    MPU" or "abort MPU" requests on the same MPU object. They take up
    storage space and may cause listing slowdowns, until they are
    manually removed.

    Technically, orphaned parts are defined as part metadata which do
    not have a corresponding overview key with the same upload ID
    (overview keys are present for incomplete, but visible, MPUs).

Principle:
    This script takes care of cleaning up those orphaned parts data and
    metadata keys with a two-phase process for each target bucket:

    - Discovery phase: builds a map of orphaned MPU upload IDs present
      in the bucket

    - Cleanup phase: scans the bucket with a versioned listing,
      matches any completed MPU with their orphaned counterpart to
      detect used sproxyd keys, and only deletes the unused ones along
      with the orphaned metadata. Any orphaned upload IDs that were not
      matched to any completed object version are also deleted
      unconditionally.

Usage:
    node cleanupMpuOrphans.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint
    One of:
        BUCKETS: comma-separated list of buckets to scan
    or:
        RAFT_SESSIONS: comma-separated list of raft sessions to scan

Optional environment variables:
    VERBOSE: set to 1 for more verbose output
    TRACE: set to 1 to trace every request to bucketd and sproxyd
    LISTING_PAGE_SIZE: number of keys to list per listing request (default ${DEFAULT_LISTING_PAGE_SIZE})
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default ${DEFAULT_LOG_PROGRESS_INTERVAL})

Logs:
    Logs are output to stdout in JSON format using the standard
    werelogs formatting. The main log messages are:

    starting discovery phase: scanning MPU shadow bucket
        logged at the beginning of the discovery phase for a given bucket
    orphaned MPU found
        logged during discovery phase for each orphaned MPU discovered
    discovery phase complete
        logged when the discovery phase has completed for a given bucket
    starting cleanup phase: scanning bucket
        logged at the beginning of the cleanup phase for a given bucket
    deleted orphaned sproxyd key
        [VERBOSE or TRACE] logged for each orphaned sproxyd key deleted
    not deleting sproxyd key used by completed MPU
        [VERBOSE or TRACE] logged for each orphaned sproxyd key still used, NOT deleted
    deleted orphaned part metadata
        [VERBOSE or TRACE] logged for each orphaned part metadata key deleted
    cleanup phase complete
        logged when the cleanup phase has completed for a given bucket
    completed MPU orphan cleanup
        logged when cleanup is complete for all target buckets/RAFT sessions
    progress update
        logged every LOG_PROGRESS_INTERVAL with progress stats
`;

const log = new werelogs.Logger('s3utils:cleanupMpuOrphans');

const status = {
    phase: null,
    bucket: null,
    orphanedUploadIds: 0,
    versionsScanned: 0,
    orphanPartsDeleted: 0,
    sproxydKeysDeleted: 0,
};

function logProgress(message) {
    const fields = {
        phase: status.phase,
        bucket: status.bucket,
    };
    if (status.phase === 'discovery') {
        fields.orphanedUploadIds = status.orphanedUploadIds;
    }
    if (status.phase === 'cleanup') {
        fields.versionsScanned = status.versionsScanned;
        fields.orphanPartsDeleted = status.orphanPartsDeleted;
        fields.sproxydKeysDeleted = status.sproxydKeysDeleted;
    }
    log.info(message, fields);
}

let progressInterval;

let remainingBuckets = (BUCKETS && BUCKETS.split(',')) || [];

let sproxydAlias;

async function getSproxydAlias() {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    const res = await httpRequest('GET', url, RETRY_PARAMS);
    if (res.statusCode !== 200) {
        throw new Error(`GET ${url} returned status ${res.statusCode}`);
    }
    const resp = JSON.parse(res.body);
    sproxydAlias = resp['ring_driver:0'].alias;
}

async function raftSessionsToBuckets() {
    if (!RAFT_SESSIONS) {
        return;
    }
    const rsList = RAFT_SESSIONS.split(',');
    await Promise.all(rsList.map(async rs => {
        const url = `http://${BUCKETD_HOSTPORT}/_/raft_sessions/${rs}/bucket`;
        const res = await httpRequest('GET', url, RETRY_PARAMS);
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        const resp = JSON.parse(res.body);
        remainingBuckets = remainingBuckets.concat(resp.filter(
            bucket => !bucket.startsWith('mpuShadowBucket')
                && bucket !== 'users..bucket',
        ));
    }));
}

const OVERVIEW_KEY_PREFIX = 'overview..|..';

/**
 * Populate uploadIds with all upload IDs that have an overview key in the
 * given MPU shadow bucket.
 *
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} shadowBucket - name of the MPU shadow bucket to list
 * @param {Set<string>} uploadIds - set to populate with found upload IDs
 * @param {object} [options] - listing options
 * @param {number} [options.pageSize=1000] - number of keys per listing page
 * @param {object} [options.retry] - retry parameters forwarded to httpRequest
 *   (e.g. { times: 100, interval: 5000 })
 * @returns {Promise<void>} resolves when all overview keys have been scanned
 */
async function getUploadIdsWithOverview(bucketdHostport, shadowBucket, uploadIds, { pageSize = 1000, retry } = {}) {
    let marker = '';
    let isTruncated = true;
    while (isTruncated) {
        const url = `http://${bucketdHostport}/default/bucket/${shadowBucket}`
            + `?prefix=${encodeURIComponent(OVERVIEW_KEY_PREFIX)}&maxKeys=${pageSize}`
            + `&marker=${encodeURIComponent(marker)}`;
         
        const res = await httpRequest('GET', url, retry);
        if (res.statusCode === 404) {
            break;
        }
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        const { Contents, IsTruncated } = JSON.parse(res.body);
        for (const item of Contents) {
            // overview key format: overview..|..<objectKey>..|..<uploadId>
            const keyParts = item.key.split('..|..');
            uploadIds.add(keyParts[keyParts.length - 1]);
        }
        if (IsTruncated) {
            marker = Contents[Contents.length - 1].key;
        }
        isTruncated = IsTruncated;
    }
}

/**
 * List all part keys in the shadow bucket and return a map of orphaned entries
 * whose upload ID has no corresponding overview key.
 *
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} shadowBucket - MPU shadow bucket name to scan
 * @param {Set<string>} uploadIdsWithOverview - upload IDs that have an overview
 *   key and should be skipped
 * @param {object} [options] - listing options
 * @param {number} [options.pageSize=1000] - number of keys per listing page
 * @param {object} [options.retry] - retry parameters forwarded to httpRequest
 *   (e.g. { times: 100, interval: 5000 })
 * @returns {Promise<object>} map of the form
 *   { [uploadId]: { partKeys: string[], sproxydKeys: Set<string> } }
 */
async function collectOrphanParts(bucketdHostport, shadowBucket, uploadIdsWithOverview, { pageSize = 1000, retry } = {}) {
    const orphanMap = {};
    let partsMarker = '';
    let isTruncated = true;
    while (isTruncated) {
        const url = `http://${bucketdHostport}/default/bucket/${shadowBucket}`
            + `?maxKeys=${pageSize}`
            + `&marker=${encodeURIComponent(partsMarker)}`;
         
        const res = await httpRequest('GET', url, retry);
        if (res.statusCode === 404) {
            break;
        }
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        const { Contents, IsTruncated } = JSON.parse(res.body);
        for (const item of Contents) {
            if (item.key.startsWith(OVERVIEW_KEY_PREFIX)) {
                continue; // skip overview keys
            }
            // part key format: <uploadId>..|..<5-digit-index>
            const sepPos = item.key.indexOf('..|..');
            if (sepPos === -1) {
                log.warn('unexpected key format in MPU shadow bucket', {
                    shadowBucket, key: item.key,
                });
                continue;
            }
            const uploadId = item.key.slice(0, sepPos);
            if (uploadIdsWithOverview.has(uploadId)) {
                continue; // has a live overview key: not orphaned
            }
            if (!orphanMap[uploadId]) {
                orphanMap[uploadId] = { partKeys: [], sproxydKeys: new Set() };
            }
            orphanMap[uploadId].partKeys.push(item.key);

            let md;
            try {
                md = JSON.parse(item.value);
            } catch (e) {
                log.warn('failed to parse part key metadata', {
                    shadowBucket,
                    key: item.key,
                    error: { message: e.message },
                });
                continue;
            }
            const { partLocations } = md;
            if (!partLocations || partLocations.length === 0) {
                log.warn('part key has no partLocations', {
                    shadowBucket, uploadId, key: item.key,
                });
                continue;
            }
            for (const loc of partLocations) {
                orphanMap[uploadId].sproxydKeys.add(loc.key);
            }
        }
        if (IsTruncated) {
            partsMarker = Contents[Contents.length - 1].key;
        }
        isTruncated = IsTruncated;
    }
    return orphanMap;
}

/**
 * Phase 1: builds a map of orphaned MPU upload IDs for a given bucket.
 *
 * An orphaned MPU has one or more part keys in the MPU shadow bucket but no
 * corresponding overview key.
 *
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} shadowBucket - MPU shadow bucket name to scan
 * @param {object} [options] - listing options
 * @param {number} [options.pageSize=1000] - number of keys per listing page
 * @param {object} [options.retry] - retry parameters forwarded to httpRequest
 *   (e.g. { times: 100, interval: 5000 })
 * @returns {Promise<object>} map of the form
 *   { [uploadId]: { partKeys: string[], sproxydKeys: Set<string> } }
 */
async function buildOrphanMap(bucketdHostport, shadowBucket, { pageSize = 1000, retry } = {}) {
    const uploadIdsWithOverview = new Set();

    // Step 1: collect upload IDs that have an overview key
    await getUploadIdsWithOverview(
        bucketdHostport,
        shadowBucket,
        uploadIdsWithOverview,
        { pageSize, retry },
    );

    // Step 2: list all parts, build orphan map
    const orphanMap = await collectOrphanParts(
        bucketdHostport,
        shadowBucket,
        uploadIdsWithOverview,
        { pageSize, retry },
    );

    // Step 3: re-check overview keys to eliminate upload IDs that gained an
    // overview key between step 1 and step 2 (race condition)
    await getUploadIdsWithOverview(
        bucketdHostport,
        shadowBucket,
        uploadIdsWithOverview,
        { pageSize, retry },
    );
    for (const uploadId of Object.keys(orphanMap)) {
        if (uploadIdsWithOverview.has(uploadId)) {
            delete orphanMap[uploadId];
        }
    }
    return orphanMap;
}

/**
 * Delete orphaned sproxyd keys (keysToDelete) and all part metadata entries
 * for the given orphaned upload ID. Failures are logged but do not abort.
 *
 * @param {object} reqLogger - werelogs request logger with bucket and uploadId
 *   already set as default fields
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} sproxydHostport - host:port of the sproxyd endpoint
 * @param {string} shadowBucket - MPU shadow bucket name
 * @param {{partKeys: string[]}} orphanEntry - orphan entry with part keys to delete
 * @param {Set<string>} keysToDelete - sproxyd keys to delete
 * @param {object} [retry] - retry parameters forwarded to httpRequest
 * @returns {Promise<void>} resolves when all deletions have been attempted
 */
async function cleanupOrphanEntry(reqLogger, bucketdHostport, sproxydHostport, shadowBucket, orphanEntry, keysToDelete, retry) {
    for (const sproxydKey of keysToDelete) {
        const sproxydUrl = `http://${sproxydHostport}/${sproxydAlias}/${sproxydKey}`;
        try {
            const res = await httpRequest('DELETE', sproxydUrl, retry);  
            if (res.statusCode !== 200) {
                reqLogger.error('failed to delete orphaned sproxyd key', {
                    sproxydKey, error: { statusCode: res.statusCode },
                });
            } else {
                reqLogger.debug('deleted orphaned sproxyd key', { sproxydKey });
                status.sproxydKeysDeleted += 1;
            }
        } catch (err) {
            reqLogger.error('failed to delete orphaned sproxyd key', {
                sproxydKey, error: { message: err.message },
            });
        }
    }
    for (const partKey of orphanEntry.partKeys) {
        const partUrl = `http://${bucketdHostport}/default/bucket/${shadowBucket}/${
            encodeURIComponent(partKey)}`;
        try {
            const res = await httpRequest('DELETE', partUrl, retry);  
            if (res.statusCode !== 200 && res.statusCode !== 404) {
                reqLogger.error('failed to delete orphaned part metadata', {
                    partKey, error: { statusCode: res.statusCode },
                });
            } else {
                reqLogger.debug('deleted orphaned part metadata', { partKey });
                status.orphanPartsDeleted += 1;
            }
        } catch (err) {
            reqLogger.error('failed to delete orphaned part metadata', {
                partKey, error: { message: err.message },
            });
        }
    }
}

/**
 * Phase 2: scan all object versions in the original bucket to find completed
 * MPU objects that share sproxyd keys with orphaned parts, delete only the
 * orphaned keys (those not referenced by the completed object), then delete
 * all remaining orphans unconditionally.
 *
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} sproxydHostport - host:port of the sproxyd endpoint
 * @param {string} bucket - original bucket name
 * @param {string} shadowBucket - MPU shadow bucket name
 * @param {object} orphanMap - map of orphaned upload IDs (mutated in place)
 * @param {object} [options] - listing options
 * @param {number} [options.pageSize=1000] - number of keys per listing page
 * @param {object} [options.retry] - retry parameters forwarded to httpRequest
 *   (e.g. { times: 100, interval: 5000 })
 * @returns {Promise<void>} resolves when all orphans have been processed
 */
async function cleanupOrphans(bucketdHostport, sproxydHostport, bucket, shadowBucket, orphanMap, { pageSize = 1000, retry } = {}) {
    for await (const { value: resolvedMd } of listVersions(bucketdHostport, bucket, {
        pageSize,
        retry,
    })) {
        status.versionsScanned += 1;
        // Common case: skip version if not an MPU that has orphaned parts
        if (!resolvedMd.uploadId || !orphanMap[resolvedMd.uploadId]
            || !Array.isArray(resolvedMd.location)) {
            continue;
        }
        const { uploadId } = resolvedMd;

        const reqLogger = log.newRequestLogger();
        reqLogger.addDefaultFields({ bucket, uploadId });

        const locationKeys = new Set(resolvedMd.location.map(loc => loc.key));
        const orphanEntry = orphanMap[uploadId];
        // Only delete sproxyd keys not referenced by the completed object
        const keysToDelete = new Set();
        for (const sproxydKey of orphanEntry.sproxydKeys) {
            if (locationKeys.has(sproxydKey)) {
                reqLogger.debug('not deleting sproxyd key used by completed MPU',
                    { sproxydKey });
            } else {
                keysToDelete.add(sproxydKey);
            }
        }
         
        await cleanupOrphanEntry(reqLogger, bucketdHostport, sproxydHostport, shadowBucket, orphanEntry, keysToDelete, retry);
        // eslint-disable-next-line no-param-reassign
        delete orphanMap[uploadId];
    }

    // Delete remaining orphans not referenced by any completed object
    for (const uploadId of Object.keys(orphanMap)) {
        const reqLogger = log.newRequestLogger();
        reqLogger.addDefaultFields({ bucket, uploadId });

        const orphanEntry = orphanMap[uploadId];
         
        await cleanupOrphanEntry(reqLogger, bucketdHostport, sproxydHostport, shadowBucket, orphanEntry, orphanEntry.sproxydKeys, retry);
        // eslint-disable-next-line no-param-reassign
        delete orphanMap[uploadId];
    }
}

async function processBucket(bucket) {
    const shadowBucket = `mpuShadowBucket${bucket}`;

    log.info('starting discovery phase: scanning MPU shadow bucket', { bucket, shadowBucket });

    status.bucket = bucket;
    status.phase = 'discovery';
    status.orphanedUploadIds = 0;
    status.versionsScanned = 0;
    status.orphanPartsDeleted = 0;
    status.sproxydKeysDeleted = 0;

    const orphanMap = await buildOrphanMap(
        BUCKETD_HOSTPORT,
        shadowBucket,
        { pageSize: LISTING_PAGE_SIZE, retry: RETRY_PARAMS },
    );
    const orphanCount = Object.keys(orphanMap).length;
    status.orphanedUploadIds = orphanCount;
    logProgress('discovery phase complete');
    if (orphanCount === 0) {
        return;
    }

    for (const [uploadId, info] of Object.entries(orphanMap)) {
        log.info('orphaned MPU found', {
            bucket,
            uploadId,
            partCount: info.partKeys.length,
            sproxydKeyCount: info.sproxydKeys.size,
        });
    }

    log.info('starting cleanup phase: scanning bucket', { bucket });

    status.phase = 'cleanup';
    await cleanupOrphans(
        BUCKETD_HOSTPORT,
        SPROXYD_HOSTPORT,
        bucket,
        shadowBucket,
        orphanMap,
        { pageSize: LISTING_PAGE_SIZE, retry: RETRY_PARAMS },
    );
    logProgress('cleanup phase complete');
}

async function main() {
    if (!BUCKETS && !RAFT_SESSIONS) {
        console.error('ERROR: either BUCKETS or RAFT_SESSIONS environment '
                      + 'variable must be defined');
        console.error(USAGE);
        process.exit(1);
    }
    if (BUCKETS && RAFT_SESSIONS) {
        console.error('ERROR: only one of BUCKETS or RAFT_SESSIONS environment '
                      + 'variables can be defined');
        console.error(USAGE);
        process.exit(1);
    }
    if (!BUCKETD_HOSTPORT) {
        console.error('ERROR: BUCKETD_HOSTPORT not defined');
        console.error(USAGE);
        process.exit(1);
    }
    if (!SPROXYD_HOSTPORT) {
        console.error('ERROR: SPROXYD_HOSTPORT not defined');
        console.error(USAGE);
        process.exit(1);
    }

    progressInterval = setInterval(
        () => logProgress('progress update'),
        LOG_PROGRESS_INTERVAL * 1000,
    );
    try {
        await getSproxydAlias();
        await raftSessionsToBuckets();
        await async.eachSeries(remainingBuckets, processBucket);
        clearInterval(progressInterval);
        log.info('completed MPU orphan cleanup');
        process.exit(0);
    } catch (err) {
        clearInterval(progressInterval);
        log.error('an error occurred during cleanup', {
            error: { message: err.message },
        });
        process.exit(1);
    }
}

function stop() {
    clearInterval(progressInterval);
    log.info('stopping execution');
    process.exit(0);
}

if (require.main === module) {
    main();
    process.on('SIGINT', stop);
    process.on('SIGHUP', stop);
    process.on('SIGTERM', stop);
    process.on('SIGQUIT', stop);
}

module.exports = { getSproxydAlias, getUploadIdsWithOverview, collectOrphanParts, buildOrphanMap, cleanupOrphanEntry, cleanupOrphans };
