/* eslint-disable no-console */
const async = require('async');

const werelogs = require('werelogs');

const httpRequest = require('./utils/async/httpRequest');
const listVersions = require('./utils/async/bucketd/listVersions');

const DEFAULT_LISTING_LIMIT = 1000;

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

const LISTING_LIMIT = (
    process.env.LISTING_LIMIT
        && Number.parseInt(process.env.LISTING_LIMIT, 10))
      || DEFAULT_LISTING_LIMIT;

const USAGE = `
cleanupMpuOrphans.js

This script cleans up orphaned multipart upload (MPU) data from S3 buckets.

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
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
`;

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

const log = new werelogs.Logger('s3utils:cleanupMpuOrphans');

let remainingBuckets = (BUCKETS && BUCKETS.split(',')) || [];

let sproxydAlias;

async function getSproxydAlias() {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    const res = await httpRequest('GET', url);
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
        const res = await httpRequest('GET', url);
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        const resp = JSON.parse(res.body);
        remainingBuckets = remainingBuckets.concat(resp.filter(
            bucket => !bucket.startsWith('mpuShadowBucket')
                && bucket !== 'users..bucket'
        ));
    }));
}

/**
 * Delete orphaned sproxyd keys (keysToDelete) and all part metadata entries
 * for the given orphaned upload ID. Failures are logged but do not abort.
 */
async function cleanupOrphanEntry(bucket, shadowBucket, uploadId, orphanEntry, keysToDelete) {
    for (const sproxydKey of keysToDelete) {
        const sproxydUrl = `http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${sproxydKey}`;
        try {
            const res = await httpRequest('DELETE', sproxydUrl); // eslint-disable-line no-await-in-loop
            if (res.statusCode !== 200) {
                log.error('failed to delete orphaned sproxyd key', {
                    bucket, uploadId, sproxydKey, error: { statusCode: res.statusCode },
                });
            } else {
                log.debug('deleted orphaned sproxyd key', { bucket, uploadId, sproxydKey });
            }
        } catch (err) {
            log.error('failed to delete orphaned sproxyd key', {
                bucket, uploadId, sproxydKey, error: { message: err.message },
            });
        }
    }
    for (const partKey of orphanEntry.partKeys) {
        const partUrl = `http://${BUCKETD_HOSTPORT}/default/bucket/${shadowBucket}/`
            + encodeURIComponent(partKey);
        try {
            const res = await httpRequest('DELETE', partUrl); // eslint-disable-line no-await-in-loop
            if (res.statusCode !== 200 && res.statusCode !== 404) {
                log.error('failed to delete orphaned part metadata', {
                    bucket, uploadId, partKey, error: { statusCode: res.statusCode },
                });
            } else {
                log.debug('deleted orphaned part metadata', { bucket, uploadId, partKey });
            }
        } catch (err) {
            log.error('failed to delete orphaned part metadata', {
                bucket, uploadId, partKey, error: { message: err.message },
            });
        }
    }
}

const OVERVIEW_KEY_PREFIX = 'overview..|..';

/**
 * Phase 1: builds a map of orphaned MPU upload IDs for a given bucket.
 *
 * An orphaned MPU has one or more part keys in the MPU shadow bucket but no
 * corresponding overview key.
 *
 * Returns a map of the form:
 *   { [uploadId]: { partKeys: string[], sproxydKeys: Set<string> } }
 */
async function buildOrphanMap(bucket, shadowBucket) {
    const uploadIdsWithOverview = new Set();
    const orphanMap = {};

    async function listOverviewKeys() {
        let marker = '';
        let isTruncated = true;
        while (isTruncated) {
            const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${shadowBucket}`
                + `?prefix=overview%2E%2E%7C%2E%2E&maxKeys=${LISTING_LIMIT}`
                + `&marker=${encodeURIComponent(marker)}`;
            // eslint-disable-next-line no-await-in-loop
            const { Contents, IsTruncated } = await async.retry(
                { times: 100, interval: 5000 },
                async () => {
                    const res = await httpRequest('GET', url);
                    if (res.statusCode === 404) {
                        return { Contents: [], IsTruncated: false };
                    }
                    if (res.statusCode !== 200) {
                        throw new Error(`GET ${url} returned status ${res.statusCode}`);
                    }
                    return JSON.parse(res.body);
                }
            );
            (Contents || []).forEach(item => {
                // overview key format: overview..|..<objectKey>..|..<uploadId>
                const parts = item.key.split('..|..');
                uploadIdsWithOverview.add(parts[parts.length - 1]);
            });
            if (IsTruncated && Contents.length > 0) {
                marker = Contents[Contents.length - 1].key;
            }
            isTruncated = IsTruncated;
        }
    }

    // Step 1: collect upload IDs that have an overview key
    await listOverviewKeys();

    // Step 2: list all parts, populate orphan map
    let partsMarker = '';
    let isTruncated = true;
    while (isTruncated) {
        const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${shadowBucket}`
            + `?maxKeys=${LISTING_LIMIT}`
            + `&marker=${encodeURIComponent(partsMarker)}`;
        // eslint-disable-next-line no-await-in-loop
        const { Contents, IsTruncated } = await async.retry(
            { times: 100, interval: 5000 },
            async () => {
                const res = await httpRequest('GET', url);
                if (res.statusCode === 404) {
                    return { Contents: [], IsTruncated: false };
                }
                if (res.statusCode !== 200) {
                    throw new Error(`GET ${url} returned status ${res.statusCode}`);
                }
                return JSON.parse(res.body);
            }
        );
        (Contents || []).forEach(item => {
            if (item.key.startsWith(OVERVIEW_KEY_PREFIX)) {
                return; // skip overview keys
            }
            // part key format: <uploadId>..|..<5-digit-index>
            const sepPos = item.key.indexOf('..|..');
            if (sepPos === -1) {
                log.warn('unexpected key format in MPU shadow bucket', {
                    shadowBucket, key: item.key,
                });
                return;
            }
            const uploadId = item.key.slice(0, sepPos);
            if (uploadIdsWithOverview.has(uploadId)) {
                return; // has a live overview key: not orphaned
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
                    shadowBucket, key: item.key,
                    error: { message: e.message },
                });
                return;
            }
            const { partLocations } = md;
            if (!partLocations || partLocations.length === 0) {
                log.warn('part key has no partLocations', {
                    shadowBucket, uploadId, key: item.key,
                });
                return;
            }
            partLocations.forEach(loc => orphanMap[uploadId].sproxydKeys.add(loc.key));
        });
        if (IsTruncated && Contents.length > 0) {
            partsMarker = Contents[Contents.length - 1].key;
        }
        isTruncated = IsTruncated;
    }

    // Step 3: re-check overview keys to eliminate upload IDs that gained an
    // overview key between step 1 and step 2 (race condition)
    await listOverviewKeys();
    Object.keys(orphanMap).forEach(uploadId => {
        if (uploadIdsWithOverview.has(uploadId)) {
            delete orphanMap[uploadId];
        }
    });

    return orphanMap;
}

async function processBucket(bucket) {
    const shadowBucket = `mpuShadowBucket${bucket}`;

    log.info('scanning MPU shadow bucket', { bucket, shadowBucket });

    const orphanMap = await buildOrphanMap(bucket, shadowBucket);
    const orphanCount = Object.keys(orphanMap).length;
    log.info('phase 1 complete', { bucket, orphanedUploadIds: orphanCount });
    Object.entries(orphanMap).forEach(([uploadId, info]) => {
        log.info('orphaned MPU found', {
            bucket, uploadId,
            partCount: info.partKeys.length,
            sproxydKeyCount: info.sproxydKeys.size,
        });
    });
    if (orphanCount === 0) {
        return;
    }

    // --- Phase 2: scan original bucket versions to find any completed MPU
    //     objects that share sproxyd keys with orphaned parts, then delete
    //     orphaned data (not part of the completed MPU). ---

    for await (const { value: resolvedMd } of listVersions(BUCKETD_HOSTPORT, bucket, { pageSize: LISTING_LIMIT, retry: { times: 100, interval: 5000 } })) {
        if (!resolvedMd.uploadId || !orphanMap[resolvedMd.uploadId]) {
            continue;
        }
        const uploadId = resolvedMd.uploadId;
        const locationKeys = new Set(
            (resolvedMd.location || []).map(loc => loc.key)
        );
        const orphanEntry = orphanMap[uploadId];
        // Only delete sproxyd keys not referenced by the completed object
        const keysToDelete = [...orphanEntry.sproxydKeys]
            .filter(k => !locationKeys.has(k));
        await cleanupOrphanEntry( // eslint-disable-line no-await-in-loop
            bucket, shadowBucket, uploadId, orphanEntry, keysToDelete
        );
        delete orphanMap[uploadId];
    }
    // Delete remaining orphans not referenced by any completed object
    for (const uploadId of Object.keys(orphanMap)) {
        const orphanEntry = orphanMap[uploadId];
        const keysToDelete = [...orphanEntry.sproxydKeys];
        await cleanupOrphanEntry( // eslint-disable-line no-await-in-loop
            bucket, shadowBucket, uploadId, orphanEntry, keysToDelete
        );
        delete orphanMap[uploadId];
    }
    log.info('phase 2 complete', { bucket });
}

async function main() {
    try {
        await getSproxydAlias();
        await raftSessionsToBuckets();
        await async.eachSeries(remainingBuckets, processBucket);
        log.info('completed MPU orphan cleanup');
        process.exit(0);
    } catch (err) {
        log.error('an error occurred during cleanup', {
            error: { message: err.message },
        });
        process.exit(1);
    }
}

main();

function stop() {
    log.info('stopping execution');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGTERM', stop);
process.on('SIGQUIT', stop);

module.exports = {};
