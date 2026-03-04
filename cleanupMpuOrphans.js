/* eslint-disable no-console */
const http = require('http');
const { promisify } = require('util');
const { http: httpArsn } = require('httpagent');
const async = require('async');

const werelogs = require('werelogs');

const DEFAULT_LISTING_LIMIT = 1000;

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
    BUCKETS, RAFT_SESSIONS,
} = process.env;

const VERBOSE = process.env.VERBOSE === '1';
const TRACE = process.env.TRACE === '1';

// eslint-disable-next-line no-nested-ternary
werelogs.configure({ level: TRACE ? 'trace' : VERBOSE ? 'debug' : 'info', dump: 'error' });

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

const httpAgent = new httpArsn.Agent({
    keepAlive: true,
});

let remainingBuckets = (BUCKETS && BUCKETS.split(',')) || [];

function httpRequest(method, url, cb) {
    const urlObj = new URL(url);
    const req = http.request({
        hostname: urlObj.hostname,
        port: urlObj.port,
        path: `${urlObj.pathname}${urlObj.search}`,
        method,
        agent: httpAgent,
    }, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.once('end', () => {
            // eslint-disable-next-line no-param-reassign
            res.body = chunks.join('');
            log.trace('received HTTP response', { method, url, statusCode: res.statusCode });
            return cb(null, res);
        });
        res.once('error', err => cb(new Error(
            'error reading response from HTTP request '
                + `to ${url}: ${err.message}`
        )));
        return undefined;
    });
    req.once('error', err => cb(new Error(
        `error sending HTTP request to ${url}: ${err.message}`
    )));
    log.trace('sending HTTP request', { method, url });
    req.end();
}

let sproxydAlias;

async function getSproxydAlias() {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    const res = await httpRequestAsync('GET', url);
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
        const res = await httpRequestAsync('GET', url);
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
function cleanupOrphanEntry(bucket, shadowBucket, uploadId, orphanEntry, keysToDelete, cb) {
    async.series([
        done => async.eachSeries(keysToDelete, (sproxydKey, keyDone) => {
            const sproxydUrl = `http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${sproxydKey}`;
            httpRequest('DELETE', sproxydUrl, (err, res) => {
                if (err || res.statusCode !== 200) {
                    log.error('failed to delete orphaned sproxyd key', {
                        bucket, uploadId, sproxydKey,
                        error: err ? { message: err.message } : { statusCode: res.statusCode },
                    });
                } else {
                    log.debug('deleted orphaned sproxyd key', { bucket, uploadId, sproxydKey });
                }
                keyDone();
            });
        }, done),
        done => async.eachSeries(orphanEntry.partKeys, (partKey, partDone) => {
            const partUrl = `http://${BUCKETD_HOSTPORT}/default/bucket/${shadowBucket}/`
                + encodeURIComponent(partKey);
            httpRequest('DELETE', partUrl, (err, res) => {
                if (err || (res.statusCode !== 200 && res.statusCode !== 404)) {
                    log.error('failed to delete orphaned part metadata', {
                        bucket, uploadId, partKey,
                        error: err ? { message: err.message } : { statusCode: res.statusCode },
                    });
                } else {
                    log.debug('deleted orphaned part metadata', { bucket, uploadId, partKey });
                }
                partDone();
            });
        }, done),
    ], cb);
}

/**
 * Fetch the complete metadata of an object version from bucketd.
 *
 * Correctly handles non-versioned objects and null versions:
 *
 * - Non-versioned (versionId === 'null'): fetch without a versionId query
 *   param; reject the result if it now has a versionId field (the object was
 *   overwritten by a versioned one since the listing was taken).
 *
 * - Versioned: try the primary ?versionId=<id> URL first. If that fails and
 *   the listing entry carries isNull, try the master-key URL and
 *   ?versionId=null as fallbacks, accepting the result only when its versionId
 *   matches the expected one.
 *
 * Returns the full metadata object, or null when not found/skipped.
 */
async function fetchFullObjectMetadata(bucket, key, versionId, listingParsedMd) {
    const baseUrl = `http://${BUCKETD_HOSTPORT}/default/bucket/${bucket}/`
        + encodeURIComponent(key);

    function parseResponse(url, res) {
        if (res.statusCode === 404) {
            return null;
        }
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        try {
            return JSON.parse(res.body);
        } catch (e) {
            throw new Error(`failed to parse metadata from ${url}: ${e.message}`);
        }
    }

    if (versionId === 'null') {
        // Non-versioned object: fetch without versionId param
        const res = await httpRequestAsync('GET', baseUrl);
        const fullMd = parseResponse(baseUrl, res);
        if (fullMd === null) {
            return null; // 404: object is gone
        }
        if ('versionId' in fullMd) {
            // Object has since been overwritten by a versioned one; skip
            return null;
        }
        return fullMd;
    }

    // Versioned object: try the primary URL first
    const primaryUrl = `${baseUrl}?versionId=${encodeURIComponent(versionId)}`;
    const res = await httpRequestAsync('GET', primaryUrl);
    if (res.statusCode === 200) {
        return parseResponse(primaryUrl, res);
    }
    if (res.statusCode !== 404) {
        throw new Error(`GET ${primaryUrl} returned status ${res.statusCode}`);
    }
    // Primary returned 404; if the listing entry is a null version,
    // try alternative URLs (same fallback logic as the Python script)
    if (!('isNull' in listingParsedMd)) {
        return null;
    }
    for (const altUrl of [baseUrl, `${baseUrl}?versionId=null`]) {
        // eslint-disable-next-line no-await-in-loop
        const altRes = await httpRequestAsync('GET', altUrl);
        const altMd = parseResponse(altUrl, altRes);
        if (altMd !== null && altMd.versionId === versionId) {
            return altMd;
        }
    }
    return null;
}

const httpRequestAsync = promisify(httpRequest);

/**
 * Async generator that iterates over all versions in a bucket using
 * DelimiterVersions listing. Yields { key, versionId, value } for each
 * version, where value is the fully resolved metadata. When the listing
 * result has a pruned location array (large MPUs), the full metadata is
 * fetched individually.
 *
 * Page fetches and individual metadata fetches are each retried up to
 * 100 times on transient errors.
 */
async function* makeVersionsListingIterator(bucket) {
    let keyMarker = '';
    let versionIdMarker = '';
    let isTruncated = true;

    while (isTruncated) {
        const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${bucket}`
            + `?listingType=DelimiterVersions&maxKeys=${LISTING_LIMIT}`
            + `&keyMarker=${encodeURIComponent(keyMarker)}`
            + `&versionIdMarker=${encodeURIComponent(versionIdMarker)}`;

        // eslint-disable-next-line no-await-in-loop
        const { Versions, IsTruncated, NextKeyMarker, NextVersionIdMarker } = await async.retry(
            { times: 100, interval: 5000 },
            async () => {
                const res = await httpRequestAsync('GET', url);
                if (res.statusCode !== 200) {
                    throw new Error(`GET ${url} returned status ${res.statusCode}`);
                }
                return JSON.parse(res.body);
            }
        );

        for (const entry of (Versions || [])) {
            const { key, versionId } = entry;
            let parsedMd;
            try {
                parsedMd = JSON.parse(entry.value);
            } catch (e) {
                log.warn('failed to parse object metadata', {
                    bucket, key,
                    error: { message: e.message },
                });
                continue;
            }
            // Only fetch full metadata when the listing result has a pruned
            // location array: typically the field is absent for large MPUs
            const needMdFetch = (
                'content-length' in parsedMd
                && parsedMd['content-length'] !== 0
                && (parsedMd.location === undefined || parsedMd.location === null)
            );
            if (!needMdFetch) {
                yield { key, versionId, value: parsedMd };
                continue;
            }
            // eslint-disable-next-line no-await-in-loop
            const fullMd = await async.retry(
                { times: 100, interval: 5000 },
                () => fetchFullObjectMetadata(bucket, key, versionId, parsedMd)
            );
            if (fullMd === null) {
                log.warn('full object metadata not found or skipped', {
                    bucket, key, versionId,
                });
                continue;
            }
            yield { key, versionId, value: fullMd };
        }

        isTruncated = IsTruncated;
        if (isTruncated) {
            keyMarker = NextKeyMarker || '';
            versionIdMarker = NextVersionIdMarker || '';
        }
    }
}

const OVERVIEW_KEY_PREFIX = 'overview..|..';

/**
 * Phase 1: build a map of orphaned MPU upload IDs for a given bucket.
 *
 * An orphaned MPU has one or more part keys in the MPU shadow bucket but no
 * corresponding overview key.
 *
 * Returns (via cb) a map of the form:
 *   { [uploadId]: { partKeys: string[], sproxydKeys: Set<string> } }
 */
function processBucket(bucket, cb) {
    const shadowBucket = `mpuShadowBucket${bucket}`;
    const uploadIdsWithOverview = new Set();
    // uploadId => { partKeys: string[], sproxydKeys: Set<string> }
    const orphanMap = {};

    log.info('scanning MPU shadow bucket', { bucket, shadowBucket });

    // --- Step 1: collect upload IDs that have an overview key ---

    let overviewMarker = '';

    function listOverviewKeysIter(iterCb) {
        let url = `http://${BUCKETD_HOSTPORT}/default/bucket/${shadowBucket}`
            + `?prefix=overview%2E%2E%7C%2E%2E&maxKeys=${LISTING_LIMIT}`
            + `&marker=${encodeURIComponent(overviewMarker)}`;
        httpRequest('GET', url, (err, res) => {
            if (err) {
                return iterCb(err);
            }
            if (res.statusCode === 404) {
                // shadow bucket does not exist: no MPUs for this bucket
                return iterCb(null, false);
            }
            if (res.statusCode !== 200) {
                return iterCb(new Error(`GET ${url} returned status ${res.statusCode}`));
            }
            const { Contents, IsTruncated } = JSON.parse(res.body);
            (Contents || []).forEach(item => {
                // overview key format: overview..|..<objectKey>..|..<uploadId>
                const parts = item.key.split('..|..');
                uploadIdsWithOverview.add(parts[parts.length - 1]);
            });
            if (IsTruncated && Contents.length > 0) {
                overviewMarker = Contents[Contents.length - 1].key;
            }
            return iterCb(null, IsTruncated);
        });
    }

    // --- Step 2: list all keys, collect orphaned part keys ---

    let partsMarker = '';

    function listAllPartsIter(iterCb) {
        let url = `http://${BUCKETD_HOSTPORT}/default/bucket/${shadowBucket}`
            + `?maxKeys=${LISTING_LIMIT}`
            + `&marker=${encodeURIComponent(partsMarker)}`;
        httpRequest('GET', url, (err, res) => {
            if (err) {
                return iterCb(err);
            }
            if (res.statusCode === 404) {
                return iterCb(null, false);
            }
            if (res.statusCode !== 200) {
                return iterCb(new Error(`GET ${url} returned status ${res.statusCode}`));
            }
            const { Contents, IsTruncated } = JSON.parse(res.body);
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
            return iterCb(null, IsTruncated);
        });
    }

    async.series([
        // Step 1: collect upload IDs that have an overview key
        done => async.doWhilst(
            iterDone => async.retry(
                { times: 100, interval: 5000 },
                listOverviewKeysIter,
                iterDone
            ),
            async isTruncated => isTruncated,
            done
        ),
        // Step 2: list all parts, populate orphan map
        done => async.doWhilst(
            iterDone => async.retry(
                { times: 100, interval: 5000 },
                listAllPartsIter,
                iterDone
            ),
            async isTruncated => isTruncated,
            done
        ),
        // Step 3: re-check overview keys a second time to eliminate upload IDs
        // that gained an overview key between step 1 and step 2 (race condition)
        done => {
            overviewMarker = '';
            async.doWhilst(
                iterDone => async.retry(
                    { times: 100, interval: 5000 },
                    listOverviewKeysIter,
                    iterDone
                ),
                async isTruncated => isTruncated,
                err => {
                    if (err) {
                        return done(err);
                    }
                    Object.keys(orphanMap).forEach(uploadId => {
                        if (uploadIdsWithOverview.has(uploadId)) {
                            delete orphanMap[uploadId];
                        }
                    });
                    return done();
                }
            );
        },
    ], err => {
        if (err) {
            return cb(err);
        }
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
            return cb();
        }

        // --- Phase 2: scan original bucket versions to find any completed MPU
        //     objects that share sproxyd keys with orphaned parts, then delete
        //     orphaned data (not part of the completed MPU). ---

        return (async () => {
            for await (const { value: resolvedMd } of makeVersionsListingIterator(bucket)) {
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
                await new Promise(resolve => // eslint-disable-line no-await-in-loop
                    cleanupOrphanEntry(
                        bucket, shadowBucket, uploadId, orphanEntry, keysToDelete, resolve
                    )
                );
                delete orphanMap[uploadId];
            }
            // Delete remaining orphans not referenced by any completed object
            for (const uploadId of Object.keys(orphanMap)) {
                const orphanEntry = orphanMap[uploadId];
                const keysToDelete = [...orphanEntry.sproxydKeys];
                await new Promise(resolve => // eslint-disable-line no-await-in-loop
                    cleanupOrphanEntry(
                        bucket, shadowBucket, uploadId, orphanEntry, keysToDelete, resolve
                    )
                );
                delete orphanMap[uploadId];
            }
        })().then(() => {
            log.info('phase 2 complete', { bucket });
            return cb();
        }).catch(cb);
    });
}

async function main() {
    try {
        await getSproxydAlias();
        await raftSessionsToBuckets();
        await new Promise((resolve, reject) =>
            async.eachSeries(remainingBuckets, processBucket,
                err => (err ? reject(err) : resolve()))
        );
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

module.exports = { httpRequest };
