/* eslint-disable no-console */
const http = require('http');
const { http: httpArsn } = require('httpagent');
const async = require('async');

const { Logger } = require('werelogs');

const DEFAULT_LISTING_LIMIT = 1000;

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
    BUCKETS, RAFT_SESSIONS,
} = process.env;

const VERBOSE = process.env.VERBOSE === '1';

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

const log = new Logger('s3utils:cleanupMpuOrphans');

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
    req.end();
}

function raftSessionsToBuckets(cb) {
    if (!RAFT_SESSIONS) {
        return cb();
    }
    const rsList = RAFT_SESSIONS.split(',');
    return async.each(rsList, (rs, done) => {
        const url = `http://${BUCKETD_HOSTPORT}/_/raft_sessions/${rs}/bucket`;
        httpRequest('GET', url, (err, res) => {
            if (err) {
                return cb(err);
            }
            if (res.statusCode !== 200) {
                return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
            }
            const resp = JSON.parse(res.body);
            remainingBuckets = remainingBuckets.concat(resp.filter(
                bucket => !bucket.startsWith('mpuShadowBucket')
                    && bucket !== 'users..bucket'
            ));
            return done();
        });
    }, cb);
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
            + `?prefix=overview%2E%2E%7C%2E%2E&maxKeys=${LISTING_LIMIT}`;
        if (overviewMarker) {
            url += `&marker=${encodeURIComponent(overviewMarker)}`;
        }
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
            + `?maxKeys=${LISTING_LIMIT}`;
        if (partsMarker) {
            url += `&marker=${encodeURIComponent(partsMarker)}`;
        }
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
        // Step 1
        done => async.doWhilst(
            iterDone => async.retry(
                { times: 100, interval: 5000 },
                listOverviewKeysIter,
                iterDone
            ),
            async isTruncated => isTruncated,
            done
        ),
        // Step 2
        done => async.doWhilst(
            iterDone => async.retry(
                { times: 100, interval: 5000 },
                listAllPartsIter,
                iterDone
            ),
            async isTruncated => isTruncated,
            done
        ),
    ], err => {
        if (err) {
            return cb(err);
        }
        const orphanCount = Object.keys(orphanMap).length;
        log.info('phase 1 complete', { bucket, orphanedUploadIds: orphanCount });
        if (VERBOSE) {
            Object.entries(orphanMap).forEach(([uploadId, info]) => {
                log.info('orphaned MPU found', {
                    bucket, uploadId,
                    partCount: info.partKeys.length,
                    sproxydKeyCount: info.sproxydKeys.size,
                });
            });
        }
        return cb(null, orphanMap);
    });
}

function main() {
    async.series([
        done => raftSessionsToBuckets(done),
        done => async.eachSeries(remainingBuckets, processBucket, done),
    ], err => {
        if (err) {
            log.error('an error occurred during cleanup', {
                error: { message: err.message },
            });
            process.exit(1);
        } else {
            log.info('completed MPU orphan cleanup');
            process.exit(0);
        }
    });
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
