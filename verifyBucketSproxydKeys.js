/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const http = require('http');
const async = require('async');
const { URL } = require('url');

const { jsutil } = require('arsenal');
const { Logger } = require('werelogs');

const DEFAULT_WORKERS = 100;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;
const DEFAULT_LISTING_LIMIT = 1000;

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
    BUCKETS, RAFT_SESSIONS, FROM_URL,
} = process.env;

const WORKERS = (
    process.env.WORKERS
        && Number.parseInt(process.env.WORKERS, 10)) || DEFAULT_WORKERS;
const VERBOSE = process.env.VERBOSE === '1';

const LISTING_LIMIT = (
    process.env.LISTING_LIMIT
        && Number.parseInt(process.env.LISTING_LIMIT, 10))
      || DEFAULT_LISTING_LIMIT;

const LOG_PROGRESS_INTERVAL = (
    process.env.LOG_PROGRESS_INTERVAL
        && Number.parseInt(process.env.LOG_PROGRESS_INTERVAL, 10))
      || DEFAULT_LOG_PROGRESS_INTERVAL;

const MPU_ONLY = process.env.MPU_ONLY === '1';

const USAGE = `
verifyBucketSproxydKeys.js

This script verifies that all sproxyd keys referenced by objects
in S3 buckets exist on the RING. It can help to identify objects
affected by the S3C-1959 bug.

Usage:
    node verifyBucketSproxydKeys.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint
    Either:
        BUCKETS: comma-separated list of buckets to scan
    or:
        RAFT_SESSIONS: comma-separated list of raft sessions to scan

Optional environment variables:
    WORKERS: concurrency value for sproxyd requests (default ${DEFAULT_WORKERS})
    FROM_URL: URL from which to resume scanning ("s3://bucket[/key]")
    VERBOSE: set to 1 for more verbose output (shows one line for every sproxyd key)
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default ${DEFAULT_LOG_PROGRESS_INTERVAL})
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
    MPU_ONLY: only scan MPU objects, i.e. objects with at least two sproxyd locations
`;

if (!BUCKETS && !RAFT_SESSIONS) {
    console.error('ERROR: either BUCKETS or RAFT_SESSIONS environment '
                  + 'variable must be defined');
    console.error(USAGE);
    process.exit(1);
}
if (BUCKETS && RAFT_SESSIONS) {
    console.error('ERROR: BUCKETS and RAFT_SESSIONS environment variables '
                  + 'cannot be both defined');
    console.error(USAGE);
    process.exit(1);
}
if (!BUCKETD_HOSTPORT) {
    console.error('ERROR: BUCKETD_HOSTPORT not defined!');
    console.error(USAGE);
    process.exit(1);
}
if (!SPROXYD_HOSTPORT) {
    console.error('ERROR: SPROXYD_HOSTPORT not defined!');
    console.error(USAGE);
    process.exit(1);
}

const log = new Logger('s3utils:verifyBucketSproxydKeys');

const httpAgent = new http.Agent({
    keepAlive: true,
});

const status = {
    objectsSkipped: 0,
    objectsScanned: 0,
    objectsWithMissingKeys: 0,
    objectsErrors: 0,
    bucketInProgress: null,
    KeyMarker: '',
};

let remainingBuckets = (BUCKETS && BUCKETS.split(',')) || [];
let sproxydAlias;

// used to skip versions that have already been processed as master key
let lastMasterKey;
let lastMasterVersionId;

function getObjectURL(bucket, objectKey) {
    if (!bucket) {
        return 's3://';
    }
    if (!objectKey) {
        return `s3://${bucket}`;
    }
    return `s3://${bucket}/${encodeURI(objectKey)}`;
}

function setupFromUrl(fromUrl) {
    if (!fromUrl.startsWith('s3://')) {
        console.error('ERROR: FROM_URL must start with "s3://"');
        console.error(USAGE);
        process.exit(1);
    }
    const trimmed = fromUrl.slice(5);
    const sep = trimmed.indexOf('/');
    const bucket = (sep !== -1 ? trimmed.slice(0, sep) : trimmed);
    const bucketIndex = remainingBuckets.indexOf(bucket);
    if (bucketIndex === -1) {
        console.error(`ERROR: FROM_URL bucket ${bucket} `
                      + `${BUCKETS ? 'is not in BUCKETS'
                          : 'does not belong to RAFT_SESSIONS'}`);
        console.error(USAGE);
        process.exit(1);
    }
    remainingBuckets = remainingBuckets.slice(bucketIndex);
    const encodedKey = trimmed.slice(bucket.length + 1);
    status.KeyMarker = decodeURI(encodedKey);
}

function logProgress(message) {
    log.info(message, {
        skipped: status.objectsSkipped,
        scanned: status.objectsScanned,
        haveMissingKeys: status.objectsWithMissingKeys,
        errors: status.objectErrors,
        url: getObjectURL(status.bucketInProgress, status.KeyMarker),
    });
}

setInterval(() => logProgress('progress update'),
    LOG_PROGRESS_INTERVAL * 1000);


function httpRequest(method, url, cb) {
    const cbOnce = jsutil.once(cb);
    const urlObj = new URL(url);
    const req = http.request({
        hostname: urlObj.hostname,
        port: urlObj.port,
        path: `${urlObj.pathname}${urlObj.search}`,
        method,
        agent: httpAgent,
    }, res => {
        if (method === 'HEAD') {
            return cbOnce(null, res);
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.once('end', () => {
            const body = chunks.join('');
            // eslint-disable-next-line no-param-reassign
            res.body = body;
            return cbOnce(null, res);
        });
        res.once('error', err => cbOnce(new Error(
            'error reading response from HTTP request '
                + `to ${url}: ${err.message}`
        )));
        return undefined;
    });
    req.once('error', err => cbOnce(new Error(
        `error sending HTTP request to ${url}: ${err.message}`
    )));
    req.end();
}

function getSproxydAlias(cb) {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    httpRequest('GET', url, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(
                `GET ${url} returned status ${res.statusCode}`
            ));
        }
        const resp = JSON.parse(res.body);
        sproxydAlias = resp['ring_driver:0'].alias;
        return cb();
    });
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

function fetchObjectLocations(bucket, objectKey, cb) {
    const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${bucket}/${encodeURI(objectKey)}`;
    httpRequest('GET', url, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
        }
        const md = JSON.parse(res.body);
        return cb(null, md.location);
    });
}

function checkSproxydKeys(objectUrl, locations, cb) {
    let keyError = false;
    let keyMissing = false;

    if (MPU_ONLY && locations.length < 2) {
        status.objectsSkipped += 1;
        return cb();
    }
    return async.eachSeries(locations, (loc, locDone) => {
        const sproxydUrl = `http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${loc.key}`;
        httpRequest('HEAD', sproxydUrl, (err, res) => {
            if (err) {
                log.error('sproxyd check error', {
                    objectUrl,
                    sproxydKey: loc.key,
                    error: err.message,
                });
                keyError = true;
            } else if (res.statusCode === 404) {
                log.error('sproxyd check reported missing key', {
                    objectUrl,
                    sproxydKey: loc.key,
                });
                keyMissing = true;
            } else if (res.statusCode !== 200) {
                log.error('sproxyd check returned HTTP error', {
                    objectUrl,
                    sproxydKey: loc.key,
                    httpCode: res.statusCode,
                });
                keyError = true;
            } else if (VERBOSE) {
                log.info('sproxyd check returned success', {
                    objectUrl,
                    sproxydKey: loc.key,
                });
            }
            locDone();
        });
    }, () => {
        status.objectsScanned += 1;
        if (keyError) {
            status.objectsErrors += 1;
        } else if (keyMissing) {
            status.objectsWithMissingKeys += 1;
        }
        return cb();
    });
}

function listBucketIter(bucket, cb) {
    const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${bucket}?maxKeys=`
          + `${LISTING_LIMIT}&marker=${status.KeyMarker ? encodeURI(status.KeyMarker) : ''}`;
    httpRequest('GET', url, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
        }
        const resp = JSON.parse(res.body);
        const { Contents, IsTruncated } = resp;

        return async.eachLimit(Contents, WORKERS, (item, itemDone) => {
            const vidSepPos = item.key.lastIndexOf('\0');
            const objectUrl = getObjectURL(bucket, item.key);

            const md = JSON.parse(item.value);
            if (vidSepPos === -1) {
                lastMasterKey = item.key;
                lastMasterVersionId = md.versionId;
            } else {
                const masterKey = item.key.slice(0, vidSepPos);
                if (masterKey === lastMasterKey
                    && md.versionId === lastMasterVersionId) {
                    // we have already processed this versioned key as
                    // the master key, so skip it
                    return itemDone();
                }
            }

            if (md['content-length'] === 0) {
                // empty object
                if (MPU_ONLY) {
                    status.objectsSkipped += 1;
                } else {
                    status.objectsScanned += 1;
                }
                return itemDone();
            }
            // big MPUs may not have their location in the listing
            // result, we need to fetch the locations array from
            // bucketd explicitly in this case
            if (md.location) {
                return checkSproxydKeys(objectUrl, md.location, itemDone);
            }
            return fetchObjectLocations(bucket, item.key, (err, locations) => {
                if (err) {
                    log.error('error fetching object locations array', {
                        objectUrl,
                        error: err.message,
                    });
                    status.objectsScanned += 1;
                    status.objectsErrors += 1;
                    return itemDone();
                }
                return checkSproxydKeys(objectUrl, locations, itemDone);
            });
        }, () => {
            if (IsTruncated) {
                status.KeyMarker = Contents[Contents.length - 1].key;
            }
            cb(null, IsTruncated);
        });
    });
}

function listBucket(bucket, cb) {
    status.bucketInProgress = bucket;
    logProgress('start scanning bucket');
    async.doWhilst(
        done => async.retry({ times: 100, interval: 5000 },
            _done => listBucketIter(bucket, _done), done),
        IsTruncated => IsTruncated,
        err => {
            status.bucketInProgress = null;
            status.KeyMarker = '';
            cb(err);
        }
    );
}


function main() {
    async.series([
        done => getSproxydAlias(done),
        done => raftSessionsToBuckets(done),
        done => {
            if (FROM_URL) {
                setupFromUrl(FROM_URL);
            }
            async.eachSeries(remainingBuckets, listBucket, done);
        },
    ], err => {
        if (err) {
            log.error('an error occurred during scan', {
                error: err.message,
            });
            logProgress('last status');
            process.exit(1);
        } else {
            logProgress('completed scan');
            if (status.objectsWithMissingKeys) {
                process.exit(101);
            }
            if (status.objectsErrors) {
                process.exit(102);
            }
            process.exit(0);
        }
    });
}

main();


function stop() {
    log.info('stopping execution');
    logProgress('last status');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
