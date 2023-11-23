/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const http = require('http');
const { http: httpArsn } = require('httpagent');
const async = require('async');
const { URL } = require('url');
const jsonStream = require('JSONStream');

const { jsutil, versioning } = require('arsenal');
const { Logger } = require('werelogs');

const getObjectURL = require('./VerifyBucketSproxydKeys/getObjectURL');
const getBucketdURL = require('./VerifyBucketSproxydKeys/getBucketdURL');
const FindDuplicateSproxydKeys = require('./VerifyBucketSproxydKeys/FindDuplicateSproxydKeys');
const BlockDigestsStream = require('./CompareRaftMembers/BlockDigestsStream');
const BlockDigestsStorage = require('./CompareRaftMembers/BlockDigestsStorage');

const DEFAULT_WORKERS = 100;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;
const DEFAULT_LISTING_LIMIT = 1000;
const DEFAULT_LISTING_DIGESTS_BLOCK_SIZE = 1000;

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
    BUCKETS, RAFT_SESSIONS, KEYS_FROM_STDIN, FROM_URL,
    LISTING_DIGESTS_OUTPUT_DIR,
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
const NO_MISSING_KEY_CHECK = process.env.NO_MISSING_KEY_CHECK === '1';

// To check duplicate sproxyd keys while coping with out-of-order
// checks due to concurrency as well as duplicate keys straddling
// across multiple versions, we need to maintain duplicate info for a
// window of versions that is not too small to be confident we can
// catch all cases.
const DUPLICATE_KEYS_WINDOW_SIZE = 10000;

const LISTING_DIGESTS_BLOCK_SIZE = (
    process.env.LISTING_DIGESTS_BLOCK_SIZE
        && Number.parseInt(process.env.LISTING_DIGESTS_BLOCK_SIZE, 10))
      || DEFAULT_LISTING_DIGESTS_BLOCK_SIZE;

const USAGE = `
verifyBucketSproxydKeys.js

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

Usage:
    node verifyBucketSproxydKeys.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint (optional if NO_MISSING_KEY_CHECK=1)
    One of:
        BUCKETS: comma-separated list of buckets to scan
    or:
        RAFT_SESSIONS: comma-separated list of raft sessions to scan
    or:
        KEYS_FROM_STDIN: reads objects to scan from stdin if this environment variable is
          set, where the input is a stream of JSON objects, each of the form:
          {"bucket":"bucketname","key":"objectkey\\0objectversion"}
          note: if "\\0objectversion" is not present, it checks the master key

Optional environment variables:
    WORKERS: concurrency value for sproxyd requests (default ${DEFAULT_WORKERS})
    FROM_URL: URL from which to resume scanning ("s3://bucket[/key]")
    VERBOSE: set to 1 for more verbose output (shows one line for every sproxyd key)
    LOG_PROGRESS_INTERVAL: interval in seconds between progress update log lines (default ${DEFAULT_LOG_PROGRESS_INTERVAL})
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
    MPU_ONLY: only scan objects uploaded with multipart upload method
    NO_MISSING_KEY_CHECK: do not check for existence of sproxyd keys,
    for a performance benefit - other checks like duplicate keys and
    empty metadata are still done
    LISTING_DIGESTS_OUTPUT_DIR: output listing digests into the specified directory (in the LevelDB format)
    LISTING_DIGESTS_BLOCK_SIZE: number of keys in each listing digest block (default ${DEFAULT_LISTING_DIGESTS_BLOCK_SIZE})
`;

if (!BUCKETS && !RAFT_SESSIONS && !KEYS_FROM_STDIN) {
    console.error('ERROR: either BUCKETS or RAFT_SESSIONS or KEYS_FROM_STDIN environment '
                  + 'variable must be defined');
    console.error(USAGE);
    process.exit(1);
}
if ((BUCKETS ? 1 : 0) + (RAFT_SESSIONS ? 1 : 0) + (KEYS_FROM_STDIN ? 1 : 0) > 1) {
    console.error('ERROR: only one of BUCKETS, RAFT_SESSIONS or KEYS_FROM_STDIN environment '
                  + 'variables can be defined');
    console.error(USAGE);
    process.exit(1);
}
if (!BUCKETD_HOSTPORT) {
    console.error('ERROR: BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!SPROXYD_HOSTPORT && !NO_MISSING_KEY_CHECK) {
    console.error('ERROR: SPROXYD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}

const log = new Logger('s3utils:verifyBucketSproxydKeys');

const httpAgent = new httpArsn.Agent({
    keepAlive: true,
});

const status = {
    objectsSkipped: 0,
    objectsScanned: 0,
    objectsWithMissingKeys: 0,
    objectsWithDupKeys: 0,
    objectsWithEmptyMetadata: 0,
    objectsWithDupVersionIds: 0,
    objectsErrors: 0,
    bucketInProgress: null,
    KeyMarker: '',
    bucketsDeleted: 0
};

let remainingBuckets = (BUCKETS && BUCKETS.split(',')) || [];
let sproxydAlias;

// used to skip versions that have already been processed as master key
let lastMasterKey;
let lastMasterVersionId;

// helper to detect duplicate sproxyd keys
let findDuplicateSproxydKeys;

// digests stream, to create block digests from a stream of key/value pairs
let digestsStream;
let levelStream;
if (LISTING_DIGESTS_OUTPUT_DIR) {
    digestsStream = new BlockDigestsStream({ blockSize: LISTING_DIGESTS_BLOCK_SIZE });
    levelStream = new BlockDigestsStorage({ levelPath: LISTING_DIGESTS_OUTPUT_DIR });
    digestsStream.pipe(levelStream);
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
        haveMissingKeys: NO_MISSING_KEY_CHECK ? undefined : status.objectsWithMissingKeys,
        haveDupKeys: status.objectsWithDupKeys,
        haveEmptyMetadata: status.objectsWithEmptyMetadata,
        haveDupVersionIds: status.objectsWithDupVersionIds,
        haveObjectsErrors: status.objectsErrors,
        url: getObjectURL(status.bucketInProgress, status.KeyMarker),
    });
}

setInterval(
    () => logProgress('progress update'),
    LOG_PROGRESS_INTERVAL * 1000
);


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
    const url = getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        Key: objectKey,
    });
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
    let dupKey = false;

    const onComplete = () => {
        status.objectsScanned += 1;
        if (keyError) {
            status.objectsErrors += 1;
        } else if (keyMissing) {
            status.objectsWithMissingKeys += 1;
        }
        if (dupKey) {
            status.objectsWithDupKeys += 1;
        }
        return cb();
    };

    const dupInfo = findDuplicateSproxydKeys.insertVersion(objectUrl, locations.map(loc => loc.key));
    if (dupInfo) {
        log.error('duplicate sproxyd key found', {
            objectUrl,
            objectUrl2: dupInfo.objectId,
            sproxydKey: dupInfo.key,
        });
        dupKey = true;
    }
    if (NO_MISSING_KEY_CHECK) {
        if (VERBOSE) {
            locations.forEach(loc => log.info('sproxyd key', {
                objectUrl,
                sproxydKey: loc.key,
            }));
        }
        return process.nextTick(onComplete);
    }
    return async.eachSeries(locations, (loc, locDone) => {
        // existence check
        const sproxydUrl = `http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${loc.key}`;
        httpRequest('HEAD', sproxydUrl, (err, res) => {
            if (err) {
                log.error('sproxyd check error', {
                    objectUrl,
                    sproxydKey: loc.key,
                    error: { message: err.message },
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
    }, onComplete);
}

function fetchAndCheckObject(bucket, itemKey, cb) {
    const objectUrl = getObjectURL(bucket, itemKey);
    return fetchObjectLocations(bucket, itemKey, (err, locations) => {
        if (err) {
            log.error('error fetching object locations array', {
                objectUrl,
                error: { message: err.message },
            });
            status.objectsScanned += 1;
            status.objectsErrors += 1;
            return cb();
        }
        return checkSproxydKeys(objectUrl, locations, () => cb(locations));
    });
}

function listBucketIter(bucket, cb) {
    const url = getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        MaxKeys: LISTING_LIMIT,
        KeyMarker: status.KeyMarker,
    });
    httpRequest('GET', url, (err, res) => {
        if (err) {
            return cb(err);
        }

        if (res.statusCode === 404) {
            // the bucket was deleted during scan, so we continue to next bucket
            status.bucketsDeleted += 1;
            return cb(null, false);
        }

        if (res.statusCode !== 200) {
            return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
        }
        const resp = JSON.parse(res.body);
        const { Contents, IsTruncated } = resp;

        return async.mapLimit(Contents, WORKERS, (item, itemDone) => {
            if (item.key.startsWith(versioning.VersioningConstants.DbPrefixes.Replay)) {
                return itemDone();
            }
            const vidSepPos = item.key.lastIndexOf('\0');
            const objectUrl = getObjectURL(bucket, item.key);
            let digestKey = item.key;
            let md;
            try {
                md = JSON.parse(item.value);
            } catch (e) {
                log.error('Invalid Metadata JSON', {
                    object: objectUrl,
                    error: e.message,
                    mdValue: item.value,
                });
                status.objectsScanned += 1;
                status.objectsErrors += 1;
                return itemDone();
            }
            if (md.isNull && !md.location && !md['md-model-version']) {
                log.error('Null Metadata JSON', {
                    object: objectUrl,
                    mdValue: item.value,
                });
                status.objectsScanned += 1;
                status.objectsErrors += 1;
                return itemDone();
            }
            if (vidSepPos === -1) {
                const reVersionIds = /"versionId":"([^"]*)"/g;
                const versionIds = [];
                // eslint-disable-next-line no-constant-condition
                while (true) {
                    const reVersionIdMatch = reVersionIds.exec(item.value);
                    if (!reVersionIdMatch) {
                        break;
                    }
                    versionIds.push(reVersionIdMatch[1]);
                }
                if (versionIds.length > 1) {
                    const versionedKey = `${item.key}\0${versionIds[0]}`;
                    const versionedKeyUrl = getObjectURL(bucket, versionedKey);
                    log.error('object master metadata with duplicate "versionId" field found', {
                        objectUrl,
                        firstVersionId: versionIds[0],
                        versionedKeyUrl,
                    });
                    status.objectsWithDupVersionIds += 1;
                    // replace with the (assumed legit) first version
                    // ID to allow correct skipping of version keys
                    // eslint-disable-next-line prefer-destructuring
                    md.versionId = versionIds[0];
                }
                lastMasterKey = item.key;
                lastMasterVersionId = md.versionId;
                if (md.versionId) {
                    digestKey = `${item.key}\0${md.versionId}`;
                }
            } else {
                const masterKey = item.key.slice(0, vidSepPos);
                if (masterKey === lastMasterKey
                    && md.versionId === lastMasterVersionId) {
                    // we have already processed this versioned key as
                    // the master key, so skip it
                    findDuplicateSproxydKeys.skipVersion();
                    return itemDone();
                }
            }

            if (item.value === '{}') {
                log.error('object with empty metadata found', {
                    objectUrl,
                });
                status.objectsScanned += 1;
                status.objectsWithEmptyMetadata += 1;
                findDuplicateSproxydKeys.skipVersion();
                return itemDone();
            }

            if (md.isPHD) {
                // object is a Place Holder Delete (PHD)
                findDuplicateSproxydKeys.skipVersion();
                return itemDone();
            }

            const isNotMPU = md['content-md5'] && md['content-md5'].indexOf('-') === -1;

            if (MPU_ONLY && isNotMPU) {
                // not an MPU object
                status.objectsSkipped += 1;
                findDuplicateSproxydKeys.skipVersion();
                return itemDone();
            }

            if (md['content-length'] === 0) {
                // empty object
                status.objectsScanned += 1;
                findDuplicateSproxydKeys.skipVersion();
                // empty objects should be included in the digest,
                // hence returning their metadata to the map results
                return itemDone(null, { key: digestKey, value: item.value });
            }

            // big MPUs may not have their location in the listing
            // result, we need to fetch the locations array from
            // bucketd explicitly in this case
            if (md.location) {
                return checkSproxydKeys(objectUrl, md.location, () => {
                    itemDone(null, { key: digestKey, value: item.value });
                });
            }
            return fetchAndCheckObject(bucket, item.key, location => {
                if (location && digestsStream) {
                    // thanks to preservation of JS object attribute
                    // order, we can attach the locations array and
                    // reconstruct the metadata blob to match what is
                    // stored in the DB on disk. This is needed for
                    // the block digests to have a chance to match.
                    const fullMdBlob = JSON.stringify(Object.assign(md, { location }));
                    itemDone(null, { key: digestKey, value: fullMdBlob });
                } else {
                    // if there was an error fetching locations, just ignore the entry
                    itemDone();
                }
            });
        }, (_, items) => {
            if (IsTruncated) {
                status.KeyMarker = Contents[Contents.length - 1].key;
            }
            if (digestsStream) {
                items.forEach(item => {
                    if (item) {
                        const { key, value } = item;
                        digestsStream.write({ key: `${bucket}/${key}`, value });
                    }
                });
            }
            if (digestsStream && digestsStream.writableNeedDrain) {
                digestsStream.once('drain', () => cb(null, IsTruncated));
            } else {
                cb(null, IsTruncated);
            }
        });
    });
}

function listBucket(bucket, cb) {
    status.bucketInProgress = bucket;

    findDuplicateSproxydKeys = new FindDuplicateSproxydKeys(DUPLICATE_KEYS_WINDOW_SIZE);

    logProgress('start scanning bucket');
    async.doWhilst(
        done => async.retry(
            { times: 100, interval: 5000 },
            _done => listBucketIter(bucket, _done),
            done
        ),
        IsTruncated => IsTruncated,
        err => {
            status.bucketInProgress = null;
            status.KeyMarker = '';
            if (digestsStream) {
                // flush the digests stream to output the last bucket block
                digestsStream.flush();
            }
            cb(err);
        }
    );
}

function consumeStdin(cb) {
    findDuplicateSproxydKeys = new FindDuplicateSproxydKeys(DUPLICATE_KEYS_WINDOW_SIZE);

    const itemStream = process.stdin.pipe(jsonStream.parse());
    let nParallel = 0;
    let streamDone = false;
    itemStream
        .on('data', item => {
            if (!item.bucket) {
                log.error('missing "bucket" attribute in JSON stream item');
                return undefined;
            }
            if (!item.key) {
                log.error('missing "key" attribute in JSON stream item');
                return undefined;
            }
            ++nParallel;
            if (nParallel === WORKERS) {
                itemStream.pause();
            }
            return fetchAndCheckObject(item.bucket, item.key, () => {
                if (nParallel === WORKERS) {
                    itemStream.resume();
                }
                --nParallel;
                if (nParallel === 0 && streamDone) {
                    cb();
                }
            });
        })
        .on('end', () => {
            streamDone = true;
            if (nParallel === 0) {
                cb();
            }
        })
        .on('error', err => {
            log.error('error parsing JSON input', {
                error: err.message,
            });
            return cb(err);
        });
}


function main() {
    async.series([
        done => {
            if (NO_MISSING_KEY_CHECK) {
                return done();
            }
            return getSproxydAlias(done);
        },
        done => raftSessionsToBuckets(done),
        done => {
            if (FROM_URL) {
                setupFromUrl(FROM_URL);
            }
            if (KEYS_FROM_STDIN) {
                return consumeStdin(done);
            }
            return async.eachSeries(remainingBuckets, listBucket, done);
        },
        done => {
            if (digestsStream) {
                digestsStream.end();
                levelStream.on('finish', done);
            } else {
                done();
            }
        },
    ], err => {
        if (err) {
            log.error('an error occurred during scan', {
                error: { message: err.message },
            });
            logProgress('last status');
            process.exit(1);
        } else {
            logProgress('completed scan');
            if (status.objectsWithMissingKeys) {
                process.exit(101);
            }
            if (status.objectsWithDupKeys) {
                process.exit(102);
            }
            if (status.objectsWithEmptyMetadata) {
                process.exit(104);
            }
            if (status.objectsErrors) {
                process.exit(103);
            }
            process.exit(0);
        }
    });
}

main();

function stop() {
    if (digestsStream) {
        digestsStream.destroy();
    }
    log.info('stopping execution');
    logProgress('last status');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);

module.exports = { httpRequest };
