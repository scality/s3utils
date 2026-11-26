/* eslint-disable no-console */
// the scan context "ctx" is designed to be updated by scan functions
/* eslint no-param-reassign: ["warn", { "props": true, "ignorePropertyModificationsFor": ["ctx"] }] */
const http = require('http');
const { http: httpArsn } = require('httpagent');
const async = require('async');
const { URL } = require('url');
const jsonStream = require('JSONStream');

const { jsutil, versioning } = require('arsenal');
const { Logger } = require('werelogs');

const getObjectURL = require('./MetadataScan/getObjectURL');
const getBucketdURL = require('./MetadataScan/getBucketdURL');
const FindDuplicateSproxydKeys = require('./MetadataScan/FindDuplicateSproxydKeys');
const validateLocations = require('./MetadataScan/validateLocations');
const BlockDigestsStream = require('./CompareRaftMembers/BlockDigestsStream');
const BlockDigestsStorage = require('./CompareRaftMembers/BlockDigestsStorage');
const { getConfig, OPTIONS_USAGE } = require('./MetadataScan/getConfig');
const { isHelpRequested } = require('./utils/parseOptions');

// To check duplicate sproxyd keys while coping with out-of-order
// checks due to concurrency as well as duplicate keys straddling
// across multiple versions, we need to maintain duplicate info for a
// window of versions that is not too small to be confident we can
// catch all cases.
const DUPLICATE_KEYS_WINDOW_SIZE = 10000;

const USAGE = `
metadataScan.js

This script scans the metadata of objects in S3 buckets, and verifies
that:

1. sproxyd keys are unique across versions of the same object
2. object metadata is not an empty JSON object '{}'

It can help to identify objects affected by one of S3C-2731, S3C-3778
(1) or S3C-5987 (2).

With --check-missing-sproxyd-keys, it also verifies that all sproxyd
keys referenced by objects exist on the RING, which can help to
identify objects affected by the S3C-1959 bug.

The script can also be used to generate block digests from the listing
results as seen by the leader, for the purpose of finding
discrepancies between raft members, that can be caused by bugs like
S3C-5739.

Usage:
    node metadataScan.js [OPTIONS]

Each option can be set either as a command-line long option, or as an
equivalent environment variable: the command line takes precedence.
Boolean options are set on the command line with --option or
--no-option, and as environment variables with 1, true or yes, or 0,
false or no.

One of --buckets, --raft-sessions or --keys-from-stdin is mandatory.

Options:
${OPTIONS_USAGE}

Backward compatibility with the legacy script name:
  When invoked as "verifyBucketSproxydKeys.js", the check for missing
  sproxyd keys is enabled by default, and can be disabled by setting the
  NO_MISSING_KEY_CHECK environment variable to 1, true or yes.
`;

/**
 * Error caused by an invalid usage of the script, reported along
 * with the usage text
 */
class UsageError extends Error {}

/**
 * Create the status of a scan, with all counters set to zero
 *
 * @return {object} - scan status
 */
function createScanStatus() {
    return {
        objectsSkipped: 0,
        objectsScanned: 0,
        objectsWithMissingKeys: 0,
        objectsWithDupKeys: 0,
        objectsWithEmptyMetadata: 0,
        objectsWithDupVersionIds: 0,
        objectsWithBrokenMetadata: 0,
        objectsErrors: 0,
        bucketInProgress: null,
        KeyMarker: '',
        bucketsDeleted: 0,
    };
}

/**
 * Create the context holding the configuration and state of a scan
 *
 * @param {object} config - scan configuration, see getConfig()
 * @param {werelogs.Logger} log - logger
 * @return {object} - scan context, passed to all scan functions
 */
function createScanContext(config, log) {
    const ctx = {
        config,
        log,
        httpAgent: new httpArsn.Agent({
            keepAlive: true,
        }),
        status: createScanStatus(),
        remainingBuckets: config.buckets || [],
        sproxydAlias: undefined,
        // used to skip versions that have already been processed as master key
        lastMasterKey: undefined,
        lastMasterVersionId: undefined,
        // helper to detect duplicate sproxyd keys
        findDuplicateSproxydKeys: undefined,
        // digests stream, to create block digests from a stream of key/value pairs
        digestsStream: undefined,
        levelStream: undefined,
    };
    if (config.listingDigestsOutputDir) {
        ctx.digestsStream = new BlockDigestsStream({ blockSize: config.listingDigestsBlockSize });
        ctx.levelStream = new BlockDigestsStorage({ levelPath: config.listingDigestsOutputDir });
        ctx.digestsStream.pipe(ctx.levelStream);
    }
    return ctx;
}

/**
 * Restrict the buckets to scan and set the initial listing marker
 * from the FROM_URL option
 *
 * @param {object} ctx - scan context
 * @param {string} fromUrl - URL to resume from ("s3://bucket[/key]")
 * @return {undefined}
 * @throws {UsageError} - if the URL is invalid or its bucket is not
 * part of the buckets to scan
 */
function setupFromUrl(ctx, fromUrl) {
    if (!fromUrl.startsWith('s3://')) {
        throw new UsageError('FROM_URL must start with "s3://"');
    }
    const trimmed = fromUrl.slice(5);
    const sep = trimmed.indexOf('/');
    const bucket = (sep !== -1 ? trimmed.slice(0, sep) : trimmed);
    const bucketIndex = ctx.remainingBuckets.indexOf(bucket);
    if (bucketIndex === -1) {
        throw new UsageError(`FROM_URL bucket ${bucket} `
                             + `${ctx.config.buckets ? 'is not in BUCKETS'
                                 : 'does not belong to RAFT_SESSIONS'}`);
    }
    ctx.remainingBuckets = ctx.remainingBuckets.slice(bucketIndex);
    const encodedKey = trimmed.slice(bucket.length + 1);
    ctx.status.KeyMarker = decodeURI(encodedKey);
}

/**
 * Log the scan status counters and current position
 *
 * @param {object} ctx - scan context, only "log", "config" and
 * "status" attributes are used
 * @param {string} message - log message
 * @return {undefined}
 */
function logProgress(ctx, message) {
    const { log, config, status } = ctx;
    log.info(message, {
        skipped: status.objectsSkipped,
        scanned: status.objectsScanned,
        haveMissingKeys: config.checkMissingSproxydKeys ? status.objectsWithMissingKeys : undefined,
        haveDupKeys: status.objectsWithDupKeys,
        haveEmptyMetadata: status.objectsWithEmptyMetadata,
        haveDupVersionIds: status.objectsWithDupVersionIds,
        haveBrokenMetadata: status.objectsWithBrokenMetadata,
        errors: status.objectsErrors,
        url: getObjectURL(status.bucketInProgress, status.KeyMarker),
    });
}

function httpRequest(agent, method, url, cb) {
    const cbOnce = jsutil.once(cb);
    const urlObj = new URL(url);
    const req = http.request({
        hostname: urlObj.hostname,
        port: urlObj.port,
        path: `${urlObj.pathname}${urlObj.search}`,
        method,
        agent,
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

function getSproxydAlias(ctx, cb) {
    const url = `http://${ctx.config.sproxydHostport}/.conf`;
    httpRequest(ctx.httpAgent, 'GET', url, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(
                `GET ${url} returned status ${res.statusCode}`
            ));
        }
        const resp = JSON.parse(res.body);
        ctx.sproxydAlias = resp['ring_driver:0'].alias;
        return cb();
    });
}

function raftSessionsToBuckets(ctx, cb) {
    if (!ctx.config.raftSessions) {
        return cb();
    }
    const rsList = ctx.config.raftSessions;
    return async.each(rsList, (rs, done) => {
        const url = `http://${ctx.config.bucketdHostport}/_/raft_sessions/${rs}/bucket`;
        httpRequest(ctx.httpAgent, 'GET', url, (err, res) => {
            if (err) {
                return cb(err);
            }
            if (res.statusCode !== 200) {
                return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
            }
            const resp = JSON.parse(res.body);
            ctx.remainingBuckets = ctx.remainingBuckets.concat(resp.filter(
                bucket => !bucket.startsWith('mpuShadowBucket')
                    && bucket !== 'users..bucket'
            ));
            return done();
        });
    }, cb);
}

function fetchObjectLocations(ctx, bucket, objectKey, cb) {
    const url = getBucketdURL(ctx.config.bucketdHostport, {
        Bucket: bucket,
        Key: objectKey,
    });
    httpRequest(ctx.httpAgent, 'GET', url, (err, res) => {
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

function checkSproxydKeys(ctx, objectUrl, locations, cb) {
    const { config, log, status } = ctx;
    if (!validateLocations(objectUrl, locations, status, ctx.findDuplicateSproxydKeys)) {
        return process.nextTick(cb);
    }

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

    const dupInfo = ctx.findDuplicateSproxydKeys.insertVersion(objectUrl, locations.map(loc => loc.key));
    if (dupInfo) {
        log.error('duplicate sproxyd key found', {
            objectUrl,
            objectUrl2: dupInfo.objectId,
            sproxydKey: dupInfo.key,
        });
        dupKey = true;
    }
    if (!config.checkMissingSproxydKeys) {
        if (config.verbose) {
            locations.forEach(loc => log.info('sproxyd key', {
                objectUrl,
                sproxydKey: loc.key,
            }));
        }
        return process.nextTick(onComplete);
    }
    return async.eachSeries(locations, (loc, locDone) => {
        // existence check
        const sproxydUrl = `http://${config.sproxydHostport}/${ctx.sproxydAlias}/${loc.key}`;
        httpRequest(ctx.httpAgent, 'HEAD', sproxydUrl, (err, res) => {
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
            } else if (config.verbose) {
                log.info('sproxyd check returned success', {
                    objectUrl,
                    sproxydKey: loc.key,
                });
            }
            locDone();
        });
    }, onComplete);
}

function fetchAndCheckObject(ctx, bucket, itemKey, cb) {
    const objectUrl = getObjectURL(bucket, itemKey);
    return fetchObjectLocations(ctx, bucket, itemKey, (err, locations) => {
        if (err) {
            ctx.log.error('error fetching object locations array', {
                objectUrl,
                error: { message: err.message },
            });
            ctx.status.objectsScanned += 1;
            ctx.status.objectsErrors += 1;
            return cb();
        }
        return checkSproxydKeys(ctx, objectUrl, locations, () => cb(locations));
    });
}

function listBucketIter(ctx, bucket, cb) {
    const { config, log, status, digestsStream } = ctx;
    const url = getBucketdURL(config.bucketdHostport, {
        Bucket: bucket,
        MaxKeys: config.listingLimit,
        KeyMarker: status.KeyMarker,
    });
    httpRequest(ctx.httpAgent, 'GET', url, (err, res) => {
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

        return async.mapLimit(Contents, config.workers, (item, itemDone) => {
            if (item.key.startsWith(versioning.VersioningConstants.DbPrefixes.Replay)) {
                return itemDone();
            }
            const vidSepPos = item.key.lastIndexOf('\0');
            const objectUrl = getObjectURL(bucket, item.key);
            let digestKey = item.key;

            const md = JSON.parse(item.value);
            if (vidSepPos === -1) {
                const reVersionIds = /"versionId":"([^"]*)"/g;
                const versionIds = [];
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
                    md.versionId = versionIds[0];
                }
                ctx.lastMasterKey = item.key;
                ctx.lastMasterVersionId = md.versionId;
                if (md.versionId) {
                    digestKey = `${item.key}\0${md.versionId}`;
                }
            } else {
                const masterKey = item.key.slice(0, vidSepPos);
                if (masterKey === ctx.lastMasterKey
                    && md.versionId === ctx.lastMasterVersionId) {
                    // we have already processed this versioned key as
                    // the master key, so skip it
                    ctx.findDuplicateSproxydKeys.skipVersion();
                    return itemDone();
                }
            }

            if (item.value === '{}') {
                log.error('object with empty metadata found', {
                    objectUrl,
                });
                status.objectsScanned += 1;
                status.objectsWithEmptyMetadata += 1;
                ctx.findDuplicateSproxydKeys.skipVersion();
                return itemDone();
            }

            if (md.isPHD) {
                // object is a Place Holder Delete (PHD)
                ctx.findDuplicateSproxydKeys.skipVersion();
                return itemDone();
            }

            const isNotMPU = md['content-md5'] && md['content-md5'].indexOf('-') === -1;

            if (config.mpuOnly && isNotMPU) {
                // not an MPU object
                status.objectsSkipped += 1;
                ctx.findDuplicateSproxydKeys.skipVersion();
                return itemDone();
            }

            if (md['content-length'] === 0) {
                // empty object
                status.objectsScanned += 1;
                ctx.findDuplicateSproxydKeys.skipVersion();
                // empty objects should be included in the digest,
                // hence returning their metadata to the map results
                return itemDone(null, { key: digestKey, value: item.value });
            }

            // big MPUs may not have their location in the listing
            // result, we need to fetch the locations array from
            // bucketd explicitly in this case
            if (md.location) {
                return checkSproxydKeys(ctx, objectUrl, md.location, () => {
                    itemDone(null, { key: digestKey, value: item.value });
                });
            }
            return fetchAndCheckObject(ctx, bucket, item.key, location => {
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

function listBucket(ctx, bucket, cb) {
    ctx.status.bucketInProgress = bucket;

    ctx.findDuplicateSproxydKeys = new FindDuplicateSproxydKeys(DUPLICATE_KEYS_WINDOW_SIZE);

    logProgress(ctx, 'start scanning bucket');
    async.doWhilst(
        done => async.retry(
            { times: 100, interval: 5000 },
            _done => listBucketIter(ctx, bucket, _done),
            done
        ),
        async IsTruncated => IsTruncated,
        err => {
            ctx.status.bucketInProgress = null;
            ctx.status.KeyMarker = '';
            if (ctx.digestsStream) {
                // flush the digests stream to output the last bucket block
                ctx.digestsStream.flush();
            }
            cb(err);
        }
    );
}

function consumeStdin(ctx, cb) {
    const { config, log } = ctx;
    ctx.findDuplicateSproxydKeys = new FindDuplicateSproxydKeys(DUPLICATE_KEYS_WINDOW_SIZE);

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
            if (nParallel === config.workers) {
                itemStream.pause();
            }
            return fetchAndCheckObject(ctx, item.bucket, item.key, () => {
                if (nParallel === config.workers) {
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

/**
 * Get the process exit code reflecting the scan results
 *
 * @param {object} scanStatus - scan status counters
 * @return {number} - 0 if no issue was found, otherwise an exit code
 * specific to the first kind of issue found, in order of precedence:
 * 101 (missing sproxyd keys), 102 (duplicate sproxyd keys), 104
 * (empty metadata), 105 (broken metadata), 103 (other errors)
 */
function getExitCode(scanStatus) {
    if (scanStatus.objectsWithMissingKeys) {
        return 101;
    }
    if (scanStatus.objectsWithDupKeys) {
        return 102;
    }
    if (scanStatus.objectsWithEmptyMetadata) {
        return 104;
    }
    if (scanStatus.objectsWithBrokenMetadata) {
        return 105;
    }
    if (scanStatus.objectsErrors) {
        return 103;
    }
    return 0;
}

/**
 * Run the scan
 *
 * @param {object} ctx - scan context, see createScanContext()
 * @param {function} cb - callback(err, exitCode): err is a
 * UsageError for invalid options, or another error if the scan could
 * not complete; exitCode is the process exit code reflecting the
 * scan results, see getExitCode()
 * @return {undefined}
 */
function main(ctx, cb) {
    const { config } = ctx;
    const progressTimer = setInterval(
        () => logProgress(ctx, 'progress update'),
        config.logProgressInterval * 1000
    );
    async.series([
        done => {
            if (!config.checkMissingSproxydKeys) {
                return done();
            }
            return getSproxydAlias(ctx, done);
        },
        done => raftSessionsToBuckets(ctx, done),
        done => {
            if (config.fromUrl) {
                try {
                    setupFromUrl(ctx, config.fromUrl);
                } catch (err) {
                    return done(err);
                }
            }
            if (config.keysFromStdin) {
                return consumeStdin(ctx, done);
            }
            return async.eachSeries(
                ctx.remainingBuckets,
                (bucket, bucketDone) => listBucket(ctx, bucket, bucketDone),
                done
            );
        },
        done => {
            if (ctx.digestsStream) {
                ctx.digestsStream.end();
                ctx.levelStream.on('finish', done);
            } else {
                done();
            }
        },
    ], err => {
        // release resources that would keep the process alive
        clearInterval(progressTimer);
        ctx.httpAgent.destroy();

        if (err instanceof UsageError) {
            return cb(err);
        }
        if (err) {
            ctx.log.error('an error occurred during scan', {
                error: { message: err.message },
            });
            logProgress(ctx, 'last status');
            return cb(err);
        }
        logProgress(ctx, 'completed scan');
        return cb(null, getExitCode(ctx.status));
    });
}

function stop(ctx) {
    if (ctx.digestsStream) {
        ctx.digestsStream.destroy();
    }
    ctx.log.info('stopping execution');
    logProgress(ctx, 'last status');
    process.exit(0);
}

// Run the scan and wire up signal handlers only when this file is launched
// as a CLI. Skip both when a test requires the file as a module — otherwise
// the SIGINT / SIGTERM handlers would register against the Jest worker
// process and `stop()` could silently exit it on signal (which would mask
// CI timeouts as passing runs).
if (require.main === module) {
    const argv = process.argv;
    if (isHelpRequested(argv.slice(2))) {
        console.log(USAGE);
        process.exit(0);
    }

    let config;
    try {
        config = getConfig({ env: process.env, argv });
    } catch (err) {
        console.error(`ERROR: ${err.message}`);
        console.error(USAGE);
        process.exit(1);
    }

    const ctx = createScanContext(config, new Logger('s3utils:metadataScan'));
    main(ctx, (err, exitCode) => {
        if (err instanceof UsageError) {
            console.error(`ERROR: ${err.message}`);
            console.error(USAGE);
        }
        process.exit(err ? 1 : exitCode);
    });
    for (const signal of ['SIGINT', 'SIGHUP', 'SIGQUIT', 'SIGTERM']) {
        process.on(signal, () => stop(ctx));
    }
}

module.exports = {
    httpRequest,
    createScanStatus,
    logProgress,
    getExitCode,
};
