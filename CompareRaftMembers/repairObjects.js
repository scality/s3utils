/* eslint-disable no-console */

const async = require('async');
const http = require('http');
const jsonStream = require('JSONStream');

const { Logger } = require('werelogs');
const { jsutil, errors, versioning } = require('arsenal');

const getBucketdURL = require('../VerifyBucketSproxydKeys/getBucketdURL');
const getRepairStrategy = require('./RepairObjects/getRepairStrategy');

const VID_SEP = versioning.VersioningConstants.VersionId.Separator;

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
} = process.env;

const VERBOSE = process.env.VERBOSE === '1';
const DRY_RUN = process.env.DRY_RUN === '1';

const USAGE = `
CompareRaftMembers/repairObjects.js

This script checks each entry from stdin, corresponding to a line from
the output of followerDiff.js, and repairs the object on metadata if
deemed safe to do so with a readable version.

Usage:
    node CompareRaftMembers/repairObjects.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint

Optional environment variables:
    VERBOSE: set to 1 for more verbose output (shows one line for
    every sproxyd key checked)
    DRY_RUN: set to 1 to log statuses without attempting to repair anything
`;

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

const log = new Logger('s3utils:CompareRaftMembers:repairObjects');

const countByStatus = {
    AutoRepair: 0,
    AutoRepairError: 0,
    ManualRepair: 0,
    NotRepairable: 0,
    UpdatedByClient: 0,
};

const httpAgent = new http.Agent({
    keepAlive: true,
});

let sproxydAlias;

const retryDelayMs = 1000;
const maxRetryDelayMs = 10000;

const retryParams = {
    times: 20,
    interval: retryCount => Math.min(
        // the first retry comes as "retryCount=2", hence substract 2
        retryDelayMs * (2 ** (retryCount - 2)),
        maxRetryDelayMs,
    ),
};

function httpRequest(method, url, requestBody, cb) {
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
            `error reading response from HTTP request to ${url}: ${err.message}`,
        )));
        return undefined;
    });
    req.once('error', err => cbOnce(new Error(
        `error sending HTTP request to ${url}: ${err.message}`,
    )));
    req.end(requestBody || undefined);
}

function getSproxydAlias(cb) {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    httpRequest('GET', url, null, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(
                `GET ${url} returned status ${res.statusCode}`,
            ));
        }
        const resp = JSON.parse(res.body);
        sproxydAlias = resp['ring_driver:0'].alias;
        return cb();
    });
}

function checkSproxydKeys(bucketdUrl, locations, cb) {
    if (!locations) {
        return process.nextTick(cb);
    }
    return async.eachSeries(locations, (loc, locDone) => {
        // existence check
        const sproxydUrl = `http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${loc.key}`;
        let locationNotFound = false;
        async.retry(retryParams, reqDone => httpRequest('HEAD', sproxydUrl, null, (err, res) => {
            if (err) {
                log.error('sproxyd check error', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                    error: { message: err.message },
                });
                reqDone(err);
            } else if (res.statusCode === 404) {
                log.error('sproxyd check reported missing key', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                });
                locationNotFound = true;
                reqDone();
            } else if (res.statusCode !== 200) {
                log.error('sproxyd check returned HTTP error', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                    httpCode: res.statusCode,
                });
                reqDone(err);
            } else {
                if (VERBOSE) {
                    log.info('sproxyd check returned success', {
                        bucketdUrl,
                        sproxydKey: loc.key,
                    });
                }
                reqDone();
            }
        }), err => {
            if (err) {
                return locDone(err);
            }
            if (locationNotFound) {
                return locDone(errors.LocationNotFound);
            }
            return locDone();
        });
    }, cb);
}

function parseDiffKey(diffKey) {
    const slashPos = diffKey.indexOf('/');
    const [bucket, key] = [diffKey.slice(0, slashPos), diffKey.slice(slashPos + 1)];
    return { bucket, key };
}

function repairObjectMD(bucketdUrl, repairMd, cb) {
    return httpRequest('POST', bucketdUrl, repairMd, (err, response) => {
        if (err) {
            log.error('HTTP request to repair object failed', {
                bucketdUrl,
                error: err.message,
            });
            return cb(err);
        }
        if (response.statusCode !== 200) {
            log.error('HTTP request to repair object returned an error status', {
                bucketdUrl,
                statusCode: response.statusCode,
                statusMessage: response.body,
            });
            return cb(errors.InternalError);
        }
        return cb();
    });
}

function repairDiffEntry(diffEntry, cb) {
    const {
        bucket,
        key,
    } = parseDiffKey(diffEntry[0] ? diffEntry[0].key : diffEntry[1].key);
    const keyBucketdURL = getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        Key: key,
    });
    const vidSepPos = key.lastIndexOf(VID_SEP);
    const isVersionedKey = vidSepPos !== -1;
    const masterKeyBucketdURL = isVersionedKey && getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        Key: key.slice(0, vidSepPos),
    });

    async.waterfall([
        next => async.map(diffEntry, (diffItem, itemDone) => {
            if (!diffItem) {
                return itemDone(null, false);
            }
            const parsedMd = JSON.parse(diffItem.value);
            return checkSproxydKeys(
                keyBucketdURL,
                parsedMd.location,
                err => itemDone(null, !err),
            );
        }, next),
        ([
            followerIsReadable,
            leaderIsReadable,
        ], next) => async.map(
            [
                keyBucketdURL,
                masterKeyBucketdURL,
            ],
            (url, urlDone) => {
                if (!url) {
                    return urlDone();
                }
                return async.retry(
                    retryParams,
                    reqDone => httpRequest('GET', url, null, (err, res) => {
                        if (err || (res.statusCode !== 200 && res.statusCode !== 404)) {
                            log.error('error during HTTP request to check object metadata prior to repair', {
                                bucket,
                                key,
                                error: err && err.message,
                                statusCode: res && res.statusCode,
                            });
                            return reqDone(err);
                        }
                        return reqDone(null, res);
                    }),
                    urlDone,
                );
            },
            (err, [keyRes, masterKeyRes]) => {
                if (err) {
                    return next(err);
                }
                const refreshedMd = keyRes.statusCode === 200 ? keyRes.body : null;
                const refreshedMasterMd = masterKeyRes && masterKeyRes.statusCode === 200
                    ? masterKeyRes.body : null;
                const followerState = {
                    diffMd: diffEntry[0] && diffEntry[0].value,
                    isReadable: followerIsReadable,
                };
                const leaderState = {
                    diffMd: diffEntry[1] && diffEntry[1].value,
                    isReadable: leaderIsReadable,
                    refreshedMd,
                    refreshedMasterMd,
                };
                return next(null, followerState, leaderState);
            },
        ),
        (followerState, leaderState, next) => {
            const repairStrategy = getRepairStrategy(followerState, leaderState);
            if (repairStrategy.message) {
                const logFunc = repairStrategy.status === 'NotRepairable' ? log.warn : log.info;
                logFunc.bind(log)(repairStrategy.message, {
                    bucket,
                    key,
                    repairStatus: repairStrategy.status,
                });
            }
            countByStatus[repairStrategy.status] += 1;
            if (repairStrategy.status !== 'AutoRepair') {
                return next();
            }
            const repairMd = repairStrategy.source === 'Follower'
                ? followerState.diffMd
                : leaderState.diffMd;
            if (DRY_RUN) {
                log.info('dry run: did not repair object metadata', {
                    bucket,
                    key,
                    repairStatus: 'AutoRepair',
                    repairSource: repairStrategy.source,
                    repairMaster: repairStrategy.repairMaster,
                });
                return next();
            }
            return async.each(
                [
                    keyBucketdURL,
                    repairStrategy.repairMaster ? masterKeyBucketdURL : null,
                ],
                (url, urlDone) => {
                    if (!url) {
                        return urlDone();
                    }
                    return repairObjectMD(url, repairMd, urlDone);
                },
                err => {
                    if (err) {
                        log.error('error during repair of object metadata', {
                            bucket,
                            key,
                            repairStatus: 'AutoRepair',
                            repairSource: repairStrategy.source,
                            repairMaster: repairStrategy.repairMaster,
                            error: err.message,
                        });
                        countByStatus.AutoRepairError += 1;
                        return next(err);
                    }
                    log.info('repaired object metadata successfully', {
                        bucket,
                        key,
                        repairStatus: 'AutoRepair',
                        repairSource: repairStrategy.source,
                        repairMaster: repairStrategy.repairMaster,
                    });
                    return next();
                },
            );
        },
    ], () => cb());
}

function consumeStdin(cb) {
    const diffEntryStream = process.stdin.pipe(jsonStream.parse());
    diffEntryStream
        .on('data', diffEntry => {
            diffEntryStream.pause();
            return repairDiffEntry(diffEntry, () => {
                diffEntryStream.resume();
            });
        })
        .on('end', () => {
            cb();
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
        done => getSproxydAlias(done),
        done => consumeStdin(done),
    ], err => {
        if (err) {
            log.error('an error occurred during repair', {
                error: { message: err.message },
                countByStatus,
            });
            process.exit(1);
        } else {
            log.info('completed repair', { countByStatus });
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
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
