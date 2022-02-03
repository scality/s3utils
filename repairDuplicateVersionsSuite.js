/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const async = require('async');
const crypto = require('crypto');
const http = require('http');
const { URL } = require('url');
const readline = require('readline');

const { jsutil, errors } = require('arsenal');
const { Logger } = require('werelogs');

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
} = process.env;

const log = new Logger('s3utils:repairDuplicateVersions');

const httpAgent = new http.Agent({
    keepAlive: true,
});

let sproxydAlias;
const objectsToRepair = [];

const status = {
    logLinesRead: 0,
    objectsRepaired: 0,
    objectsSkipped: 0,
    objectsErrors: 0,
    sproxydKeysCopied: 0,
    sproxydKeysCopyErrors: 0,
    sproxydBytesCopied: 0,
};

function logProgress(message) {
    log.info(message, { ...status, objectsToRepair: objectsToRepair.length, });
}

function checkStatus(property) {
    return status[property] > 0;
}

function httpRequest(method, url, reqBody, cb) {
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
        return res.once('error', err => cbOnce(new Error(
            'error reading response from HTTP request '
                + `to ${url}: ${err.message}`
        )));
    });
    req.once('error', err => cbOnce(new Error(
        `error sending HTTP request to ${url}: ${err.message}`
    )));
    if (reqBody) {
        req.setHeader('content-type', 'application/json');
        req.setHeader('content-length', reqBody.length);
        req.write(reqBody);
    }
    req.end();
}

function getSproxydAlias(cb) {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    httpRequest('GET', url, null, (err, res) => {
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

function readVerifyLog(cb) {
    const logLines = readline.createInterface({ input: process.stdin });
    logProgress('start reading verify log');
    logLines.on('line', line => {
        status.logLinesRead += 1;
        try {
            const parsedLine = JSON.parse(line);
            if (parsedLine.message !== 'duplicate sproxyd key found') {
                return undefined;
            }
            if (!parsedLine.objectUrl || !parsedLine.objectUrl2) {
                log.error('malformed verify log line: missing fields', {
                    lineNumber: status.logLinesRead,
                });
                return undefined;
            }
            objectsToRepair.push({
                objectUrl: parsedLine.objectUrl,
                objectUrl2: parsedLine.objectUrl2,
            });
        } catch (err) {
            log.info('ignoring malformed JSON line');
        }
        return undefined;
    });
    logLines.on('close', () => {
        logProgress('finished reading verify log');
        cb();
    });
}

function fetchObjectMetadata(objectUrl, cb) {
    if (!objectUrl.startsWith('s3://')) {
        return cb(new Error(`malformed object URL ${objectUrl}: must start with "s3://"`));
    }
    const bucketAndObject = objectUrl.slice(5);
    const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${bucketAndObject}`;
    return httpRequest('GET', url, null, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
        }
        const md = JSON.parse(res.body);
        return cb(null, md);
    });
}

function putObjectMetadata(objectUrl, objMD, cb) {
    if (!objectUrl.startsWith('s3://')) {
        return cb(new Error(`malformed object URL ${objectUrl}: must start with "s3://"`));
    }
    const bucketAndObject = objectUrl.slice(5);
    const url = `http://${BUCKETD_HOSTPORT}/default/bucket/${bucketAndObject}`;
    return httpRequest('POST', url, JSON.stringify(objMD), (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(`POST ${url} returned status ${res.statusCode}`));
        }
        return cb();
    });
}

function genSproxydKey(fromKey) {
    // See sproxydclient:lib/keygen.js for details on how sproxyd keys
    // are generated.
    //
    // Here, instead of needing the original info to construct the
    // key, we reuse the fields from the existing key and regenerate
    // the random parts.

    const rand = crypto.randomBytes(11);
    return [rand.slice(0, 8).toString('hex').toUpperCase(),
        fromKey.slice(16, 32),
        rand.slice(8, 11).toString('hex').toUpperCase(),
        fromKey.slice(38, 40)].join('');
}

function copySproxydKey(objectUrl, sproxydKey, cb) {
    const cbOnce = jsutil.once((err, newKey) => {
        if (err) {
            status.sproxydKeysCopyErrors += 1;
        } else {
            status.sproxydKeysCopied += 1;
        }
        cb(err, newKey);
    });
    const newKey = genSproxydKey(sproxydKey);
    const sproxydSourceUrl = new URL(`http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${sproxydKey}`);
    const sproxydDestUrl = new URL(`http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${newKey}`);
    const sourceReq = http.request({
        hostname: sproxydSourceUrl.hostname,
        port: sproxydSourceUrl.port,
        path: sproxydSourceUrl.pathname,
        method: 'GET',
        agent: httpAgent,
    }, sourceRes => {
        if (sourceRes.statusCode !== 200) {
            log.error('sproxyd returned HTTP error code', {
                objectUrl,
                sproxydKey,
                httpCode: sourceRes.statusCode,
                sproxydSourceUrl,
                sproxydDestUrl
            });
            return sourceRes.resume().once('end', () => cbOnce(errors.InternalError));
        }
        const targetReq = http.request({
            hostname: sproxydDestUrl.hostname,
            port: sproxydDestUrl.port,
            path: sproxydDestUrl.pathname,
            method: 'PUT',
            agent: httpAgent,
            headers: {
                'Content-Length': Number.parseInt(sourceRes.headers['content-length'], 10),
            },
        }, targetRes => {
            if (targetRes.statusCode !== 200) {
                log.error('sproxyd returned HTTP error code', {
                    objectUrl,
                    sproxydKey: newKey,
                    httpCode: targetRes.statusCode,
                    sproxydSourceUrl,
                    sproxydDestUrl
                });
                return cbOnce(errors.InternalError);
            }
            targetRes.once('error', err => {
                log.error('error reading response from sproxyd', {
                    objectUrl,
                    sproxydKey: newKey,
                    error: { message: err.message },
                });
                return cbOnce(errors.InternalError);
            });
            return targetRes.resume().once('end', () => {
                status.sproxydBytesCopied
                    += Number.parseInt(sourceRes.headers['content-length'], 10);
                cbOnce(null, newKey);
            });
        });
        sourceRes.pipe(targetReq);
        sourceRes.once('error', err => {
            log.error('error reading data from sproxyd', {
                objectUrl,
                sproxydKey,
                error: { message: err.message },
            });
            return cbOnce(errors.InternalError);
        });
        return targetReq.once('error', err => {
            log.error('error sending data to sproxyd', {
                objectUrl,
                sproxydKey: newKey,
                error: { message: err.message },
            });
            return cbOnce(errors.InternalError);
        });
    });
    sourceReq.once('error', err => {
        log.error('error sending request to sproxyd', {
            objectUrl,
            sproxydKey,
            error: { message: err.message },
        });
        return cbOnce(errors.InternalError);
    });
    sourceReq.end();
}

function repairObject(objInfo, cb) {
    async.mapValues({
        objectUrl: objInfo.objectUrl,
        objectUrl2: objInfo.objectUrl2,
    }, (url, key, done) => {
        fetchObjectMetadata(url, (err, md) => {
            if (err) {
                log.error('error fetching object location', {
                    objectUrl: url,
                    error: { message: err.message },
                });
                return done(err);
            }
            if (!Array.isArray(md.location)) {
                const msg = 'location field is not an array';
                log.error(msg, {
                    objectUrl: url,
                });
                return done(new Error(msg));
            }
            const locationKeys = new Set(md.location.map(loc => loc.key));
            return done(null, { md, locationKeys });
        });
    }, (err, results) => {
        if (err) {
            return cb(err);
        }
        const copiedKeys = {};
        return async.eachSeries(results.objectUrl.locationKeys, (sproxydKey, done) => {
            if (!results.objectUrl2.locationKeys.has(sproxydKey)) {
                // sproxyd key is not duplicated
                return done();
            }
            // sproxyd key is duplicated, need to copy the data to a
            // new key and update metadata for objectUrl
            return copySproxydKey(objInfo.objectUrl, sproxydKey, (err, newKey) => {
                if (err) {
                    return done(err);
                }
                log.info('sproxyd key copied', {
                    objectUrl: objInfo.objectUrl,
                    sproxydKey,
                    newKey,
                });
                copiedKeys[sproxydKey] = newKey;
                return done();
            });
        }, err => {
            if (err) {
                return cb(err);
            }
            if (Object.keys(copiedKeys).length === 0) {
                log.info('skip object already repaired', {
                    objectUrl: objInfo.objectUrl,
                });
                status.objectsSkipped += 1;
                return cb();
            }
            const objMD = results.objectUrl.md;
            objMD.location.forEach(loc => {
                if (copiedKeys[loc.key]) {
                    // eslint-disable-next-line no-param-reassign
                    loc.key = copiedKeys[loc.key];
                }
            });
            return putObjectMetadata(objInfo.objectUrl, objMD, err => {
                if (err) {
                    log.error('error putting object metadata', {
                        objectUrl: objInfo.objectUrl,
                        error: { message: err.message },
                    });
                    return cb(err);
                }
                log.info('repaired object metadata', {
                    objectUrl: objInfo.objectUrl,
                });
                status.objectsRepaired += 1;
                return cb(null, { copiedKeys, objectUrl: objInfo.objectUrl });
            });
        });
    });
}

function repairObjects(cb) {
    logProgress('start repairing objects');
    async.eachSeries(objectsToRepair, (objInfo, done) => {
        repairObject(objInfo, err => {
            if (err) {
                log.error('an error occurred repairing object', {
                    objectUrl: objInfo.objectUrl,
                    error: { message: err.message },
                });
                status.objectsErrors += 1;
            }
            done();
        });
    }, cb);
}

module.exports = {
    fetchObjectMetadata,
    putObjectMetadata,
    copySproxydKey,
    httpRequest,
    repairObject,
    repairObjects,
    readVerifyLog,
    getSproxydAlias,
    checkStatus,
    logProgress
};
