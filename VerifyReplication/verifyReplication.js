const async = require('async');

const storage = require('./storage');

let bucketMatch = false;
let compareSize = false;
let compareAllVersions = false;
let destinationStorage = null;
let destinationClient = null;
let listingLimit = null;
let listingWorkers = null;
let mdRequestWorkers = null;
let prefixFilters = null;
let logger = null;
let sourceStorage = null;
let sourceClient = null;
let statusObj = {};

function verifyObjects(objectList, cb) {
    statusObj.srcListedCount += objectList.length;
    return async.eachLimit(objectList, mdRequestWorkers, (object, done) => {
        const { Key: key, Size: size, LastModified: srcLastModified } = object;
        const params = {
            client: destinationClient,
            bucket: statusObj.dstBucket,
            key: bucketMatch ? key : `${statusObj.srcBucket}/${key}`,
        };
        return destinationStorage.getObjMd(params, (err, dstMd) => {
            ++statusObj.dstProcessedCount;
            if (err && err.code !== 'NotFound') {
                return done(err);
            }
            if (err && err.code === 'NotFound') {
                ++statusObj.missingInDstCount;
                logger.info('object missing in destination', {
                    key,
                    size,
                    srcLastModified,
                });
                return done();
            }
            const srcSize = Number.parseInt(size, 10);
            const dstSize = Number.parseInt(dstMd.size, 10);
            if (compareSize && (srcSize !== dstSize)) {
                ++statusObj.sizeMismatchCount;
                logger.info('object size does not match in destination', {
                    key,
                    srcSize,
                    dstSize,
                    srcLastModified,
                    dstLastModified: dstMd.lastModified,
                });
            } else {
                ++statusObj.replicatedCount;
            }
            return done();
        });
    }, cb);
}

function handlePrefixes(prefixList, cb) {
    const prefixes = prefixList.map(p => p.Prefix);
    return async.eachLimit(prefixes, listingWorkers, (prefix, done) => {
        const params = {
            client: sourceClient,
            bucket: statusObj.srcBucket,
            prefix,
            listingLimit,
        };
        // eslint-disable-next-line no-use-before-define
        return listAndCompare(params, done);
    }, cb);
}

function listAndCompare(params, cb) {
    return sourceStorage.listObjects(params, (err, data) => {
        if (err) {
            return cb(err);
        }
        const {
            IsTruncated,
            NextContinuationToken: nextContinuationToken,
            Contents,
            CommonPrefixes,
        } = data;
        return async.parallel([
            done => verifyObjects(Contents, done),
            done => handlePrefixes(CommonPrefixes, done),
        ], error => {
            if (error) {
                return cb(error);
            }
            if (IsTruncated) {
                const listingParams = { ...params, nextContinuationToken };
                return listAndCompare(listingParams, cb);
            }
            return cb();
        });
    });
}

function verifyReplication(params, cb) {
    const {
        source,
        destination,
        verification,
        status,
        log,
    } = params;

    logger = log;
    statusObj = status;
    compareSize = verification.compareObjectSize;
    compareAllVersions = verification.compareObjectAllVersions;
    sourceStorage = storage[source.storageType];
    destinationStorage = storage[destination.storageType];
    sourceClient = sourceStorage.getClient(source);
    destinationClient = destinationStorage.getClient(destination);
    prefixFilters = source.prefixes ? source.prefixes.split(',') : [];
    listingLimit = source.listingLimit;
    listingWorkers = source.listingWorkers;
    mdRequestWorkers = destination.requestWorkers;
    bucketMatch = destination.bucketMatch;
    statusObj.srcBucket = source.bucket;
    statusObj.dstBucket = destination.bucket;

    // initial listing params
    const listingParams = {
        client: sourceClient,
        bucket: source.bucket,
        listingLimit,
    };

    // prefix filters
    if (prefixFilters.length > 0) {
        // include the prefix filters used in the result/summary
        status.prefixFilters = prefixFilters;
        return async.eachLimit(prefixFilters, listingWorkers, (prefix, done) => {
            const listParam = { ...listingParams, prefix };
            return listAndCompare(listParam, done);
        }, cb);
    }
    return listAndCompare({ ...listingParams, delimiter: '/' }, cb);
}

module.exports = {
    verifyReplication,
};
