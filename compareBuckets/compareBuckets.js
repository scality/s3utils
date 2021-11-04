/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const async = require('async');

const { listBucketMasterKeys, getObjectURL } = require('./utils');

function compareBuckets(params, log, cb) {
    const {
        bucketdSrcParams,
        bucketdDstParams,
        statusObj,
    } = params;

    statusObj.srcBucketInProgress = bucketdSrcParams.bucket;
    statusObj.dstBucketInProgress = bucketdDstParams.bucket;

    let srcDone = false;
    let dstDone = false;
    let srcMarker = '';
    let dstMarker = '';
    let srcContents = [];
    let dstContents = [];

    async.doWhilst(
        done => {
            async.parallel({
                src: _done => {
                    if (srcDone || srcContents.length > 0) {
                        return process.nextTick(_done);
                    }

                    return listBucketMasterKeys(bucketdSrcParams,
                        (err, isTruncated, marker, contents) => {
                            if (err) {
                                return _done(err);
                            }

                            srcContents = contents;
                            srcDone = !isTruncated;
                            srcMarker = marker;
                            statusObj.srcKeyMarker = srcMarker;
                            return _done();
                        });
                },
                dst: _done => {
                    if (dstDone || dstContents.length > 0) {
                        return process.nextTick(_done);
                    }

                    return listBucketMasterKeys(bucketdDstParams,
                        (err, isTruncated, marker, contents) => {
                            if (err) {
                                return _done(err);
                            }

                            dstContents = contents;
                            dstDone = !isTruncated;
                            dstMarker = marker;
                            statusObj.dstKeyMarker = dstMarker;
                            return _done();
                        });
                },
            }, err => {
                if (err) {
                    return done(err);
                }

                let srcIdx = 0;
                let dstIdx = 0;

                if (srcDone && srcContents.length === 0) {
                    while (dstIdx < dstContents.length) {
                        log.error('missing object in source', {
                            bucket: bucketdSrcParams.bucket,
                            object: dstContents[dstIdx].key,
                            objectUrl: getObjectURL(bucketdDstParams.bucket, dstContents[dstIdx].key),
                        });
                        ++statusObj.missingInSrcCount;
                        ++dstIdx;
                    }
                }

                if (dstDone && dstContents.length === 0) {
                    while (srcIdx < srcContents.length) {
                        log.error('missing object in destination', {
                            bucket: bucketdDstParams.bucket,
                            object: srcContents[srcIdx].key,
                            objectUrl: getObjectURL(bucketdSrcParams.bucket, srcContents[srcIdx].key),
                        });
                        ++statusObj.missingInDstCount;
                        ++srcIdx;
                    }
                }

                while (srcIdx < srcContents.length && dstIdx < dstContents.length) {
                    if (srcContents[srcIdx].key < dstContents[dstIdx].key) {
                        log.error('missing object in destination', {
                            bucket: bucketdDstParams.bucket,
                            object: srcContents[srcIdx].key,
                            objectUrl: getObjectURL(bucketdSrcParams.bucket, srcContents[srcIdx].key),
                        });
                        ++statusObj.missingInDstCount;
                        ++srcIdx;
                        continue;
                    }

                    if (srcContents[srcIdx].key > dstContents[dstIdx].key) {
                        log.error('missing object in source', {
                            bucket: bucketdSrcParams.bucket,
                            object: dstContents[dstIdx].key,
                            objectUrl: getObjectURL(bucketdDstParams.bucket, dstContents[dstIdx].key),
                        });
                        ++statusObj.missingInSrcCount;
                        ++dstIdx;
                        continue;
                    }

                    ++dstIdx;
                    ++srcIdx;
                }

                srcContents = srcContents.slice(srcIdx);
                dstContents = dstContents.slice(dstIdx);
                return setTimeout(() => done(null), 500);
            });
        },
        () => (!srcDone || !dstDone || srcContents.length > 0 || dstContents.length > 0),
        err => {
            statusObj.dstBucketInProgress = null;
            statusObj.srcBucketInProgress = null;
            cb(err);
        }
    );
}

module.exports = {
    compareBuckets,
};
