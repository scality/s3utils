/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const async = require('async');

const { listBucketMasterKeys } = require('./utils');

function getReportObject(bucket, entry, verbose) {
    const md = JSON.parse(entry.value);
    const obj = {
        bucket,
        objectKey: entry.key,
    };

    if (verbose) {
        obj.lastModified = md['last-modified'];
        obj.status = md.replicationInfo ? md.replicationInfo.status : '';
    }

    return obj;
}

function compareObjectsReport(srcBucket, src, dstBucket, dst, options) {
    const srcMD = JSON.parse(src.value);
    const dstMD = JSON.parse(dst.value);

    const report = {
        sourceObject: {
            bucket: srcBucket,
            key: src.key,
            versionId: srcMD.versionId,
            size: srcMD['content-length'],
            contentMD5: srcMD['content-md5'],
        },
        destinationObject: {
            bucket: dstBucket,
            key: dst.key,
            versionId: dstMD.versionId,
            size: dstMD['content-length'],
            contentMD5: dstMD['content-md5'],
        },
    };

    if (options.compareVersionId) {
        const srcVersionID = srcMD.versionId;
        const dstVersionID = dstMD.versionId;

        if (srcVersionID !== dstVersionID) {
            report.error = 'destination object version-id does not match source object';
        }
        return report;
    }

    if (options.compareObjectSize) {
        const srcSize = Number.parseInt(srcMD['content-length'], 10);
        const dstSize = Number.parseInt(dstMD['content-length'], 10);

        if (srcSize !== dstSize) {
            report.error = 'destination object size does not match source object';
        }

        return report;
    }

    return null;
}

function compareBuckets(params, log, cb) {
    const {
        bucketdSrcParams,
        bucketdDstParams,
        statusObj,
        verbose,
        compareVersionId,
        compareObjectSize,
    } = params;

    statusObj.srcBucketInProgress = bucketdSrcParams.bucket;
    statusObj.dstBucketInProgress = bucketdDstParams.bucket;

    let srcDone = false;
    let dstDone = false;
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
                            bucketdSrcParams.marker = marker;
                            statusObj.srcKeyMarker = marker;
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
                            bucketdDstParams.marker = marker;
                            statusObj.dstKeyMarker = marker;
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
                        log.info('missing object in source',
                            getReportObject(
                                bucketdSrcParams.bucket,
                                dstContents[dstIdx],
                                verbose
                            ));
                        ++statusObj.dstProcessedCount;
                        ++statusObj.missingInSrcCount;
                        ++dstIdx;
                    }
                }

                if (dstDone && dstContents.length === 0) {
                    while (srcIdx < srcContents.length) {
                        log.info('missing object in destination',
                            getReportObject(
                                bucketdDstParams.bucket,
                                srcContents[srcIdx],
                                verbose
                            ));
                        ++statusObj.srcProcessedCount;
                        ++statusObj.missingInDstCount;
                        ++srcIdx;
                    }
                }

                while (srcIdx < srcContents.length && dstIdx < dstContents.length) {
                    if (srcContents[srcIdx].key < dstContents[dstIdx].key) {
                        log.info('missing object in destination',
                            getReportObject(
                                bucketdDstParams.bucket,
                                srcContents[srcIdx],
                                verbose
                            ));
                        ++statusObj.srcProcessedCount;
                        ++statusObj.missingInDstCount;
                        ++srcIdx;
                        continue;
                    }

                    if (srcContents[srcIdx].key > dstContents[dstIdx].key) {
                        log.info('missing object in source',
                            getReportObject(
                                bucketdSrcParams.bucket,
                                dstContents[dstIdx],
                                verbose
                            ));
                        ++statusObj.dstProcessedCount;
                        ++statusObj.missingInSrcCount;
                        ++dstIdx;
                        continue;
                    }

                    const report = compareObjectsReport(
                        bucketdSrcParams.bucket,
                        srcContents[srcIdx],
                        bucketdDstParams.bucket,
                        dstContents[dstIdx],
                        {
                            compareVersionId,
                            compareObjectSize,
                        }
                    );

                    if (report && report.error) {
                        log.info('object mismatch found', report);
                    }

                    ++statusObj.dstProcessedCount;
                    ++statusObj.srcProcessedCount;
                    ++dstIdx;
                    ++srcIdx;
                }

                srcContents = srcContents.slice(srcIdx);
                dstContents = dstContents.slice(dstIdx);
                return process.nextTick(() => done(null));
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
    compareObjectsReport,
};
