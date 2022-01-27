/* eslint-disable max-len */
/* eslint-disable no-console */
/* eslint-disable comma-dangle */

const assert = require('assert');
const async = require('async');
const SortedSet = require('./SortedSet');

const { listBucketMasterKeys } = require('./utils');

// const BucketdOplogInterface = require('./BucketdOplogInterface');
const MongoOplogInterface = require('./MongoOplogInterface');
const PersistFileInterface = require('./PersistFileInterface');
const PersistMemInterface = require('./PersistMemInterface');

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

function compareObjectsReport(diffMgr, srcBucket, src, dstBucket, dst, options) {
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

function compareBuckets(diffMgr, params, log, cb) {
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
                        diffMgr.srcAdd(dstContents[dstIdx], diffMgr.MismatchNoExist);
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
                        diffMgr.dstAdd(srcContents[srcIdx], diffMgr.MismatchNoExist);
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
                        diffMgr.dstAdd(srcContents[srcIdx], diffMgr.MismatchNoExist);
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
                        diffMgr.srcAdd(dstContents[dstIdx], diffMgr.MismatchNoExist);
                        ++statusObj.dstProcessedCount;
                        ++statusObj.missingInSrcCount;
                        ++dstIdx;
                        continue;
                    }

                    const report = compareObjectsReport(
                        diffMgr,
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

function updateMissingCount(diffMgr, source, incr) {
    if (source === diffMgr.IdxSrc) {
        // eslint-disable-next-line
        diffMgr.params.statusObj.missingInSrcCount += incr;
    } else {
        // eslint-disable-next-line
        diffMgr.params.statusObj.missingInDstCount += incr;
    }
}

function genericUpdate(diffMgr, source, target, addQueue, delQueue) {
    // console.log('genericUpdate', source, target);
    assert(source === diffMgr.IdxSrc ||
           source === diffMgr.IdxDst);
    assert(target === diffMgr.IdxSrc ||
           target === diffMgr.IdxDst);
    addQueue.forEach(item => {
        // console.log('update add', item);
        const value = diffMgr.state[source].get(item.key);
        if (value !== undefined) {
            // console.log('found', item.key, value, item.value);
            assert(value.mismatch === diffMgr.MismatchNoExist);
            diffMgr.state[source].del(item.key);
            updateMissingCount(diffMgr, source, -1);
        } else {
            // console.log('not found', item.key, item.value);
            const _value = {};
            Object.assign(_value, item.value);
            _value.mismatch = diffMgr.MismatchNoExist;
            diffMgr.state[target].set(item.key, _value);
            updateMissingCount(diffMgr, target, 1);
        }
    });
    delQueue.forEach(item => {
        // console.log('update del', item);
        const value = diffMgr.state[target].get(item.key);
        if (value !== undefined) {
            // console.log('found', item.key, value);
            assert(value.mismatch === diffMgr.MismatchNoExist);
            diffMgr.state[target].del(item.key);
            updateMissingCount(diffMgr, target, -1);
        } else {
            // console.log('not found', item.key);
            const _value = {};
            Object.assign(_value, item.value);
            _value.mismatch = diffMgr.MismatchNoExist;
            diffMgr.state[source].set(item.key, _value);
            updateMissingCount(diffMgr, source, 1);
        }
    });
}

class PersistDataTargetInterface {

    constructor(diffMgr) {
        this.diffMgr = diffMgr;
    }

    initState(cb) {
        return process.nextTick(cb);
    }

    updateState(addQueue, delQueue, cb) {
        this.diffMgr.params.statusObj.dstOplogProcessedPutCount += addQueue.length;
        this.diffMgr.params.statusObj.dstOplogProcessedDelCount += delQueue.length;
        genericUpdate(this.diffMgr,
                      this.diffMgr.IdxDst,
                      this.diffMgr.IdxSrc,
                      addQueue,
                      delQueue);
        return process.nextTick(cb);
    }

    loadState(stream, cb) {
        return process.nextTick(cb);
    }

    saveState(stream, cb) {
        return process.nextTick(cb);
    }
}

class DiffManager {

    constructor(params, log) {
        assert(params !== undefined);
        this.params = params;
        /*
          bucketdSrcParams,
          bucketdDstParams,
          statusObj,
          verbose,
          compareVersionId,
          compareObjectSize,
        */
        this.log = log;
        this.srcPersist = new PersistFileInterface();
        this.dstPersist = new PersistMemInterface();
        this.dstOffset = {};
        this.dstPersistData = new PersistDataTargetInterface(this);
        this.srcOplogInterface = new MongoOplogInterface();
        this.dstOplogInterface = new MongoOplogInterface();
        this.state = [new SortedSet(), // src
                      new SortedSet(), // dst
                      this.dstOffset,
                      this.params.statusObj];
    }

    get IdxSrc() {
        return 0;
    }

    get IdxDst() {
        return 1;
    }

    get MismatchNoExist() {
        return 0;
    }

    // not yet supported
    get MismatchContent() {
        return 1;
    }

    dumpState() {
        console.log('src', this.state[0].keys, this.state[0].values);
        console.log('dst', this.state[1].keys, this.state[1].values);
    }

    start(cb) {
        this.srcOplogInterface.start(
            this.params.bucketdSrcParams.bucket,
            this.srcPersist,
            this,
            -1,
            cb);
    }

    startTarget() {
        this.dstOplogInterface.start(
            this.params.bucketdDstParams.bucket,
            this.dstPersist,
            this.dstPersistData,
            -1,
            err => {
                if (err) {
                    this.log.error('error on target bucket', { err });
                    process.exit(1);
                }
                this.log.info('end on target bucket');
                process.exit(0);
            });
    }

    initState(cb) {
        this.compareBuckets(err => {
            this.startTarget();
            return cb(err);
        });
    }

    loadState(stream, cb) {
        const chunks = [];
        stream.on('data', chunk => {
            chunks.push(chunk);
        });
        stream.on('end', () => {
            const _state = JSON.parse(Buffer.concat(chunks));
            Object.assign(this.dstOffset, _state[2]);
            Object.assign(this.params.statusObj, _state[3]);
            this.state = [new SortedSet(_state[this.IdxSrc]),
                          new SortedSet(_state[this.IdxDst]),
                          this.dstOffset,
                          this.params.statusObj];
            this.dstPersist.setOffset(
                this.params.bucketdDstParams.bucket,
                this.dstOffset);
            this.startTarget();
            return cb();
        });
    }

    saveState(stream, cb) {
        Object.assign(
            this.dstOffset,
            this.dstPersist.getOffset(this.params.bucketdDstParams.bucket));
        stream.write(JSON.stringify(this.state));
        stream.end();
        return cb();
    }

    updateState(addQueue, delQueue, cb) {
        this.params.statusObj.srcOplogProcessedPutCount += addQueue.length;
        this.params.statusObj.srcOplogProcessedDelCount += delQueue.length;
        genericUpdate(this, this.IdxSrc, this.IdxDst, addQueue, delQueue);
        return process.nextTick(cb);
    }

    // item value is already parsed
    _add(idx, item, mismatch) {
        // console.log('add', idx, item, mismatch);
        this.state[idx].set(item.key, {
            mismatch,
            versionId: item.value.versionId,
            size: item.value.size,
            md5: item.value.md5,
        });
    }

    add(idx, item, mismatch) {
        const _value = JSON.parse(item.value);
        const _item = {
            key: item.key,
            value: {
                versionId: _value.versionId,
                size: _value['content-length'],
                md5: _value['content-md5'],
            },
        };
        return this._add(idx, _item, mismatch);
    }

    // add a missing object in src set
    srcAdd(item, mismatch) {
        return this.add(this.IdxSrc, item, mismatch);
    }

    // add a missing object in dst set
    dstAdd(item, mismatch) {
        return this.add(this.IdxDst, item, mismatch);
    }

    dump() {
        console.log('state', this.state);
    }

    compareObjectsReport(srcBucket, src, dstBucket, dst, options) {
        return compareObjectsReport(this, srcBucket, src, dstBucket, dst, options);
    }

    compareBuckets(cb) {
        return compareBuckets(this, this.params, this.log, cb);
    }

    genericUpdate(source, target, addQueue, delQueue) {
        return genericUpdate(this, source, target, addQueue, delQueue);
    }
}

module.exports = DiffManager;
