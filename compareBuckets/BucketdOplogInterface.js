/*
 * Main interface for bucketd oplog management
 *
 * persist is an interface with the following methods:
 * - constructor(params)
 * - load(bucketName, persistData, cb(err, offset))
 * - save(bucketName, persistData, offset, cb(err))

 * persistData is an interface with the following methods:
 * - constuctor(params)
 * - initState(cb(err)): initialize the structure, e.g. initial bucket scan
 * - loadState(stream, cb(err)): load the state
 * - saveState(stream, cb(err)): save the state
 */
const async = require('async');
const BucketClient = require('bucketclient').RESTClient;
const { jsutil } = require('arsenal');
const LogConsumer = require('arsenal').storage.metadata.bucketclient.LogConsumer;
const { isMasterKey } = require('arsenal/lib/versioning/Version');
const Flusher = require('./Flusher');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class BucketdOplogInterface {

    constructor(params) {
        let bkBootstrap = ['localhost:9000'];
        if (params && params.bootstrap) {
            bkBootstrap = params.bootstrap;
        }
        this.bkClient = new BucketClient(bkBootstrap);
        this.backendRetryTimes = 3;
        this.backendRetryInterval = 300;
        this.bucketdOplogQuerySize = 20;
        this.raftId = 2; // XXX use route
        this.logger = new werelogs.Logger('BucketdOplogInterface');
    }

    start(bucketName, persist, persistData, flusherParams, cb) {
        async.waterfall([
            next => {
                let cseq = undefined;
                // get stored offset if we have it
                // load persistData if present
                persist.load(bucketName, persistData, (err, offset) => {
                    if (err) {
                        return next(err);
                    }
                    cseq = offset;
                    return next(null, cseq, persistData);
                });
            },
            (cseq, persistData, next) => {
                if (cseq !== undefined) {
                    this.logger.info(`skipping cseq acquisition (cseq=${cseq})`,
                                     { bucketName });
                    return next(null, cseq, persistData, true);
                }
                this.logger.info('cseq acquisition',
                                 { bucketName });
                async.retry(
                    {
                        times: this.backendRetryTimes,
                        interval: this.backendRetryInterval,
                    },
                    done => {
                        this.bkClient.getRaftLog(
                            this.raftId,
                            1,
                            1,
                            true,
                            null,
                            (err, stream) => {
                                if (err) {
                                    this.logger.info('retrying getRaftLog', { err, bucketName });
                                    return done(err);
                                }
                                const chunks = [];
                                stream.on('data', chunk => {
                                    chunks.push(chunk);
                                });
                                stream.on('end', () => {
                                    const info = JSON.parse(Buffer.concat(chunks));
                                    return done(null, info);
                                });
                                return undefined;
                            });
                    },
                    (err, res) => {
                        if (err) {
                            this.logger.error('getRaftLog too many failures', { err, bucketName });
                            return next(err);
                        }
                        return next(null, res.info.cseq, persistData, false);
                    });
                return undefined;
            },
            (cseq, persistData, skipListing, next) => {
                if (skipListing) {
                    this.logger.info(`skipping listing cseq=${cseq}`,
                                     { bucketName });
                    return next(null, cseq, persistData);
                }
                this.logger.info(`listing cseq=${cseq}`,
                                 { bucketName });
                persistData.initState(err => {
                    if (err) {
                        return next(err);
                    }
                    persist.save(
                        bucketName, persistData, cseq, err => {
                            if (err) {
                                return next(err);
                            }
                            return next(null, cseq, persistData);
                        });
                    return undefined;
                });
                return undefined;
            },
            (cseq, persistData, next) => {
                this.logger.info(`reading oplog raftId=${this.raftId} cseq=${cseq}`,
                                 { bucketName });
                // only way to get out of the loop in all cases
                const nextOnce = jsutil.once(next);
                const doStop = false;
                // resume reading the oplog from cseq. changes are idempotent
                // setup oplog manager
                const flusher = new Flusher(bucketName, persist, persistData, flusherParams);
                flusher.events.on('stop', () => nextOnce());
                flusher.startFlusher();
                const logConsumer = new LogConsumer({
                    bucketClient: this.bkClient,
                    raftSession: this.raftId,
                });
                let _cseq = cseq;
                async.until(
                    () => doStop,
                    _next => {
                        // console.error(
                        // 'readRecords', _cseq, this.bucketdOplogQuerySize);
                        logConsumer.readRecords({
                            startSeq: _cseq,
                            limit: this.bucketdOplogQuerySize,
                        }, (err, record) => {
                            if (err) {
                                this.logger.error('readRecords error', { err, bucketName });
                                // return _next(err);
                                return setTimeout(() => _next(), 5000);
                            }
                            // console.error('record info', record.info);
                            if (!record.log) {
                                // nothing to read
                                return setTimeout(() => _next(), 5000);
                            }
                            const seqs = [];
                            record.log.on('data', chunk => {
                                seqs.push(chunk);
                            });
                            record.log.on('end', () => {
                                for (let i = 0; i < seqs.length; i++) {
                                    if (seqs[i].db === bucketName) {
                                        for (let j = 0; j < seqs[i].entries.length; j++) {
                                            // console.info(i, j, seqs[i].db, seqs[i].entries[j]);
                                            let opType;
                                            const _item = {};
                                            _item.key = seqs[i].entries[j].key;
                                            if (seqs[i].entries[j].type !== undefined &&
                                                seqs[i].entries[j].type === 'del') {
                                                opType = 'del';
                                                if (!isMasterKey(_item.key)) {
                                                    // ignore for now
                                                    return;
                                                }
                                            } else {
                                                opType = 'put';
                                                const _value = seqs[i].entries[j].value;
                                                _item.value = {};
                                                _item.value.versionId = _value.versionId;
                                                _item.value.size = _value['content-length'];
                                                _item.value.md5 = _value['content-md5'];
                                            }
                                            flusher.addEvent(
                                                _item,
                                                _cseq + i + 1,
                                                opType);
                                        }
                                    }
                                }
                                flusher.flushQueue(err => {
                                    if (err) {
                                        return _next(err);
                                    }
                                    _cseq += seqs.length;
                                    return _next();
                                });
                            });
                            return undefined;
                        });
                    }, err => {
                        if (err) {
                            return nextOnce(err);
                        }
                        return nextOnce();
                    });
            },
        ], err => {
            if (err) {
                return cb(err);
            }
            this.logger.info('returning',
                             { bucketName });
            return cb();
        });
    }
}

module.exports = BucketdOplogInterface;
