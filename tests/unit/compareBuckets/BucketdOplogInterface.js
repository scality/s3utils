const async = require('async');
const bucketInfo = require('./BucketInfo.json');
const BucketdOplogInterface = require('../../../compareBuckets/BucketdOplogInterface');
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;
const PersistMemInterface = require('../../../compareBuckets/PersistMemInterface');
const Injector = require('./Injector');
const http = require('http');
const url = require('url');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class PersistDataInterface {

    constructor() {
        this.data = null;
    }

    initState(cb) {
        this.data = {};
        return process.nextTick(cb);
    }

    loadState(stream, cb) {
        const chunks = [];
        stream.on('data', chunk => {
            chunks.push(chunk);
        });
        stream.on('end', () => {
            this.data = JSON.parse(Buffer.concat(chunks));
            return process.nextTick(cb);
        });
    }

    saveState(stream, cb) {
        stream.write(JSON.stringify(this.data));
        stream.end();
        return process.nextTick(cb);
    }

    updateState(addQueue, deleteQueue, cb) {
        // console.log('addQueue', addQueue);
        // console.log('deleteQueue', deleteQueue);
        return process.nextTick(cb);
    }
}

describe('BucketdOplogInterface', () => {
    const logger = new werelogs.Logger('BucketOplogInterface');

    const fakePort = 9090;
    const fakeBucket = 'fake';
    const fakeRaftId = 2;
    const numObjs = 20000;
    const fakeCseq = 20001;
    let oplogInjected = false;
    const numOplogSeqs = 100;
    const oplogBatchSize = 2;
    const endCseq = fakeCseq + numOplogSeqs;
    const maxLimit = 2;
    const oplogKeys = [];
    const oplogValues = [];
    let oplogKeysIdx = 0;

    const params = {
        bootstrap: [`localhost:${fakePort}`],
    };

    const bucketdOplog = new BucketdOplogInterface(params);
    const memBackend = new MetadataWrapper(
        'mem', {}, null, logger);
    const injector = new Injector(memBackend);
    const persist = new PersistMemInterface();

    const requestListener = (req, res) => {
        const _url = url.parse(req.url, true);
        // console.error(_url.pathname, _url.query);
        if (_url.pathname === `/_/raft_sessions/${fakeRaftId}/log`) {
            const begin = _url.query.begin;
            const limit = _url.query.limit;
            if (begin === '1' && limit === '1') {
                res.writeHead(200);
                res.end(JSON.stringify(
                    {
                        info: {
                            start: 1,
                            cseq: fakeCseq,
                            prune: 1,
                        },
                    }));
            } else {
                // console.error(begin, limit);
                const realLimit = Math.min(limit, maxLimit);
                async.until(
                    () => oplogInjected,
                    next => {
                        // inject similar but different random objects
                        // console.error('injecting...');
                        injector.inject(
                            fakeBucket,
                            numOplogSeqs * oplogBatchSize,
                            numObjs, 1,
                            true, 'obj_', '_bis',
                            oplogKeys, oplogValues,
                            err => {
                                if (err) {
                                    return next(err);
                                }
                                oplogInjected = true;
                                return next();
                            });
                    }, err => {
                        if (err) {
                            res.writeHead(404);
                            res.end('error', err);
                            return undefined;
                        }
                        if (begin < endCseq) {
                            res.writeHead(200);
                            const resp = {};
                            resp.info = {
                                start: begin,
                                cseq: endCseq,
                                prune: 1,
                            };
                            // console.error('replying', begin, endCseq, realLimit, oplogBatchSize, oplogKeysIdx);
                            resp.log = [];
                            for (let i = 0; i < realLimit; i++) {
                                resp.log[i] = {};
                                resp.log[i].db = fakeBucket;
                                resp.log[i].method = 8;
                                resp.log[i].entries = [];
                                for (let j = 0; j < oplogBatchSize; j++) {
                                    resp.log[i].entries[j] = {};
                                    resp.log[i].entries[j].key = oplogKeys[oplogKeysIdx];
                                    resp.log[i].entries[j].value = oplogValues[oplogKeysIdx];
                                    oplogKeysIdx++;
                                }
                            }
                            res.end(JSON.stringify(resp));
                        } else {
                            // console.error('end...');
                        }
                        return undefined;
                    });
            }
        } else if (_url.pathname === `/default/bucket/${fakeBucket}`) {
            const marker = _url.query.marker === '' ? null : _url.query.marker;
            const maxKeys = parseInt(_url.query.maxKeys, 10);
            // console.error('listing', marker, maxKeys);
            memBackend.listObjects(fakeBucket, {
                listingType: 'Delimiter',
                marker,
                maxKeys,
            }, (err, result) => {
                if (err) {
                    res.writeHead(404);
                    res.end('error', err);
                    return undefined;
                }
                res.writeHead(200);
                res.end(JSON.stringify(result));
                return undefined;
            });
        }
    };

    beforeAll(done => {
        const server = http.createServer(requestListener);
        server.listen(fakePort);
        async.waterfall([
            next => memBackend.createBucket(fakeBucket, bucketInfo, logger, next),
            next => injector.inject(
                fakeBucket, numObjs, numObjs, 1, false, 'obj_', '', null, null, next),
        ], done);
    });

    afterAll(done => {
        memBackend.deleteBucket(fakeBucket, logger, done);
    });

    it('simulation', done => {
        const oplogMgrParams = {
            stopAt: numObjs + numOplogSeqs,
            interactive: true,
        };
        const persistData = new PersistDataInterface();
        bucketdOplog.start(
            fakeBucket,
            persist,
            persistData,
            oplogMgrParams,
            done);
    });
});
