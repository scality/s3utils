const http = require('http');

const RaftOplogStream = require('../../../CompareRaftMembers/RaftOplogStream');

const TEST_OPLOG_NB_RECORDS = 123;
const DUMMY_VERSION_ID = '987654321 FOOBAR42.42';

function buildTestOplog() {
    const oplog = [];
    for (let r = 0; r < TEST_OPLOG_NB_RECORDS; ++r) {
        const record = {
            db: `bucket-${r}`,
            method: 8, // BATCH
            entries: [
                {
                    key: `key-${r}`,
                    value: `value-${r}`,
                },
                {
                    key: `key-${r}\u0000${DUMMY_VERSION_ID}`,
                    value: `value-${r}`,
                },
                {
                    key: `key-${r}`,
                    type: 'del',
                },
                {
                    key: `key-${r}\u0000${DUMMY_VERSION_ID}`,
                    type: 'del',
                },
            ],
        };
        oplog.push(record);
    }
    return oplog;
}

const TEST_OPLOG = buildTestOplog();

describe('RaftOplogStream', () => {
    let httpServer;
    let reqCount = 0;
    beforeAll(done => {
        const handleGetOplogRequest = (req, res, url) => {
            const qsBegin = url.searchParams.get('begin');
            const qsLimit = url.searchParams.get('limit');
            const begin = qsBegin !== undefined
                ? Number.parseInt(qsBegin, 10) : 1;
            const limit = qsLimit !== undefined
                ? Number.parseInt(qsLimit, 10) : 10000;
            const oplogToSend = TEST_OPLOG.slice(begin - 1, begin - 1 + limit);
            if (begin < 1 || oplogToSend.length === 0) {
                res.writeHead(416);
                return res.end();
            }
            const responseBody = JSON.stringify({
                info: {
                    start: begin,
                    cseq: TEST_OPLOG.length,
                    prune: 1,
                },
                log: oplogToSend,
            });
            res.writeHead(200, {
                'Content-Type': 'application/json',
                'Content-Length': responseBody.length,
            });
            return res.end(responseBody);
        };
        httpServer = http.createServer((req, res) => {
            req.resume();
            reqCount += 1;
            // fail 1/3 requests to check retry behavior
            if (reqCount % 3 === 0) {
                res.writeHead(500);
                return res.end('OOPS');
            }
            const url = new URL(req.url, `http://${req.headers.host}`);
            if (url.pathname === '/_/raft_sessions/1/log') {
                return handleGetOplogRequest(req, res, url);
            }
            throw new Error(`unexpected request path ${url.pathname}`);
        });
        httpServer.listen(0);
        httpServer.on('listening', done);
    });
    afterAll(done => {
        httpServer.close(done);
    });

    [
        {
            desc: 'complete oplog',
            expectedRange: [0, TEST_OPLOG.length],
        },
        {
            desc: 'last log record',
            startSeq: TEST_OPLOG.length,
            expectedRange: [TEST_OPLOG.length - 1, TEST_OPLOG.length],
        },
        {
            desc: 'already on log tail',
            startSeq: TEST_OPLOG.length + 1,
            expectedRange: [0, 0],
        },
    ].forEach(testCase => {
        test(testCase.desc, done => {
            const oplogStream = new RaftOplogStream({
                bucketdHost: 'localhost',
                bucketdPort: httpServer.address().port,
                raftSessionId: 1,
                startSeq: testCase.startSeq,
                refreshPeriodMs: 100,
                maxRecordsPerRequest: 10,
                retryDelayMs: 50,
                maxRetryDelayMs: 1000,
            });
            const [rangeStart, rangeEnd] = testCase.expectedRange;
            const expectedOplogEntries = [];
            for (let i = rangeStart; i < rangeEnd; ++i) {
                const record = TEST_OPLOG[i];
                for (const entry of record.entries) {
                    expectedOplogEntries.push({
                        method: 'BATCH',
                        bucket: record.db,
                        ...entry,
                    });
                }
            }
            // first null should come immediately on reaching the log tail
            expectedOplogEntries.push(null);
            let nextIndex = 0;
            let expectSecondNullAfter = null;
            oplogStream
                .on('data', event => {
                    const { entry } = event;
                    if (expectSecondNullAfter) {
                        expect(entry).toEqual(null);
                        expect(Date.now()).toBeGreaterThan(expectSecondNullAfter);
                        oplogStream.destroy();
                        return done();
                    }
                    expect(entry).toEqual(expectedOplogEntries[nextIndex]);
                    nextIndex += 1;
                    if (nextIndex === expectedOplogEntries.length) {
                        // second null should come after the refresh
                        // period: check it to be coming at least a
                        // little less than the refresh period
                        expectSecondNullAfter = Date.now() + 90;
                    }
                    return undefined;
                })
                .on('end', () => {
                    fail('oplog stream should not emit "end" event');
                })
                .on('error', err => {
                    fail(`an error occurred: ${err}`);
                });
        });
    });
    test('stream should emit error after all retries fail', done => {
        const oplogStream = new RaftOplogStream({
            bucketdHost: 'localhost',
            // change port to get connection errors
            bucketdPort: httpServer.address().port + 1,
            raftSessionId: 1,
            startSeq: 42,
            retryDelayMs: 10,
            maxRetryDelayMs: 100,
        });
        oplogStream
            .on('data', event => {
                fail('should not have received a "data" event', event);
            })
            .on('end', () => {
                fail('oplog stream should not emit "end" event');
            })
            .on('error', err => {
                expect(err).toBeTruthy();
                expect(err.code).toEqual('ECONNREFUSED');
                done();
            });
    });
});
