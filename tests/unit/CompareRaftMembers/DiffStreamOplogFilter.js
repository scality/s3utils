const http = require('http');
const stream = require('stream');

const { shuffle } = require('arsenal');

const DiffStreamOplogFilter = require('../../../CompareRaftMembers/DiffStreamOplogFilter');

const HTTP_TEST_PORT = 9090;

class MockRaftOplogStream extends stream.Readable {
    constructor(entriesToEmit, refreshPeriodMs) {
        super({ objectMode: true });
        this.entriesToEmit = entriesToEmit;
        this.refreshPeriodMs = refreshPeriodMs;
    }

    _read() {
        if (this.entriesToEmit.length > 0) {
            // introduce a little delay between events to make sure
            // the filter waits for oplog events before emitting
            // output entries
            setTimeout(() => {
                this.push(this.entriesToEmit.shift());
                if (this.entriesToEmit.length === 0) {
                    this.push({ entry: null });
                }
            }, 10);
        } else {
            setTimeout(() => {
                this.push({ entry: null });
            }, this.refreshPeriodMs);
        }
    }
}

describe('DiffStreamOplogFilter', () => {
    let httpServer;
    let reqCount = 0;
    beforeAll(done => {
        const handleGetBucketRSRequest = (res, bucketName) => {
            // simple mock matching bucket names "foo-on-rsX" to raft
            // session X as long as 1 <= X <= 5
            const bucketMatch = /-on-rs([1-5])$/.exec(bucketName);
            if (bucketMatch) {
                const rsIdStr = bucketMatch[1];
                res.writeHead(200, {
                    'Content-Length': 1,
                });
                return res.end(rsIdStr);
            }
            if (bucketName === 'bucket-with-error') {
                res.writeHead(500);
            } else {
                res.writeHead(404);
            }
            return res.end();
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
            const bucketRsMatch = /^\/_\/buckets\/([a-z0-9-]+)\/id$/.exec(url.pathname);
            if (bucketRsMatch) {
                const bucketName = bucketRsMatch[1];
                return handleGetBucketRSRequest(res, bucketName);
            }
            throw new Error(`unexpected request path ${url.pathname}`);
        });
        httpServer.listen(HTTP_TEST_PORT);
        httpServer.once('listening', done);
    });
    afterAll(done => {
        httpServer.close(done);
    });

    test('filtering test with mocks', done => {
        const oplogFilter = new DiffStreamOplogFilter({
            bucketdHost: 'localhost',
            bucketdPort: HTTP_TEST_PORT,
            maxBufferedEntries: 5,
            excludeFromCseqs: {
                1: 10,
                2: 20,
                3: 30,
                4: 40,
            },
            retryDelayMs: 10,
            maxRetryDelayMs: 100,
        });
        // Prepare some test diff entries and oplog entries with a
        // nested loop over raft sessions, buckets and keys
        //
        // Note: we loop over raft sessions 0 to 5 but only 1-4 are monitored:
        //
        // - use the first loop iteration (phony raft session 0) to
        // - create buckets with no raft session
        //
        // - use raft session 5 to create buckets attached to a raft
        //   session not monitored by the filter
        //
        // In both cases, all entries belonging to those buckets
        // should be filtered hence not appear in the output. This is
        // consistent with cases where buckets would have been deleted
        // during the scan, and possibly recreated to another raft
        // session: in such case, all bucket keys were necessarily
        // updated, so should be ignored.
        const inputDiffEntries = [];
        const latestOplogs = [];
        for (let rs = 0; rs <= 5; ++rs) {
            const latestRsOplog = [];
            latestOplogs[rs] = latestRsOplog;
            for (let b = 1; b <= 5; ++b) {
                for (let k = 1; k <= 5; ++k) {
                    const bucketName = `bucket${b}-on-rs${rs}`;
                    const key = `key${k}`;
                    // insert a diff entry that should pass through if on RS 1-4
                    inputDiffEntries.push([{
                        key: `${bucketName}/${key}`,
                        value: 'foobar',
                    }, null]);
                    // insert a diff entry that should be filtered out
                    // because the key appears somewhere in the latest
                    // oplog
                    inputDiffEntries.push([{
                        key: `${bucketName}/${key}-updated`,
                        value: 'oldfoobar',
                    }, null]);
                    // insert the updated key's oplog entry
                    latestRsOplog.push({
                        entry: {
                            method: 'BATCH',
                            bucket: bucketName,
                            key: `${key}-updated`,
                            value: 'newfoobar',
                        },
                    });
                }
            }
        }
        // shuffle both the input diff entries and each oplog to
        // increase fuzziness in the test, and replace actual oplog
        // streams by mocks
        shuffle(inputDiffEntries);
        for (let rs = 1; rs <= 4; ++rs) {
            shuffle(latestOplogs[rs]);
            oplogFilter.raftSessionStates[rs].oplogStream.destroy();
            const mockOplogStream = new MockRaftOplogStream(latestOplogs[rs], 100);
            oplogFilter.raftSessionStates[rs].oplogStream = mockOplogStream;
            oplogFilter._setupOplogStream(oplogFilter.raftSessionStates[rs], mockOplogStream);
        }
        // ingest all diff entries in the shuffled order in the filter
        // stream
        for (const diffEntry of inputDiffEntries) {
            oplogFilter.write(diffEntry);
        }
        oplogFilter.end();

        // listen for output diff events and store them in a set for
        // checking that they correspond to what should have been sent
        const filteredDiffEntries = new Set();
        oplogFilter
            .on('data', data => {
                // stringify in JSON to support quick lookup from the set

                // note: diff entries are forwarded as is, the
                // javascript objects embedded have their fields in
                // the same order, which is retained in the
                // stringified version
                filteredDiffEntries.add(JSON.stringify(data));
            })
            .on('end', () => {
                // check that all diff entries expected to be output
                // have been output, i.e. only non-updated diff
                // entries from RS 1 to 4
                for (let rs = 1; rs <= 4; ++rs) {
                    for (let b = 1; b <= 5; ++b) {
                        for (let k = 1; k <= 5; ++k) {
                            const bucketName = `bucket${b}-on-rs${rs}`;
                            const key = `key${k}`;
                            const outputDiffEntry = [{
                                key: `${bucketName}/${key}`,
                                value: 'foobar',
                            }, null];
                            const jsonOutputDiffEntry = JSON.stringify(outputDiffEntry);
                            expect(filteredDiffEntries.has(jsonOutputDiffEntry)).toBeTruthy();
                            filteredDiffEntries.delete(jsonOutputDiffEntry);
                        }
                    }
                }
                // check that no other entry than what was expected has been output
                expect(filteredDiffEntries.size).toEqual(0);
                done();
            })
            .on('error', err => {
                fail(`an error occurred during filtering: ${err}`);
            });
    });

    test('should handle an empty input stream', done => {
        const oplogFilter = new DiffStreamOplogFilter({
            bucketdHost: 'localhost',
            bucketdPort: HTTP_TEST_PORT,
            maxBufferedEntries: 5,
            excludeFromCseqs: {},
        });
        oplogFilter.end();
        oplogFilter
            .on('data', event => {
                fail('should not have received a "data" event', event);
            })
            .on('end', () => {
                done();
            })
            .on('error', err => {
                fail(`an error occurred during filtering: ${err}`);
            });
    });

    test('should emit a stream error if failing to fetch RSID after retries', done => {
        const oplogFilter = new DiffStreamOplogFilter({
            bucketdHost: 'localhost',
            bucketdPort: HTTP_TEST_PORT,
            maxBufferedEntries: 5,
            excludeFromCseqs: {
                1: 10,
            },
            retryDelayMs: 10,
            maxRetryDelayMs: 100,
        });
        oplogFilter.raftSessionStates[1].oplogStream.destroy();
        const mockOplogStream = new MockRaftOplogStream([], 100);
        oplogFilter.raftSessionStates[1].oplogStream = mockOplogStream;
        oplogFilter._setupOplogStream(oplogFilter.raftSessionStates[1], mockOplogStream);
        // this bucket name triggers a 500 error in the HTTP server
        // mock when requested for its raft session ID
        oplogFilter.write([{ key: 'bucket-with-error/key', value: 'foobar' }, null]);
        oplogFilter.end();
        oplogFilter
            .on('data', event => {
                fail('should not have received a "data" event', event);
            })
            .on('error', err => {
                expect(err).toBeTruthy();
                done();
            });
    });
});
