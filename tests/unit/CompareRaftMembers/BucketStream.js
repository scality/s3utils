const http = require('http');

const { versioning } = require('arsenal');

const BucketStream = require('../../../CompareRaftMembers/BucketStream');

const HTTP_TEST_PORT = 9090;
const TEST_BUCKET_FILTERED_CONTENTS_LENGTH = 3210;
const DUMMY_VERSION_ID = '987654321 FOOBAR42.42';
const VERSIONED_RANGE = [1210, 1310];
const VERSIONING_SUSPENDED_RANGE = [1410, 1510];
const PHD_RANGE = [1230, 1240];

function buildItemFromIndex(index, flags) {
    const paddedIndex = `000000${index}`.slice(-6);
    const key = flags.versionKey
        ? `key-${paddedIndex}\u0000${DUMMY_VERSION_ID}`
        : `key-${paddedIndex}`;
    let value;
    if (flags.isPHD) {
        value = '{"isPHD":true}';
    } else if (flags.versionKey || flags.hasVersionId) {
        value = `{"versionId":"${DUMMY_VERSION_ID}","value":"value-${paddedIndex}"}`;
    } else {
        value = `{"value":"value-${paddedIndex}"}`;
    }
    return { key, value };
}

function buildTestBucketContents() {
    const contents = [];
    for (let i = 0; i < TEST_BUCKET_FILTERED_CONTENTS_LENGTH; ++i) {
        // introduce mainly master keys without a version key, with a
        // few random variations at random locations to check
        // filtering:
        // - versioned keys with both a master and a version key
        // - PHD key

        // introduce master/nonversioned key
        if (i >= PHD_RANGE[0] && i < PHD_RANGE[1]) {
            contents.push(buildItemFromIndex(i, { isPHD: true }));
        } else if ((i >= VERSIONED_RANGE[0] && i < VERSIONED_RANGE[1])
                   || (i >= VERSIONING_SUSPENDED_RANGE[0] && i < VERSIONING_SUSPENDED_RANGE[1])) {
            contents.push(buildItemFromIndex(i, { hasVersionId: true }));
        } else {
            contents.push(buildItemFromIndex(i, {}));
        }

        if (i >= VERSIONED_RANGE[0] && i < VERSIONED_RANGE[1]) {
            contents.push(buildItemFromIndex(i, { versionKey: true }));
        }
    }
    // introduce a replay key to check those are filtered out
    contents.push({
        key: `${versioning.VersioningConstants.DbPrefixes.Replay}foobar`,
        value: '{}',
    });
    return contents;
}

const TEST_BUCKET_CONTENTS = buildTestBucketContents();

describe('BucketStream', () => {
    let httpServer;
    let reqCount = 0;
    beforeAll(() => {
        httpServer = http.createServer((req, res) => {
            req.resume();
            reqCount += 1;
            // fail each other request
            if (reqCount % 2 === 0) {
                res.writeHead(500);
                return res.end('OOPS');
            }
            const url = new URL(req.url, `http://${req.headers.host}`);
            const marker = url.searchParams.get('marker');
            let contents;
            let isTruncated;
            const startIndex = TEST_BUCKET_CONTENTS.findIndex(
                item => (!marker || item.key > marker),
            );
            if (startIndex === -1) {
                contents = [];
                isTruncated = false;
            } else {
                // limit to 1000 entries returned per page, like bucketd does
                const endIndex = startIndex + 1000;
                contents = TEST_BUCKET_CONTENTS.slice(startIndex, endIndex);
                isTruncated = (endIndex < TEST_BUCKET_CONTENTS.length);
            }
            const responseBody = JSON.stringify({
                Contents: contents,
                IsTruncated: isTruncated,
            });
            res.writeHead(200, {
                'Content-Type': 'application/json',
                'Content-Length': responseBody.length,
            });
            return res.end(responseBody);
        });
        httpServer.listen(HTTP_TEST_PORT);
    });
    afterAll(done => {
        httpServer.close(done);
    });

    [
        {
            desc: 'complete listing',
            marker: null,
            lastKey: null,
            expectedRange: [0, TEST_BUCKET_FILTERED_CONTENTS_LENGTH - 1],
        },
        {
            desc: 'first 100 entries',
            marker: null,
            lastKey: 'key-000099',
            expectedRange: [0, 99],
        },
        {
            desc: '100 entries in an intermediate page',
            marker: 'key-002030',
            lastKey: 'key-002129',
            expectedRange: [2031, 2129],
        },
        {
            desc: '100 entries in an intermediate page starting at a versioned key',
            marker: 'key-001211',
            lastKey: 'key-001310',
            expectedRange: [1212, 1310],
        },
        {
            desc: '1500 entries across two intermediate pages',
            marker: 'key-001030',
            lastKey: 'key-002529',
            expectedRange: [1031, 2529],
        },
        {
            desc: '1180 entries across two end pages with end marker gt listing end',
            marker: 'key-002030',
            lastKey: 'key-003530',
            expectedRange: [2031, TEST_BUCKET_FILTERED_CONTENTS_LENGTH - 1],
        },
    ].forEach(testCase => {
        test(testCase.desc, done => {
            const bucketStream = new BucketStream({
                bucketdHost: 'localhost',
                bucketdPort: HTTP_TEST_PORT,
                bucketName: 'test-bucket',
                marker: testCase.marker,
                lastKey: testCase.lastKey,
                retryDelayMs: 50,
                maxRetryDelayMs: 1000,
            });
            let nextIndex = testCase.expectedRange[0];
            bucketStream
                .on('data', item => {
                    expect(nextIndex).toBeLessThanOrEqual(testCase.expectedRange[1]);
                    // build the expected listing item from the test
                    // parameters specific to this index
                    const flags = {};
                    if ((nextIndex >= VERSIONED_RANGE[0] && nextIndex < VERSIONED_RANGE[1])
                        || (nextIndex >= VERSIONING_SUSPENDED_RANGE[0]
                            && nextIndex < VERSIONING_SUSPENDED_RANGE[1])) {
                        flags.hasVersionId = true;
                    }
                    // in case where there is a PHD key, we should get the next versioned key
                    if (nextIndex >= PHD_RANGE[0] && nextIndex < PHD_RANGE[1]) {
                        flags.versionKey = true;
                    }
                    const listingItem = buildItemFromIndex(nextIndex, flags);
                    const expectedItem = {
                        key: `test-bucket/${listingItem.key}`,
                        value: listingItem.value,
                    };
                    expect(item).toEqual(expectedItem);
                    nextIndex += 1;
                })
                .on('end', () => {
                    expect(nextIndex).toEqual(testCase.expectedRange[1] + 1);
                    done();
                })
                .on('error', done);
        });
    });
});
