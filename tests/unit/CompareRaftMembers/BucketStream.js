const http = require('http');

const { versioning } = require('arsenal');

const BucketStream = require('../../../CompareRaftMembers/BucketStream');

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
        value = { isPHD: true };
    } else {
        // introduce:
        //
        // - a few objects with a hidden location, typically a large MPU
        // - where the listing does not show the location that is
        // - larger than a certain size (for the test we can do with a
        // - small location)
        //
        // - a few empty objects i.e. with a null location
        //
        const locationHidden = (index >= VERSIONED_RANGE[0] + 1 && index < VERSIONED_RANGE[0] + 5);
        const emptyObject = (index >= VERSIONED_RANGE[0] + 6 && index < VERSIONED_RANGE[0] + 10);
        let location;
        if (emptyObject) {
            location = null;
        } else if (locationHidden) {
            location = `location-hidden-${paddedIndex}`;
        } else {
            location = `location-${paddedIndex}`;
        }
        if (flags.versionKey || flags.hasVersionId) {
            value = { versionId: DUMMY_VERSION_ID, location };
        } else {
            value = { location };
        }
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
        value: {},
    });
    return contents;
}

function buildBucketWithReplayKeysContents() {
    const contents = [{
        key: 'foo',
        value: { location: null },
    }];
    for (let i = 0; i < 3000; ++i) {
        contents.push({
            key: `${versioning.VersioningConstants.DbPrefixes.Replay}replay-key-${`0000${i}`.slice(-4)}`,
            value: {},
        });
    }
    contents.push({
        key: 'éléphant',
        value: { location: null },
    });
    return contents;
}

const TEST_BUCKET_CONTENTS = buildTestBucketContents();
const BUCKET_WITH_REPLAY_KEYS_CONTENTS = buildBucketWithReplayKeysContents();

describe('BucketStream', () => {
    let httpServer;
    let reqCount = 0;
    beforeAll(() => {
        const handleListBucketRequest = (req, res, url) => {
            const marker = url.searchParams.get('marker');
            let rawContents;
            const bucketName = url.pathname.slice('/default/bucket/'.length);
            if (bucketName === 'test-bucket') {
                rawContents = TEST_BUCKET_CONTENTS;
            } else if (bucketName === 'bucket-with-replay-keys') {
                rawContents = BUCKET_WITH_REPLAY_KEYS_CONTENTS;
            } else {
                throw new Error(`unexpected bucket name ${bucketName}`);
            }
            let contents;
            let isTruncated;
            const startIndex = rawContents.findIndex(
                item => (!marker || item.key > marker),
            );
            if (startIndex === -1) {
                contents = [];
                isTruncated = false;
            } else {
                // limit to 1000 entries returned per page, like bucketd does
                const endIndex = startIndex + 1000;
                contents = rawContents
                    .slice(startIndex, endIndex)
                    .map(item => {
                        const { key, value } = item;
                        const listedValue = { ...value };
                        if (listedValue.location
                            && listedValue.location.startsWith('location-hidden')) {
                            delete listedValue.location;
                        }
                        return { key, value: JSON.stringify(listedValue) };
                    });
                isTruncated = (endIndex < rawContents.length);
            }
            const responseBody = Buffer.from(JSON.stringify({
                Contents: contents,
                IsTruncated: isTruncated,
            }), 'utf8');
            res.writeHead(200, {
                'Content-Type': 'application/json',
                'Content-Length': responseBody.byteLength,
            });
            return res.end(responseBody);
        };
        const handleGetObjectRequest = (req, res, url) => {
            const sepIndex = url.pathname.lastIndexOf('/');
            const objectKey = decodeURIComponent(url.pathname.slice(sepIndex + 1));
            const objectIndex = TEST_BUCKET_CONTENTS.findIndex(item => item.key === objectKey);
            if (objectIndex === -1) {
                res.writeHead(404);
                return res.end();
            }
            const objectEntry = TEST_BUCKET_CONTENTS[objectIndex];
            const responseBody = JSON.stringify(objectEntry.value);
            res.writeHead(200, {
                'Content-Type': 'application/json',
                'Content-Length': responseBody.length,
            });
            return res.end(responseBody);
        };
        httpServer = http.createServer((req, res) => {
            req.resume();
            reqCount += 1;
            // fail each other request
            if (reqCount % 2 === 0) {
                res.writeHead(500);
                return res.end('OOPS');
            }
            const url = new URL(req.url, `http://${req.headers.host}`);
            if (url.pathname.startsWith('/default/bucket/')) {
                const path = url.pathname.slice('/default/bucket/'.length);
                if (path.indexOf('/') === -1) {
                    return handleListBucketRequest(req, res, url);
                }
                return handleGetObjectRequest(req, res, url);
            }
            throw new Error(`unexpected request path ${url.pathname}`);
        });
        httpServer.listen(0);
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
                bucketdPort: httpServer.address().port,
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
                    const parsedValue = JSON.parse(item.value);
                    if (flags.hasVersionId && !flags.versionKey) {
                        expect(item.key).toEqual(`test-bucket/${listingItem.key}\0${DUMMY_VERSION_ID}`);
                    } else {
                        expect(item.key).toEqual(`test-bucket/${listingItem.key}`);
                    }
                    expect(parsedValue).toEqual(listingItem.value);
                    nextIndex += 1;
                })
                .on('end', () => {
                    expect(nextIndex).toEqual(testCase.expectedRange[1] + 1);
                    done();
                })
                .on('error', done);
        });
    });

    test('listing should continue when all keys in a page are ignored', done => {
        const bucketStream = new BucketStream({
            bucketdHost: 'localhost',
            bucketdPort: httpServer.address().port,
            bucketName: 'bucket-with-replay-keys',
            retryDelayMs: 50,
            maxRetryDelayMs: 1000,
        });
        const listedContents = [];
        bucketStream
            .on('data', item => {
                listedContents.push(item);
            })
            .on('end', () => {
                expect(listedContents).toEqual([
                    {
                        key: 'bucket-with-replay-keys/foo',
                        value: '{"location":null}',
                    },
                    {
                        key: 'bucket-with-replay-keys/éléphant',
                        value: '{"location":null}',
                    },
                ]);
                done();
            })
            .on('error', done);
    });
});
