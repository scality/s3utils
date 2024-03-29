const async = require('async');
const stream = require('stream');

const DiffStream = require('../../../CompareRaftMembers/DiffStream');

// parameterized by tests
let MOCK_BUCKET_STREAM_FULL_LISTING = null;

// populated by each test
let MOCK_BUCKET_STREAM_REQUESTS_MADE = null;

class MockBucketStream extends stream.Readable {
    constructor(params) {
        super({ objectMode: true });

        const {
            bucketdHost,
            bucketdPort,
            bucketName,
            marker,
            lastKey,
        } = params;
        expect(bucketdHost).toEqual('dummy-host');
        expect(bucketdPort).toEqual(4242);

        MOCK_BUCKET_STREAM_REQUESTS_MADE.push({ bucketName, marker, lastKey });

        this.listingToSend = MOCK_BUCKET_STREAM_FULL_LISTING.filter(item => {
            const { key, value } = item;
            const slashIndex = key.indexOf('/');
            const [itemBucketName, objectKey] = [key.slice(0, slashIndex), key.slice(slashIndex + 1)];
            if (itemBucketName !== bucketName) {
                return false;
            }
            return (!marker || objectKey > marker)
                && (!lastKey || objectKey <= lastKey);
        });
    }

    _read() {
        process.nextTick(() => {
            if (this.listingToSend.length === 0) {
                this.push(null);
            } else {
                const item = this.listingToSend.shift();
                this.push(item);
            }
        });
    }
}

class MockDigestsStream extends stream.Readable {
    constructor(digestsToStream) {
        super({ objectMode: true });
        this.digestsToStream = digestsToStream;
    }

    _read() {
        setTimeout(() => {
            if (this.digestsToStream.length > 0) {
                this.push(this.digestsToStream.shift());
            } else {
                this.push(null);
            }
        }, 5);
    }
}

class MockDigestsDB {
    constructor(storedDigests) {
        this.storedDigests = storedDigests;
    }

    createReadStream(params) {
        const { gte, lt } = params || {};
        return new MockDigestsStream(
            this.storedDigests.filter(
                digestEntry => (!gte || digestEntry.key >= gte)
                    && (!lt || digestEntry.key < lt),
            ),
        );
    }
}

describe('DiffStream', () => {
    describe('DiffStream._getDigestBlockForItem', () => {
        [
            {
                desc: 'with no digests DB',
                storedDigests: null,
                dbKeys: [
                    {
                        key: 'bucket/key1',
                        expectedDigestBlock: null,
                    },
                    {
                        key: 'bucket/key2',
                        expectedDigestBlock: null,
                    },
                ],
            },
            {
                desc: 'with an empty digests DB',
                storedDigests: [],
                dbKeys: [
                    {
                        key: 'bucket/key1',
                        expectedDigestBlock: null,
                    },
                    {
                        key: 'bucket/key2',
                        expectedDigestBlock: null,
                    },
                ],
            },
            {
                desc: 'with a digests DB containing a single block for the first DB key',
                storedDigests: [
                    { key: 'bucket/key1', value: '{"size":1,"digest":"somedigest"}' },
                ],
                dbKeys: [
                    {
                        key: 'bucket/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket/key1',
                            digest: 'somedigest',
                        },
                    },
                    {
                        key: 'bucket/key2',
                        expectedDigestBlock: null,
                    },
                ],
            },
            {
                desc: 'with a digests DB containing three blocks for three DB keys',
                storedDigests: [
                    { key: 'bucket/key1', value: '{"size":1,"digest":"firstdigest"}' },
                    { key: 'bucket/key2', value: '{"size":1,"digest":"seconddigest"}' },
                    { key: 'bucket/key3', value: '{"size":1,"digest":"thirddigest"}' },
                ],
                dbKeys: [
                    {
                        key: 'bucket/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket/key1',
                            digest: 'firstdigest',
                        },
                    },
                    {
                        key: 'bucket/key2',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket/key2',
                            digest: 'seconddigest',
                        },
                    },
                    {
                        key: 'bucket/key3',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket/key3',
                            digest: 'thirddigest',
                        },
                    },
                ],
            },
            {
                desc: 'with a digests DB containing three blocks for only two disjoint DB keys',
                storedDigests: [
                    { key: 'bucket/key1', value: '{"size":1,"digest":"firstdigest"}' },
                    { key: 'bucket/key2', value: '{"size":1,"digest":"seconddigest"}' },
                    { key: 'bucket/key3', value: '{"size":1,"digest":"thirddigest"}' },
                ],
                dbKeys: [
                    {
                        key: 'bucket/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket/key1',
                            digest: 'firstdigest',
                        },
                    },
                    {
                        key: 'bucket/key3',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket/key3',
                            digest: 'thirddigest',
                        },
                    },
                ],
            },
            {
                desc: 'with a digests DB containing two blocks, each matching two DB keys',
                storedDigests: [
                    { key: 'bucket/key1-2', value: '{"size":2,"digest":"firstdigest"}' },
                    { key: 'bucket/key2-2', value: '{"size":2,"digest":"seconddigest"}' },
                ],
                dbKeys: [
                    {
                        key: 'bucket/key1-1',
                        expectedDigestBlock: {
                            size: 2,
                            lastKey: 'bucket/key1-2',
                            digest: 'firstdigest',
                        },
                    },
                    {
                        key: 'bucket/key1-2',
                        expectedDigestBlock: {
                            size: 2,
                            lastKey: 'bucket/key1-2',
                            digest: 'firstdigest',
                        },
                    },
                    {
                        key: 'bucket/key2-1',
                        expectedDigestBlock: {
                            size: 2,
                            lastKey: 'bucket/key2-2',
                            digest: 'seconddigest',
                        },
                    },
                    {
                        key: 'bucket/key2-2',
                        expectedDigestBlock: {
                            size: 2,
                            lastKey: 'bucket/key2-2',
                            digest: 'seconddigest',
                        },
                    },
                    {
                        key: 'bucket/key3',
                        expectedDigestBlock: null,
                    },
                ],
            },
            {
                desc: 'with a digests DB containing three blocks for three DB keys of different buckets',
                storedDigests: [
                    { key: 'bucket1/key1', value: '{"size":1,"digest":"firstdigest"}' },
                    { key: 'bucket2/key1', value: '{"size":1,"digest":"seconddigest"}' },
                    { key: 'bucket3/key1', value: '{"size":1,"digest":"thirddigest"}' },
                ],
                dbKeys: [
                    {
                        key: 'bucket1/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket1/key1',
                            digest: 'firstdigest',
                        },
                    },
                    {
                        key: 'bucket2/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket2/key1',
                            digest: 'seconddigest',
                        },
                    },
                    {
                        key: 'bucket3/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket3/key1',
                            digest: 'thirddigest',
                        },
                    },
                ],
            },
            {
                desc: 'with a digests DB containing two blocks for three DB keys of different buckets',
                storedDigests: [
                    { key: 'bucket1/key1', value: '{"size":1,"digest":"firstdigest"}' },
                    { key: 'bucket3/key1', value: '{"size":1,"digest":"seconddigest"}' },
                ],
                dbKeys: [
                    {
                        key: 'bucket1/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket1/key1',
                            digest: 'firstdigest',
                        },
                    },
                    // this bucket in-between does not have any associated digest
                    {
                        key: 'bucket2/key1',
                        expectedDigestBlock: null,
                    },
                    {
                        key: 'bucket3/key1',
                        expectedDigestBlock: {
                            size: 1,
                            lastKey: 'bucket3/key1',
                            digest: 'seconddigest',
                        },
                    },
                ],
            },
            {
                desc: 'with a digests DB containing three blocks for two buckets of ~1000 keys',
                storedDigests: [
                    { key: 'bucket1/key0999', value: '{"size":1000,"digest":"digest1"}' },
                    { key: 'bucket1/key1099', value: '{"size":100,"digest":"digest2"}' },
                    { key: 'bucket2/key0999', value: '{"size":1000,"digest":"digest3"}' },
                ],
                get dbKeys() {
                    const keys = [];
                    for (let i = 0; i < 1000; ++i) {
                        keys.push({
                            key: `bucket1/key${`0000${i}`.slice(-4)}`,
                            expectedDigestBlock: {
                                size: 1000,
                                lastKey: 'bucket1/key0999',
                                digest: 'digest1',
                            },
                        });
                    }
                    for (let i = 1000; i < 1100; ++i) {
                        keys.push({
                            key: `bucket1/key${`0000${i}`.slice(-4)}`,
                            expectedDigestBlock: {
                                size: 100,
                                lastKey: 'bucket1/key1099',
                                digest: 'digest2',
                            },
                        });
                    }
                    for (let i = 0; i < 1000; ++i) {
                        keys.push({
                            key: `bucket2/key${`0000${i}`.slice(-4)}`,
                            expectedDigestBlock: {
                                size: 1000,
                                lastKey: 'bucket2/key0999',
                                digest: 'digest3',
                            },
                        });
                    }
                    keys.push({
                        key: 'bucket2/key1234',
                        expectedDigestBlock: null,
                    });
                    return keys;
                },
            },
        ].forEach(testCase => {
            test(testCase.desc, done => {
                let digestsDb;
                if (testCase.storedDigests) {
                    digestsDb = new MockDigestsDB(testCase.storedDigests);
                } else {
                    digestsDb = null;
                }
                const diffStream = new DiffStream({
                    bucketdHost: 'dummy-host',
                    bucketdPort: 4242,
                    digestsDb,
                    maxBufferSize: 2000,
                    BucketStreamClass: MockBucketStream,
                });
                async.eachSeries(testCase.dbKeys, (item, itemDone) => {
                    const itemInfo = diffStream._parseItem(item);
                    diffStream._getDigestBlockForItem(itemInfo, digestBlock => {
                        expect(digestBlock).toEqual(item.expectedDigestBlock);
                        itemDone();
                    });
                }, err => {
                    expect(err).toBeFalsy();
                    diffStream.cleanup(done);
                });
            });
        });
    });

    describe('should output differences between a stream of { key, value } items and bucketd', () => {
        beforeEach(() => {
            MOCK_BUCKET_STREAM_REQUESTS_MADE = [];
        });

        [
            {
                desc: 'with no contents in db nor in bucketd',
                dbContents: [],
                bucketdContents: [],
                expectedOutput: [],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [],
                    },
                    {
                        desc: 'with an empty digests DB',
                        storedDigests: [],
                        expectedBucketStreamRequests: [],
                    },
                ],
            },
            {
                desc: 'with a single identical entry in db and bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                expectedOutput: [],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with an empty digests DB',
                        storedDigests: [],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one entry',
                        storedDigests: [
                            {
                                key: 'bucket/key1',
                                value: '{"size":1,"digest":"c6c83082863070bc04b3fbbb034014c3"}',
                            },
                        ],
                        // since the digest matches, there should be no request made to bucketd
                        expectedBucketStreamRequests: [],
                    },
                ],
            },
            {
                desc: 'with a single entry with different key in db and bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key2', value: '{}' },
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                        null,
                    ],
                    [
                        null,
                        { key: 'bucket/key2', value: '{}' },
                    ],
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one entry',
                        storedDigests: [
                            {
                                key: 'bucket/key2',
                                value: '{"size":1,"digest":"5d2eb06ef5307070ec00700775a35d49"}',
                            },
                        ],
                        // since the digest mismatches, there should be a request made to bucketd
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with a single entry with same key but different value in db and bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{"foo":"bar"}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{"foo":"qux"}' },
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket/key1', value: '{"foo":"qux"}' },
                    ],
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one entry',
                        storedDigests: [
                            {
                                key: 'bucket/key1',
                                value: '{"size":1,"digest":"a7e0ed5621515647ac4f11d77bf21b9f"}',
                            },
                        ],
                        // since the digest mismatches, there should be a request made to bucketd
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: 'key1',
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with single entry in db and two entries in bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{}' },
                    { key: 'bucket/key2', value: '{"foo":"bar"}' },
                ],
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key2', value: '{"foo":"bar"}' },
                    ],
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one entry',
                        storedDigests: [
                            {
                                key: 'bucket/key2',
                                value: '{"size":2,"digest":"0462f41eb9f7ddfda1c1254c8af2da0c"}',
                            },
                        ],
                        // since the digest mismatches, there should be a request made to bucketd
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with an empty db and a single entry in bucketd',
                dbContents: [],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                // here the absolute difference is not empty but the
                // output is, because there is no input bucket from
                // the db, hence no request can be made to bucketd to
                // check for differences
                expectedOutput: [],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [],
                    },
                ],
            },
            {
                desc: 'with a single entry in db and an empty bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                        null,
                    ],
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with an empty digests DB',
                        storedDigests: [],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with two entries for two buckets in db and bucketd',
                dbContents: [
                    { key: 'bucket1/key1', value: '{}' },
                    { key: 'bucket2/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket1/key1', value: '{}' },
                    { key: 'bucket2/key1', value: '{}' },
                ],
                expectedOutput: [],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket1',
                                marker: null,
                                lastKey: null,
                            },
                            {
                                bucketName: 'bucket2',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one block for the first bucket',
                        storedDigests: [
                            {
                                key: 'bucket1/key1',
                                value: '{"size":1,"digest":"0c477b7abf5ce8b5037a796fc15437f3"}',
                            },
                        ],
                        // digest matches for the entire contents of
                        // bucket1, so there should only be one
                        // request for bucket2
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket2',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one block for the second bucket',
                        storedDigests: [
                            {
                                key: 'bucket2/key1',
                                value: '{"size":1,"digest":"cae7de298351bcbfb45f725e5e9a57ed"}',
                            },
                        ],
                        // digest matches for the entire contents of
                        // bucket2, so there should only be one
                        // request for bucket1
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket1',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one block for each bucket',
                        storedDigests: [
                            {
                                key: 'bucket1/key1',
                                value: '{"size":1,"digest":"0c477b7abf5ce8b5037a796fc15437f3"}',
                            },
                            {
                                key: 'bucket2/key1',
                                value: '{"size":1,"digest":"cae7de298351bcbfb45f725e5e9a57ed"}',
                            },
                        ],
                        // digests match for both buckets, so there
                        // should be no request to bucketd
                        expectedBucketStreamRequests: [],
                    },
                ],
            },
            {
                desc: 'with two different entries for two buckets in db and bucketd',
                dbContents: [
                    { key: 'bucket1/key1', value: '{"foo":"bar"}' },
                    { key: 'bucket2/key1', value: '{"foo":"bar"}' },
                ],
                bucketdContents: [
                    { key: 'bucket1/key1', value: '{"foo":"qux"}' },
                    { key: 'bucket2/key1', value: '{"foo":"qux"}' },
                ],
                expectedOutput: [
                    [
                        { key: 'bucket1/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket1/key1', value: '{"foo":"qux"}' },
                    ],
                    [
                        { key: 'bucket2/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket2/key1', value: '{"foo":"qux"}' },
                    ],
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket1',
                                marker: null,
                                lastKey: null,
                            },
                            {
                                bucketName: 'bucket2',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one block for the first bucket',
                        storedDigests: [
                            {
                                key: 'bucket1/key1',
                                value: '{"size":1,"digest":"e22f7360f9fddb0e4a20ed3257023b78"}',
                            },
                        ],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket1',
                                marker: null,
                                lastKey: 'key1',
                            },
                            {
                                bucketName: 'bucket2',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one block for the second bucket',
                        storedDigests: [
                            {
                                key: 'bucket2/key1',
                                value: '{"size":1,"digest":"8a8d4413233f2a0e453da726993bec53"}',
                            },
                        ],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket1',
                                marker: null,
                                lastKey: null,
                            },
                            {
                                bucketName: 'bucket2',
                                marker: null,
                                lastKey: 'key1',
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing one block for each bucket',
                        storedDigests: [
                            {
                                key: 'bucket1/key1',
                                value: '{"size":1,"digest":"e22f7360f9fddb0e4a20ed3257023b78"}',
                            },
                            {
                                key: 'bucket2/key1',
                                value: '{"size":1,"digest":"8a8d4413233f2a0e453da726993bec53"}',
                            },
                        ],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket1',
                                marker: null,
                                lastKey: 'key1',
                            },
                            {
                                bucketName: 'bucket2',
                                marker: null,
                                lastKey: 'key1',
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with 7777 identical entries in db and bucketd',
                get dbContents() {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        dbList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return dbList;
                },
                get bucketdContents() {
                    const bucketdList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        bucketdList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return bucketdList;
                },
                expectedOutput: [
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: 'key-001999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-001999',
                                lastKey: 'key-003999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-003999',
                                lastKey: 'key-005999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-005999',
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing two blocks for the first 3667 items',
                        storedDigests: [
                            {
                                key: 'bucket/key-001888',
                                value: '{"size":1889,"digest":"f662a8604d84b2251f9ea2929f7fa9d0"}',
                            },
                            {
                                key: 'bucket/key-003666',
                                value: '{"size":1778,"digest":"8cc9f6d9efc0187e1a7df7f0d1941ae0"}',
                            },
                        ],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: 'key-003666',
                                lastKey: 'key-005666',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-005666',
                                lastKey: 'key-007666',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-007666',
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing five blocks for all items',
                        storedDigests: [
                            {
                                key: 'bucket/key-001888',
                                value: '{"size":1889,"digest":"f662a8604d84b2251f9ea2929f7fa9d0"}',
                            },
                            {
                                key: 'bucket/key-003666',
                                value: '{"size":1778,"digest":"8cc9f6d9efc0187e1a7df7f0d1941ae0"}',
                            },
                            {
                                key: 'bucket/key-005222',
                                value: '{"size":1556,"digest":"8d0fbbd126f9e53edfb8ea7e7b2ca603"}',
                            },
                            {
                                key: 'bucket/key-006888',
                                value: '{"size":1666,"digest":"35d9bbb203d2c5a5e577470f894fbcc6"}',
                            },
                            {
                                key: 'bucket/key-007776',
                                value: '{"size":888,"digest":"5b8a595a3c99cad23e8c3bb08c0c7cec"}',
                            },
                        ],
                        expectedBucketStreamRequests: [],
                    },
                ],
            },
            {
                desc: 'with 7777 entries only in db',
                get dbContents() {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        dbList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return dbList;
                },
                bucketdContents: [],
                get expectedOutput() {
                    const expectedDiff = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        expectedDiff.push([
                            { key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' },
                            null,
                        ]);
                    }
                    return expectedDiff;
                },
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: 'key-001999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-001999',
                                lastKey: 'key-003999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-003999',
                                lastKey: 'key-005999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-005999',
                                lastKey: null,
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with 7777 entries in db and bucketd with a few differences',
                get dbContents() {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (i !== 2222) {
                            const paddedI = `000000${i}`.slice(-6);
                            let value;
                            if (i === 3333) {
                                value = '{"foo":"qux"}';
                            } else {
                                value = '{"foo":"bar"}';
                            }
                            dbList.push({ key: `bucket/key-${paddedI}`, value });
                        }
                    }
                    return dbList;
                },
                get bucketdContents() {
                    const bucketdList = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (i !== 4444) {
                            const paddedI = `000000${i}`.slice(-6);
                            bucketdList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                        }
                    }
                    return bucketdList;
                },
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key-002222', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key-003333', value: '{"foo":"qux"}' },
                        { key: 'bucket/key-003333', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key-004444', value: '{"foo":"bar"}' },
                        null,
                    ],
                ],
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: 'key-001999',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-001999',
                                lastKey: 'key-004000',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-004000',
                                lastKey: 'key-006000',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-006000',
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing five blocks for all items',
                        storedDigests: [
                            {
                                key: 'bucket/key-001888',
                                value: '{"size":1889,"digest":"f662a8604d84b2251f9ea2929f7fa9d0"}',
                            },
                            {
                                key: 'bucket/key-003666',
                                value: '{"size":1778,"digest":"8cc9f6d9efc0187e1a7df7f0d1941ae0"}',
                            },
                            {
                                key: 'bucket/key-005222',
                                value: '{"size":1555,"digest":"bf5ed7af3f33013326e521a14ab37de2"}',
                            },
                            {
                                key: 'bucket/key-006888',
                                value: '{"size":1666,"digest":"35d9bbb203d2c5a5e577470f894fbcc6"}',
                            },
                            {
                                key: 'bucket/key-007776',
                                value: '{"size":888,"digest":"5b8a595a3c99cad23e8c3bb08c0c7cec"}',
                            },
                        ],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: 'key-001888',
                                lastKey: 'key-003666',
                            },
                            {
                                bucketName: 'bucket',
                                marker: 'key-003666',
                                lastKey: 'key-005222',
                            },
                        ],
                    },
                ],
            },
            {
                desc: 'with two entries in db and 7777 entries in bucketd',
                dbContents: [
                    { key: 'bucket/key-001234', value: '{"foo":"bar"}' },
                    { key: 'bucket/key-006667', value: '{"foo":"bar"}' },
                ],
                get bucketdContents() {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        dbList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return dbList;
                },
                get expectedOutput() {
                    const expectedDiff = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (![1234, 6667].includes(i)) {
                            const paddedI = `000000${i}`.slice(-6);
                            expectedDiff.push([
                                null,
                                { key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' },
                            ]);
                        }
                    }
                    return expectedDiff;
                },
                withDigests: [
                    {
                        desc: 'with no digests DB',
                        storedDigests: null,
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                    {
                        desc: 'with a digests DB containing five blocks for all items',
                        storedDigests: [
                            {
                                key: 'bucket/key-001888',
                                value: '{"size":1889,"digest":"f662a8604d84b2251f9ea2929f7fa9d0"}',
                            },
                            {
                                key: 'bucket/key-003666',
                                value: '{"size":1778,"digest":"8cc9f6d9efc0187e1a7df7f0d1941ae0"}',
                            },
                            {
                                key: 'bucket/key-005222',
                                value: '{"size":1556,"digest":"8d0fbbd126f9e53edfb8ea7e7b2ca603"}',
                            },
                            {
                                key: 'bucket/key-006888',
                                value: '{"size":1666,"digest":"35d9bbb203d2c5a5e577470f894fbcc6"}',
                            },
                            {
                                key: 'bucket/key-007776',
                                value: '{"size":888,"digest":"5b8a595a3c99cad23e8c3bb08c0c7cec"}',
                            },
                        ],
                        expectedBucketStreamRequests: [
                            {
                                bucketName: 'bucket',
                                marker: null,
                                lastKey: null,
                            },
                        ],
                    },
                ],
            },
        ].forEach(testCase => {
            const { expectedOutput } = testCase;
            testCase.withDigests.forEach(withDigests => {
                test(`${testCase.desc} yielding ${expectedOutput.length} diff entries, ${withDigests.desc}`, done => {
                    MOCK_BUCKET_STREAM_FULL_LISTING = testCase.bucketdContents;
                    const { dbContents } = testCase;
                    let digestsDb;
                    if (withDigests.storedDigests) {
                        digestsDb = new MockDigestsDB(withDigests.storedDigests);
                    } else {
                        digestsDb = null;
                    }
                    const output = [];
                    const diffStream = new DiffStream({
                        bucketdHost: 'dummy-host',
                        bucketdPort: 4242,
                        digestsDb,
                        maxBufferSize: 2000,
                        BucketStreamClass: MockBucketStream,
                    });
                    diffStream
                        .on('data', data => {
                            output.push(data);
                        })
                        .on('end', () => {
                            expect(output).toEqual(expectedOutput);
                            expect(MOCK_BUCKET_STREAM_REQUESTS_MADE)
                                .toEqual(withDigests.expectedBucketStreamRequests);
                            done();
                        })
                        .on('error', done);
                    dbContents.forEach(item => {
                        diffStream.write(item);
                    });
                    diffStream.end();
                });
            });
        });
    });
});
