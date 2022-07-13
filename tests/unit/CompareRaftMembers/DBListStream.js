const assert = require('assert');
const stream = require('stream');

const { versioning } = require('arsenal');

const DBListStream = require('../../../CompareRaftMembers/DBListStream');

class MockDBStream extends stream.Readable {
    constructor(items) {
        super({ objectMode: true });
        this.items = items;
    }

    _read() {
        this.items.forEach(item => {
            this.push(item);
        });
        this.push(null);
    }
}

describe('DBListStream', () => {
    describe('DBListStream.isLegacyDb', () => {
        [
            { dbName: 'legacy', isLegacyDb: true },
            { dbName: 'storedb42', isLegacyDb: true },
            { dbName: 'storeDb0', isLegacyDb: false },
            { dbName: 'storeDb42', isLegacyDb: false },
        ].forEach(testCase => {
            test(`${testCase.dbName} => ${testCase.isLegacyDb ? 'is' : 'is not'} a legacy DB`, () => {
                const dbListStream = new DBListStream({ dbName: testCase.dbName });
                assert.strictEqual(dbListStream.isLegacyDb, testCase.isLegacyDb);
            });
        });
    });
    describe('DBListStream output', () => {
        [
            {
                desc: 'empty DB',
                dbName: 'storeDb42',
                dbEntries: [],
                listEntries: [],
            },
            {
                desc: 'single master version',
                dbName: 'storeDb42',
                dbEntries: [
                    { key: 'bucket/object', value: '{"foo":"bar"}' },
                ],
                listEntries: [
                    { key: 'bucket/object', value: '{"foo":"bar"}' },
                ],
            },
            {
                desc: 'single versioned entry',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: 'bucket/object',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
                listEntries: [
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
            },
            {
                desc: 'single versioning-suspended entry',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: 'bucket/object',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
                listEntries: [
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
            },
            {
                desc: 'single version with PHD master',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: 'bucket/object',
                        value: '{"isPHD":true}',
                    },
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
                listEntries: [
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
            },
            {
                desc: 'single object with three versions',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: 'bucket/object',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'bucket/object\u000098345767320931999999RG001  74.505.48',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'bucket/object\u000098345767321257999999RG001  74.499.29',
                        value: '{"versionId":"98345767321257999999RG001  74.499.29"}',
                    },
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
                listEntries: [
                    {
                        key: 'bucket/object\u000098345767320931999999RG001  74.505.48',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'bucket/object\u000098345767321257999999RG001  74.499.29',
                        value: '{"versionId":"98345767321257999999RG001  74.499.29"}',
                    },
                    {
                        key: 'bucket/object\u000098345767321527999998RG001  74.489.8',
                        value: '{"versionId":"98345767321527999998RG001  74.489.8"}',
                    },
                ],
            },
            {
                desc: 'bucket attributes entry',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: 'bucket/',
                        value: '{"attributes":"{}"}',
                    },
                ],
                listEntries: [],
            },
            {
                desc: 'two objects + PHD, replay keys and keys with no "/"',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: 'bucket/object',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'bucket/object\u000098345767320931999999RG001  74.505.48',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'bucket/object2',
                        value: '{"isPHD":true}',
                    },
                    {
                        key: 'bucket/object3',
                        value: '{"versionId":"98345767320611999999RG001  74.519.84"}',
                    },
                    {
                        key: 'bucket/object3\u000098345767320611999999RG001  74.519.84',
                        value: '{"versionId":"98345767320611999999RG001  74.519.84"}',
                    },
                    {
                        key: `bucket/${versioning.VersioningConstants.DbPrefixes.Replay}foobar`,
                        value: '{}',
                    },
                    {
                        key: 'noslash',
                        value: '{}',
                    },
                ],
                listEntries: [
                    {
                        key: 'bucket/object\u000098345767320931999999RG001  74.505.48',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'bucket/object3\u000098345767320611999999RG001  74.519.84',
                        value: '{"versionId":"98345767320611999999RG001  74.519.84"}',
                    },
                ],
            },
            {
                desc: 'bucket in metadata versioning format v1',
                dbName: 'storeDb42',
                dbEntries: [
                    {
                        key: `bucket/${versioning.VersioningConstants.DbPrefixes.Master}object`,
                        value: '{}',
                    },
                    {
                        key: `bucket/${versioning.VersioningConstants.DbPrefixes.Version}object\u000098345767320931999999RG001  74.505.48`,
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                ],
                listEntries: [
                    // metadata format v1 ignored for now
                ],
            },
            {
                desc: 'two objects + PHD, replay keys in legacy DB',
                dbName: 'legacy',
                dbEntries: [
                    {
                        key: 'object',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'object\u000098345767320931999999RG001  74.505.48',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'object2',
                        value: '{"isPHD":true}',
                    },
                    {
                        key: 'object3',
                        value: '{"versionId":"98345767320611999999RG001  74.519.84"}',
                    },
                    {
                        key: 'object3\u000098345767320611999999RG001  74.519.84',
                        value: '{"versionId":"98345767320611999999RG001  74.519.84"}',
                    },
                    {
                        key: `${versioning.VersioningConstants.DbPrefixes.Replay}foobar`,
                        value: '{}',
                    },
                ],
                listEntries: [
                    {
                        key: 'legacy/object\u000098345767320931999999RG001  74.505.48',
                        value: '{"versionId":"98345767320931999999RG001  74.505.48"}',
                    },
                    {
                        key: 'legacy/object3\u000098345767320611999999RG001  74.519.84',
                        value: '{"versionId":"98345767320611999999RG001  74.519.84"}',
                    },
                ],
            },
            {
                desc: 'special buckets should be ignored',
                dbName: 'storeDb42',
                dbEntries: [
                    { key: 'users..bucket/object', value: '{"foo":"bar"}' },
                    { key: 'mpuShadowBucketfoobar/object', value: '{"foo":"bar"}' },
                    { key: 'mpushadowbucketiamnotashadowbucket/object', value: '{"foo":"bar"}' },
                ],
                listEntries: [
                    {
                        key: 'mpushadowbucketiamnotashadowbucket/object',
                        value: '{"foo":"bar"}',
                    },
                ],
            },
        ].forEach(testCase => {
            test(testCase.desc, done => {
                const dbStream = new MockDBStream(testCase.dbEntries);
                const dbListStream = new DBListStream({ dbName: testCase.dbName });
                const listedEntries = [];
                dbStream.pipe(dbListStream);
                dbListStream
                    .on('data', data => listedEntries.push(data))
                    .on('end', () => {
                        assert.deepStrictEqual(listedEntries, testCase.listEntries);
                        done();
                    })
                    .on('error', err => {
                        assert.ifError(err);
                    });
            });
        });
    });
});
