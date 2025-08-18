const werelogs = require('werelogs');
const assert = require('assert');

const {
    initializeCrrWithMocks,
    listVersionRes,
    listVersionsRes,
    listVersionWithMarkerRes,
    getMetadataRes,
} = require('../../utils/crr');

const logger = new werelogs.Logger('ReplicationStatusUpdater::tests', 'debug', 'debug');

describe('ReplicationStatusUpdater', () => {
    let crr;

    beforeEach(() => {
        crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            targetPrefix: 'toto',
            listingLimit: 10,
            siteName: 'aws-location',
        }, logger);
    });

    it('should process bucket for CRR', done => {
        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.listObjectVersions).toHaveBeenCalledWith({
                Bucket: 'bucket0',
                KeyMarker: null,
                MaxKeys: 10,
                Prefix: 'toto',
                VersionIdMarker: null,
            }, expect.any(Function));

            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);
            expect(crr.s3.getBucketReplication).toHaveBeenCalledWith({
                Bucket: 'bucket0',
            }, expect.any(Function));

            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.bb.getMetadata).toHaveBeenCalledWith({
                Bucket: 'bucket0',
                Key: listVersionRes.Versions[0].Key,
                VersionId: listVersionRes.Versions[0].VersionId,
            }, expect.any(Function));

            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(1);
            const expectedReplicationInfo = {
                status: 'PENDING',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: '',
                dataStoreVersionId: '',
                isNFS: null,
            };
            expect(crr.bb.putMetadata).toHaveBeenCalledWith(
                expect.objectContaining({
                    Body: expect.stringContaining(JSON.stringify(expectedReplicationInfo)),
                }),
                expect.any(Function),
            );

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nErrors, 0);
            return done();
        });
    });

    it('should process bucket for CRR with multiple objects', done => {
        crr.s3.listObjectVersions = jest.fn((params, cb) => cb(null, listVersionsRes));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.listObjectVersions).toHaveBeenCalledWith({
                Bucket: 'bucket0',
                KeyMarker: null,
                MaxKeys: 10,
                Prefix: 'toto',
                VersionIdMarker: null,
            }, expect.any(Function));

            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);
            expect(crr.s3.getBucketReplication).toHaveBeenCalledWith({
                Bucket: 'bucket0',
            }, expect.any(Function));

            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.bb.getMetadata).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Key: listVersionsRes.Versions[0].Key,
                VersionId: listVersionsRes.Versions[0].VersionId,
            }, expect.any(Function));

            expect(crr.bb.getMetadata).toHaveBeenNthCalledWith(2, {
                Bucket: 'bucket0',
                Key: listVersionsRes.Versions[1].Key,
                VersionId: listVersionsRes.Versions[1].VersionId,
            }, expect.any(Function));

            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(2);

            assert.strictEqual(crr._nProcessed, 2);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 2);
            assert.strictEqual(crr._nErrors, 0);
            return done();
        });
    });

    [
        {
            description: 'for an object with a null replication info',
            replicationInfo: null,
            replicationStatusToProcess: ['NEW'],
            expectedReplicationInfo: {
                status: 'PENDING',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
        }, {
            description: 'for an object with empty replication info',
            replicationInfo: {
                status: '',
                backends: [],
                content: [],
                destination: '',
                storageClass: '',
                role: '',
                storageType: '',
                dataStoreVersionId: '',
                isNFS: null,
            },
            replicationStatusToProcess: ['NEW'],
            expectedReplicationInfo: {
                status: 'PENDING',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
        }, {
            description: 'for an object with a failed replication',
            replicationInfo: {
                status: 'FAILED',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'FAILED',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
            replicationStatusToProcess: ['FAILED'],
            expectedReplicationInfo: {
                status: 'PENDING',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
        }, {
            description: 'for an object with a completed replication',
            replicationInfo: {
                status: 'COMPLETED',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'COMPLETED',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
            replicationStatusToProcess: ['COMPLETED'],
            expectedReplicationInfo: {
                status: 'PENDING',
                backends: [
                    {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
        }, {
            description: 'of a single site for an object with multiple replication destinations',
            replicationInfo: {
                status: 'FAILED',
                backends: [
                    {
                        site: 'azure-location',
                        status: 'COMPLETED',
                        dataStoreVersionId: '',
                    },
                    {
                        site: 'aws-location',
                        status: 'FAILED',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'azure-location,aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
            replicationStatusToProcess: ['FAILED'],
            expectedReplicationInfo: {
                status: 'PENDING',
                backends: [
                    {
                        site: 'azure-location',
                        status: 'COMPLETED',
                        dataStoreVersionId: '',
                    }, {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'azure-location,aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
        }, {
            description: 'of a single non initialized site for an object with multiple replication destinations',
            replicationInfo: {
                status: 'FAILED',
                backends: [
                    {
                        site: 'azure-location',
                        status: 'COMPLETED',
                        dataStoreVersionId: '',
                    },
                    {
                        site: 'azure-location-2',
                        status: 'FAILED',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'azure-location,azure-location-2',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,azure',
                dataStoreVersionId: '',
                isNFS: null,
            },
            replicationStatusToProcess: ['NEW'],
            expectedReplicationInfo: {
                status: 'PENDING',
                backends: [
                    {
                        site: 'azure-location',
                        status: 'COMPLETED',
                        dataStoreVersionId: '',
                    }, {
                        site: 'azure-location-2',
                        status: 'FAILED',
                        dataStoreVersionId: '',
                    }, {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::sourcebucket',
                storageClass: 'azure-location,azure-location-2,aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,azure,aws_s3',
                dataStoreVersionId: '',
                isNFS: null,
            },
        },
    ].forEach(params => {
        it(`should trigger replication ${params.description}`, done => {
            crr.bb.getMetadata = jest.fn((p, cb) => {
                const objectMd = JSON.parse(getMetadataRes.Body);
                objectMd.replicationInfo = params.replicationInfo;
                cb(null, { Body: JSON.stringify(objectMd) });
            });
            crr.siteName = 'aws-location';
            crr.storageType = 'aws_s3';
            crr.replicationStatusToProcess = params.replicationStatusToProcess;
            crr.run(err => {
                assert.ifError(err);

                expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
                expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);
                expect(crr.bb.getMetadata).toHaveBeenCalledTimes(1);
                expect(crr.bb.putMetadata).toHaveBeenCalledTimes(1);
                expect(crr.bb.putMetadata).toHaveBeenCalledWith(
                    expect.objectContaining({
                        Body: expect.stringContaining(JSON.stringify(params.expectedReplicationInfo)),
                    }),
                    expect.any(Function),
                );

                assert.strictEqual(crr._nProcessed, 1);
                assert.strictEqual(crr._nSkipped, 0);
                assert.strictEqual(crr._nUpdated, 1);
                assert.strictEqual(crr._nErrors, 0);
                return done();
            });
        });
    });
});

describe('ReplicationStatusUpdater with specifics', () => {
    it('maxUpdates set to 1', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            maxUpdates: 1,
        }, logger);

        crr.s3.listObjectVersions = jest.fn((params, cb) => cb(null, listVersionWithMarkerRes));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);
            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(1);

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });

    it('maxUpdates set to 2', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            maxUpdates: 2,
        }, logger);

        crr.s3.listObjectVersions = jest.fn((params, cb) => cb(null, listVersionWithMarkerRes));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(2);

            expect(crr.s3.listObjectVersions).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Prefix: undefined,
                MaxKeys: undefined,
                KeyMarker: null,
                VersionIdMarker: null,
            }, expect.any(Function));

            expect(crr.s3.listObjectVersions).toHaveBeenNthCalledWith(2, {
                Bucket: 'bucket0',
                Prefix: undefined,
                MaxKeys: undefined,
                KeyMarker: 'key0',
                VersionIdMarker: 'aJdO148N3LjN00000000001I4j3QKItW',
            }, expect.any(Function));

            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(2);
            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(2);

            assert.strictEqual(crr._nProcessed, 2);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 2);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });

    it('maxScanned set to 1', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            maxScanned: 1,
        }, logger);

        crr.s3.listObjectVersions = jest.fn((params, cb) => cb(null, listVersionWithMarkerRes));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);
            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(1);

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });

    it('set inputKeyMarker', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            keyMarker: 'key1',
        }, logger);

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.listObjectVersions).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Prefix: undefined,
                MaxKeys: undefined,
                KeyMarker: 'key1',
                VersionIdMarker: undefined,
            }, expect.any(Function));

            done();
        });
    });

    it('set inputKeyMarker and inputVersionIdMarker', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            keyMarker: 'key1',
            versionIdMarker: 'vid1',
        }, logger);

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.listObjectVersions).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Prefix: undefined,
                MaxKeys: undefined,
                KeyMarker: 'key1',
                VersionIdMarker: 'vid1',
            }, expect.any(Function));

            done();
        });
    });
});

describe('ReplicationStatusUpdater with currentVersionOnly', () => {
    it('should process only latest versions when currentVersionOnly is true', done => {
        const listVersionsWithMixedLatest = {
            IsTruncated: false,
            Versions: [
                {
                    ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
                    ChecksumAlgorithm: [],
                    Size: 3263,
                    StorageClass: 'STANDARD',
                    Key: 'key0',
                    VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
                    IsLatest: true,
                    LastModified: '2024-01-05T13:11:31.861Z',
                    Owner: {
                        DisplayName: 'bart',
                        ID: '0',
                    },
                },
                {
                    ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
                    ChecksumAlgorithm: [],
                    Size: 3263,
                    StorageClass: 'STANDARD',
                    Key: 'key0',
                    VersionId: 'aJdO148N3LjN00000000001I4j3QKItV',
                    IsLatest: false,
                    LastModified: '2024-01-05T13:11:30.861Z',
                    Owner: {
                        DisplayName: 'bart',
                        ID: '0',
                    },
                },
                {
                    ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
                    ChecksumAlgorithm: [],
                    Size: 3263,
                    StorageClass: 'STANDARD',
                    Key: 'key1',
                    VersionId: 'aJdO148N3LjN00000000001I4j3QKItU',
                    IsLatest: true,
                    LastModified: '2024-01-05T13:11:32.861Z',
                    Owner: {
                        DisplayName: 'bart',
                        ID: '0',
                    },
                },
                {
                    ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
                    ChecksumAlgorithm: [],
                    Size: 3263,
                    StorageClass: 'STANDARD',
                    Key: 'key1',
                    VersionId: 'aJdO148N3LjN00000000001I4j3QKItT',
                    IsLatest: false,
                    LastModified: '2024-01-05T13:11:31.861Z',
                    Owner: {
                        DisplayName: 'bart',
                        ID: '0',
                    },
                },
            ],
            DeleteMarkers: [],
            Name: 'bucket0',
            MaxKeys: 1000,
            CommonPrefixes: [],
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            currentVersionOnly: true,
        }, logger);

        crr.s3.listObjectVersions = jest.fn((params, cb) => cb(null, listVersionsWithMixedLatest));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);

            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.bb.getMetadata).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Key: 'key0',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
            }, expect.any(Function));
            expect(crr.bb.getMetadata).toHaveBeenNthCalledWith(2, {
                Bucket: 'bucket0',
                Key: 'key1',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItU',
            }, expect.any(Function));

            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(2);

            assert.strictEqual(crr._nProcessed, 2);
            assert.strictEqual(crr._nSkipped, 2);
            assert.strictEqual(crr._nUpdated, 2);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });

    it('should process all versions when currentVersionOnly is false', done => {
        const listVersionsWithMixedLatest = {
            IsTruncated: false,
            Versions: [
                {
                    ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
                    ChecksumAlgorithm: [],
                    Size: 3263,
                    StorageClass: 'STANDARD',
                    Key: 'key0',
                    VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
                    IsLatest: true,
                    LastModified: '2024-01-05T13:11:31.861Z',
                    Owner: {
                        DisplayName: 'bart',
                        ID: '0',
                    },
                },
                {
                    ETag: '"dabcc341ecab339daf766e1cddd5d1bb"',
                    ChecksumAlgorithm: [],
                    Size: 3263,
                    StorageClass: 'STANDARD',
                    Key: 'key0',
                    VersionId: 'aJdO148N3LjN00000000001I4j3QKItV',
                    IsLatest: false,
                    LastModified: '2024-01-05T13:11:30.861Z',
                    Owner: {
                        DisplayName: 'bart',
                        ID: '0',
                    },
                },
            ],
            DeleteMarkers: [],
            Name: 'bucket0',
            MaxKeys: 1000,
            CommonPrefixes: [],
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            currentVersionOnly: false,
        }, logger);

        crr.s3.listObjectVersions = jest.fn((params, cb) => cb(null, listVersionsWithMixedLatest));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.listObjectVersions).toHaveBeenCalledTimes(1);
            expect(crr.s3.getBucketReplication).toHaveBeenCalledTimes(1);

            expect(crr.bb.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.bb.putMetadata).toHaveBeenCalledTimes(2);

            assert.strictEqual(crr._nProcessed, 2);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 2);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });
});
