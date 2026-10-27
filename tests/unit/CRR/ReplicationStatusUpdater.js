const werelogs = require('werelogs');
const assert = require('assert');
const { models } = require('arsenal');

const {
    initializeCrrWithMocks,
    listVersionRes,
    listVersionsRes,
    listVersionWithMarkerRes,
    getMetadataRes,
    getBucketReplicationV2Res,
    objectMd,
} = require('../../utils/crr');

const logger = new werelogs.Logger('ReplicationStatusUpdater::tests', 'debug', 'debug');

// The backend we write for the default fixture rule (V1, StorageClass aws-location).
const newBackend = {
    site: 'aws-location',
    status: 'PENDING',
    dataStoreVersionId: '',
    destination: 'arn:aws:s3:::destination',
    role: 'arn:aws:iam::root:role/s3-replication-role',
};

describe('ReplicationStatusUpdater', () => {
    it('should process bucket for CRR', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            targetPrefix: 'toto',
            listingLimit: 10,
            siteName: 'aws-location',
        }, logger);

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(2); // One for listObjectVersions, one for getBucketReplication
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0',
                    MaxKeys: 10,
                    Prefix: 'toto',
                    VersionIdMarker: null,
                    KeyMarker: null,
                })
            }));
            
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0'
                })
            }));

            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledWith({
                Bucket: 'bucket0',
                Key: listVersionRes.Versions[0].Key,
                VersionId: listVersionRes.Versions[0].VersionId,
            }, expect.any(Function));

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            // We write a bucket and a role on each backend, for a V1 rule too.
            const { replicationInfo } = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            expect(replicationInfo).toEqual({
                status: 'PENDING',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                backends: [newBackend],
                content: ['METADATA', 'DATA'],
                isNFS: null,
            });

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nErrors, 0);
            return done();
        });
    });

    it('should process bucket for CRR with multiple objects', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            targetPrefix: 'toto',
            listingLimit: 10,
            siteName: 'aws-location',
        }, logger, {
            ListObjectVersionsCommand: listVersionsRes,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0',
                    MaxKeys: 10,
                    Prefix: 'toto',
                    VersionIdMarker: null,
                    KeyMarker: null,
                })
            }));
            
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0'
                })
            }));

            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.cloudserverclient.getMetadata).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Key: listVersionsRes.Versions[0].Key,
                VersionId: listVersionsRes.Versions[0].VersionId,
            }, expect.any(Function));

            expect(crr.cloudserverclient.getMetadata).toHaveBeenNthCalledWith(2, {
                Bucket: 'bucket0',
                Key: listVersionsRes.Versions[1].Key,
                VersionId: listVersionsRes.Versions[1].VersionId,
            }, expect.any(Function));

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(2);

            assert.strictEqual(crr._nProcessed, 2);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 2);
            assert.strictEqual(crr._nErrors, 0);
            return done();
        });
    });

    // An object written by S3C 9.5: its backends have no bucket and no role.
    const oldFormat = (backends, storageClass, storageType) => ({
        status: backends.some(b => b.status === 'FAILED') ? 'FAILED' : 'COMPLETED',
        backends,
        content: ['METADATA', 'DATA'],
        destination: 'arn:aws:s3:::destination',
        storageClass,
        role: 'arn:aws:iam::root:role/s3-replication-role',
        storageType,
        dataStoreVersionId: '',
    });
    // What we write when we replace every S3C 9.5 backend: no top-level destination.
    const replacedOldFormat = {
        status: 'PENDING',
        role: 'arn:aws:iam::root:role/s3-replication-role',
        backends: [newBackend],
        content: ['METADATA', 'DATA'],
        isNFS: null,
    };
    // What we write when we keep an S3C 9.5 backend: we keep the top-level destination.
    const fromOldFormat = backends => ({
        status: 'PENDING',
        role: 'arn:aws:iam::root:role/s3-replication-role',
        backends,
        content: ['METADATA', 'DATA'],
        isNFS: null,
        destination: 'arn:aws:s3:::destination',
    });
    [
        {
            description: 'for an object with a null replication info',
            replicationInfo: null,
            replicationStatusToProcess: ['NEW'],
            expectedReplicationInfo: {
                status: 'PENDING',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                backends: [newBackend],
                content: ['METADATA', 'DATA'],
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
            },
            replicationStatusToProcess: ['NEW'],
            expectedReplicationInfo: {
                status: 'PENDING',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                backends: [newBackend],
                content: ['METADATA', 'DATA'],
                isNFS: null,
            },
        }, {
            description: 'for an object with a failed replication',
            replicationInfo: oldFormat(
                [{ site: 'aws-location', status: 'FAILED', dataStoreVersionId: '' }], 'aws-location', 'aws_s3'),
            replicationStatusToProcess: ['FAILED'],
            // We replace the only S3C 9.5 backend, so we drop the top-level destination.
            expectedReplicationInfo: replacedOldFormat,
        }, {
            description: 'for an object with a completed replication',
            replicationInfo: oldFormat(
                [{ site: 'aws-location', status: 'COMPLETED', dataStoreVersionId: '' }], 'aws-location', 'aws_s3'),
            replicationStatusToProcess: ['COMPLETED'],
            expectedReplicationInfo: replacedOldFormat,
        }, {
            description: 'of a single site for an object with multiple replication destinations',
            replicationInfo: oldFormat([
                { site: 'azure-location', status: 'COMPLETED', dataStoreVersionId: '' },
                { site: 'aws-location', status: 'FAILED', dataStoreVersionId: '' },
            ], 'azure-location,aws-location', 'azure,aws_s3'),
            replicationStatusToProcess: ['FAILED'],
            // The backends keep their order: we replace aws-location in place.
            expectedReplicationInfo: fromOldFormat([
                { site: 'azure-location', status: 'COMPLETED', dataStoreVersionId: '' },
                newBackend,
            ]),
        }, {
            description: 'of a single non initialized site for an object with multiple replication destinations',
            replicationInfo: oldFormat([
                { site: 'azure-location', status: 'COMPLETED', dataStoreVersionId: '' },
                { site: 'azure-location-2', status: 'FAILED', dataStoreVersionId: '' },
            ], 'azure-location,azure-location-2', 'azure,azure'),
            replicationStatusToProcess: ['NEW'],
            // The backends keep their order: we add the new one at the end.
            expectedReplicationInfo: fromOldFormat([
                { site: 'azure-location', status: 'COMPLETED', dataStoreVersionId: '' },
                { site: 'azure-location-2', status: 'FAILED', dataStoreVersionId: '' },
                newBackend,
            ]),
        },
    ].forEach(params => {
        it(`should trigger replication ${params.description}`, done => {
            const crr = initializeCrrWithMocks({
                buckets: ['bucket0'],
                workers: 10,
                replicationStatusToProcess: ['NEW'],
                targetPrefix: 'toto',
                listingLimit: 10,
                siteName: 'aws-location',
            }, logger);
            crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
                const objectMd = JSON.parse(getMetadataRes.Body);
                objectMd.replicationInfo = params.replicationInfo;
                cb(null, { Body: JSON.stringify(objectMd) });
            });
            crr.siteName = 'aws-location';
            crr.storageType = 'aws_s3';
            crr.replicationStatusToProcess = params.replicationStatusToProcess;
            crr.run(err => {
                assert.ifError(err);

                expect(crr.s3.send).toHaveBeenCalledTimes(2);
                expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                    constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                    input: expect.objectContaining({
                        Bucket: 'bucket0',
                        MaxKeys: 10,
                        Prefix: 'toto',
                        VersionIdMarker: null,
                        KeyMarker: null,
                    })
                }));
                
                expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                    constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' }),
                    input: expect.objectContaining({
                        Bucket: 'bucket0'
                    })
                }));
                
                expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(1);
                expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
                const { replicationInfo } = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
                expect(replicationInfo).toEqual(params.expectedReplicationInfo);

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
        }, logger, {
            ListObjectVersionsCommand: listVersionWithMarkerRes,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' })
            }));
            
            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);

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
        }, logger, {
            ListObjectVersionsCommand: listVersionWithMarkerRes,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(4);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0',
                    Prefix: undefined,
                    MaxKeys: undefined,
                    KeyMarker: null,
                    VersionIdMarker: null,
                })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(3, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0',
                    KeyMarker: 'key0',
                    VersionIdMarker: 'aJdO148N3LjN00000000001I4j3QKItW'
                })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(4, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' })
            }));

            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(2);

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
        }, logger, {
            ListObjectVersionsCommand: listVersionWithMarkerRes,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' })
            }));
            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);

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

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0',
                    Prefix: undefined,
                    MaxKeys: undefined,
                    KeyMarker: 'key1',
                    VersionIdMarker: undefined,
                })
            }));

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

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' }),
                input: expect.objectContaining({
                    Bucket: 'bucket0',
                    Prefix: undefined,
                    MaxKeys: undefined,
                    KeyMarker: 'key1',
                    VersionIdMarker: 'vid1'
                })
            }));

            done();
        });
    });
});

describe('ReplicationStatusUpdater top-level role', () => {
    function initCrr(params, replicationInfo) {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            ...params,
        }, logger);
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = replicationInfo;
            cb(null, { Body: JSON.stringify(md) });
        });
        return crr;
    }

    it('should take the source role from the bucket config', done => {
        // Cloudserver wrote this object when the bucket had another source role.
        const crr = initCrr({ replicationStatusToProcess: ['COMPLETED'] }, {
            status: 'COMPLETED',
            role: 'arn:aws:iam::root:role/old-source-role',
            backends: [{ ...newBackend, status: 'COMPLETED' }],
            content: ['DATA', 'METADATA'],
            isNFS: null,
        });
        crr.run(err => {
            assert.ifError(err);
            const { replicationInfo } = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            expect(replicationInfo.role).toBe('arn:aws:iam::root:role/s3-replication-role');
            expect(replicationInfo.backends).toEqual([newBackend]);
            done();
        });
    });

    it('should keep the destination and destination role while a backend written by S3C 9.5 is kept', done => {
        // Site sf is an S3C 9.5 backend that no rule names: we keep it, so we keep the
        // top-level destination and destination role it needs. The source role comes from
        // the bucket config: backbeat uses it for the new backend too.
        const crr = initCrr({}, {
            status: 'COMPLETED',
            backends: [{ site: 'sf', status: 'COMPLETED', dataStoreVersionId: '' }],
            content: ['DATA', 'METADATA'],
            destination: 'arn:aws:s3:::destination2',
            storageClass: 'sf',
            role: 'arn:aws:iam::123456789012:role/src-resource,arn:aws:iam::123456789012:role/dest-resource',
            storageType: '',
            dataStoreVersionId: '',
        });
        crr.run(err => {
            assert.ifError(err);
            const { replicationInfo } = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            expect(replicationInfo.role).toBe(
                'arn:aws:iam::root:role/s3-replication-role,arn:aws:iam::123456789012:role/dest-resource');
            expect(replicationInfo.destination).toBe('arn:aws:s3:::destination2');
            expect(replicationInfo.backends).toEqual([
                { site: 'sf', status: 'COMPLETED', dataStoreVersionId: '' },
                newBackend,
            ]);
            done();
        });
    });

    it('should write only the source role when the S3C 9.5 object role has no destination role', done => {
        // A public cloud site in S3C 9.5 has a single role: there is no destination role to
        // keep, so we write the source role alone.
        const crr = initCrr({}, {
            status: 'COMPLETED',
            backends: [{ site: 'sf', status: 'COMPLETED', dataStoreVersionId: '' }],
            content: ['DATA', 'METADATA'],
            destination: 'arn:aws:s3:::destination2',
            storageClass: 'sf',
            role: 'arn:aws:iam::123456789012:role/src-resource',
            storageType: '',
            dataStoreVersionId: '',
        });
        crr.run(err => {
            assert.ifError(err);
            const { replicationInfo } = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            expect(replicationInfo.role).toBe('arn:aws:iam::root:role/s3-replication-role');
            expect(replicationInfo.destination).toBe('arn:aws:s3:::destination2');
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
        }, logger, {
            ListObjectVersionsCommand: listVersionsWithMixedLatest,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' })
            }));

            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.cloudserverclient.getMetadata).toHaveBeenNthCalledWith(1, {
                Bucket: 'bucket0',
                Key: 'key0',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
            }, expect.any(Function));
            expect(crr.cloudserverclient.getMetadata).toHaveBeenNthCalledWith(2, {
                Bucket: 'bucket0',
                Key: 'key1',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItU',
            }, expect.any(Function));

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(2);

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
        }, logger, {
            ListObjectVersionsCommand: listVersionsWithMixedLatest,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.s3.send).toHaveBeenNthCalledWith(1, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'ListObjectVersionsCommand' })
            }));
            expect(crr.s3.send).toHaveBeenNthCalledWith(2, expect.objectContaining({
                constructor: expect.objectContaining({ name: 'GetBucketReplicationCommand' })
            }));

            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(2);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(2);

            assert.strictEqual(crr._nProcessed, 2);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 2);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });
});

describe('ReplicationStatusUpdater model version guard', () => {
    let getModelVersionSpy;

    afterEach(() => {
        if (getModelVersionSpy) {
            getModelVersionSpy.mockRestore();
            getModelVersionSpy = null;
        }
    });

    it('should refuse to overwrite when new model version is lower than original', done => {
        // Original object metadata version from test fixture is 3
        // Force ObjectMD to report a lower version to trigger the guard
        getModelVersionSpy = jest.spyOn(models.ObjectMD.prototype, 'getModelVersion')
            .mockReturnValue(2);

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger);

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nUpdated, 0);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nErrors, 1);
            done();
        });
    });

    it('should proceed when new model version equals original', done => {
        // Match the test fixture version (3) to allow write
        getModelVersionSpy = jest.spyOn(models.ObjectMD.prototype, 'getModelVersion')
            .mockReturnValue(3);

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger);

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });

    it('should proceed when new model version is higher than original', done => {
        // Higher than the test fixture version (3)
        getModelVersionSpy = jest.spyOn(models.ObjectMD.prototype, 'getModelVersion')
            .mockReturnValue(4);

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger);

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nErrors, 0);
            done();
        });
    });
});


describe('ReplicationStatusUpdater._buildArsenalConfig', () => {
    it('should map AWS SDK replication config to arsenal ReplicationConfigurationMetadata shape', () => {
        const crr = initializeCrrWithMocks({ buckets: [], workers: 1, replicationStatusToProcess: ['NEW'] }, logger);
        const repConfig = {
            Role: 'arn:aws:iam::root:role/src,arn:aws:iam::root:role/dst',
            Rules: [
                {
                    ID: 'rule1',
                    Status: 'Enabled',
                    Filter: { Prefix: '' },
                    Priority: 1,
                    Destination: { Bucket: 'arn:aws:s3:::bucket-a', StorageClass: 'dest-A', Account: '111111111111' },
                },
                {
                    ID: 'rule2',
                    Status: 'Enabled',
                    Filter: { Prefix: 'docs/' },
                    Priority: 2,
                    Destination: { Bucket: 'arn:aws:s3:::bucket-b', StorageClass: 'dest-B', Account: '222222222222' },
                },
            ],
        };

        const result = crr._buildArsenalConfig(repConfig);

        expect(result.role).toBe(repConfig.Role);
        expect(result.destination).toBe('arn:aws:s3:::bucket-a');
        expect(result.rules).toHaveLength(2);
        expect(result.rules[0]).toMatchObject({
            enabled: true, prefix: '', storageClass: 'dest-A',
            destination: 'arn:aws:s3:::bucket-a', account: '111111111111', priority: 1, id: 'rule1',
        });
        expect(result.rules[1]).toMatchObject({
            enabled: true, prefix: 'docs/', storageClass: 'dest-B',
            destination: 'arn:aws:s3:::bucket-b', account: '222222222222', priority: 2, id: 'rule2',
        });
    });
});

describe('ReplicationStatusUpdater V2 format', () => {
    // V2 config: rule1 (prefix='', dest-A) and rule2 (prefix='docs/', dest-B)
    // Object key 'key0' matches only rule1 (dest-A)
    it('should mark object pending for matching V2 rule (single match)', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            // V2 metadata: no top-level destination/storageClass/storageType
            expect(repInfo.destination).toBeUndefined();
            expect(repInfo.storageClass).toBeUndefined();
            expect(repInfo.storageType).toBeUndefined();

            // Source role only at top level
            expect(repInfo.role).toBe('arn:aws:iam::8765432:role/sourceRole');

            // Backend has per-entry destination and role (account replaced)
            expect(repInfo.backends).toHaveLength(1);
            expect(repInfo.backends[0]).toMatchObject({
                site: 'dest-A',
                status: 'PENDING',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222222222222:role/repRule',
                dataStoreVersionId: '',
            });

            // The backbeat queue populator only queues PENDING objects.
            expect(repInfo.status).toBe('PENDING');

            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nSkipped, 0);
            done();
        });
    });

    it('should mark object pending for both V2 rules when key matches both prefixes', done => {
        const listVersionDocsKey = {
            IsTruncated: false,
            Versions: [{
                ETag: '"abc"',
                ChecksumAlgorithm: [],
                Size: 100,
                StorageClass: 'STANDARD',
                Key: 'docs/report.pdf',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
                IsLatest: true,
                LastModified: '2024-01-05T13:11:31.861Z',
                Owner: { DisplayName: 'bart', ID: '0' },
            }],
            DeleteMarkers: [],
            Name: 'bucket0',
            MaxKeys: 1000,
            CommonPrefixes: [],
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            ListObjectVersionsCommand: listVersionDocsKey,
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            // Both backends present
            expect(repInfo.backends).toHaveLength(2);
            const siteA = repInfo.backends.find(b => b.site === 'dest-A');
            const siteB = repInfo.backends.find(b => b.site === 'dest-B');
            expect(siteA).toMatchObject({
                status: 'PENDING',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222222222222:role/repRule',
            });
            expect(siteB).toMatchObject({
                status: 'PENDING',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333333333333:role/repRule',
            });

            expect(repInfo.destination).toBeUndefined();
            expect(repInfo.storageClass).toBeUndefined();
            expect(repInfo.storageType).toBeUndefined();

            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });

    it('should skip object when no V2 rule matches its key prefix', done => {
        const listVersionLogsKey = {
            IsTruncated: false,
            Versions: [{
                ETag: '"abc"',
                ChecksumAlgorithm: [],
                Size: 100,
                StorageClass: 'STANDARD',
                Key: 'logs/app.log',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW',
                IsLatest: true,
                LastModified: '2024-01-05T13:11:31.861Z',
                Owner: { DisplayName: 'bart', ID: '0' },
            }],
            DeleteMarkers: [],
            Name: 'bucket0',
            MaxKeys: 1000,
            CommonPrefixes: [],
        };

        // V2 config with only a 'docs/' prefix rule (no empty-prefix catch-all)
        const v2DocsOnly = {
            ReplicationConfiguration: {
                Role: 'arn:aws:iam::8765432:role/sourceRole',
                Rules: [{
                    ID: 'rule-docs',
                    Filter: { Prefix: 'docs/' },
                    Priority: 1,
                    Status: 'Enabled',
                    Destination: {
                        Bucket: 'arn:aws:s3:::bucket-b',
                        StorageClass: 'dest-B',
                        Account: '333333333333',
                    },
                }],
            },
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            ListObjectVersionsCommand: listVersionLogsKey,
            GetBucketReplicationCommand: v2DocsOnly,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.getMetadata).not.toHaveBeenCalled();
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();

            assert.strictEqual(crr._nProcessed, 0);
            assert.strictEqual(crr._nSkipped, 1);
            assert.strictEqual(crr._nUpdated, 0);
            done();
        });
    });

    it('should filter to SITE_NAME when set in V2 mode', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            siteName: 'dest-A',
        }, logger, {
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            // Only dest-A backend, not dest-B
            expect(repInfo.backends).toHaveLength(1);
            expect(repInfo.backends[0].site).toBe('dest-A');
            done();
        });
    });

    it('should skip V2 object when all applicable sites already match the existing status', done => {
        // key0 only matches rule1 (prefix='', dest-A); dest-A is already COMPLETED; filter is NEW → skip
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = {
                status: 'COMPLETED',
                role: 'arn:aws:iam::8765432:role/sourceRole',
                backends: [{
                    site: 'dest-A',
                    status: 'COMPLETED',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222222222222:role/repRule',
                    dataStoreVersionId: '',
                }],
                content: ['METADATA', 'DATA'],
            };
            cb(null, { Body: JSON.stringify(md) });
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nSkipped, 1);
            assert.strictEqual(crr._nUpdated, 0);
            done();
        });
    });

    it('should set top-level status PENDING for docs key with mixed backend statuses in V2', done => {
        const listVersionDocsKey = {
            IsTruncated: false,
            Versions: [{
                ETag: '"abc"', ChecksumAlgorithm: [], Size: 100,
                StorageClass: 'STANDARD', Key: 'docs/report.pdf',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW', IsLatest: true,
                LastModified: '2024-01-05T13:11:31.861Z',
                Owner: { DisplayName: 'bart', ID: '0' },
            }],
            DeleteMarkers: [], Name: 'bucket0', MaxKeys: 1000, CommonPrefixes: [],
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            ListObjectVersionsCommand: listVersionDocsKey,
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        // dest-A already COMPLETED; dest-B is NEW → both rules match 'docs/report.pdf'
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = {
                status: 'COMPLETED',
                role: 'arn:aws:iam::8765432:role/sourceRole',
                backends: [{
                    site: 'dest-A',
                    status: 'COMPLETED',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222222222222:role/repRule',
                    dataStoreVersionId: '',
                }],
                content: ['METADATA', 'DATA'],
            };
            cb(null, { Body: JSON.stringify(md) });
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            // dest-A is COMPLETED and dest-B is PENDING: we write PENDING so backbeat queues dest-B.
            expect(repInfo.status).toBe('PENDING');
            expect(repInfo.backends).toHaveLength(2);
            const destA = repInfo.backends.find(b => b.site === 'dest-A');
            const destB = repInfo.backends.find(b => b.site === 'dest-B');
            expect(destA.status).toBe('COMPLETED');
            expect(destB.status).toBe('PENDING');
            done();
        });
    });

    it('should keep a V1-format backend unchanged when this run does not select it', done => {
        // dest-A is COMPLETED in V1 format (no per-backend destination/role) — skipped by NEW filter
        // dest-B is NEW and matches 'docs/' prefix — should be set to PENDING
        // We leave dest-A as it is: this run doesn't pick it.
        const listVersionDocsKey = {
            IsTruncated: false,
            Versions: [{
                ETag: '"abc"', ChecksumAlgorithm: [], Size: 100,
                StorageClass: 'STANDARD', Key: 'docs/report.pdf',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW', IsLatest: true,
                LastModified: '2024-01-05T13:11:31.861Z',
                Owner: { DisplayName: 'bart', ID: '0' },
            }],
            DeleteMarkers: [], Name: 'bucket0', MaxKeys: 1000, CommonPrefixes: [],
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            ListObjectVersionsCommand: listVersionDocsKey,
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        // V1-format existing backend for dest-A: no destination/role per backend
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = {
                status: 'PROCESSING',
                role: 'arn:aws:iam::8765432:role/sourceRole',
                storageClass: 'dest-A',
                destination: 'arn:aws:s3:::bucket-a',
                backends: [{
                    site: 'dest-A',
                    status: 'COMPLETED',
                    dataStoreVersionId: 'v1-stored-id',
                }],
                content: ['METADATA', 'DATA'],
            };
            cb(null, { Body: JSON.stringify(md) });
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            expect(repInfo.backends).toHaveLength(2);
            const destA = repInfo.backends.find(b => b.site === 'dest-A');
            const destB = repInfo.backends.find(b => b.site === 'dest-B');

            // We keep dest-A, so we keep its bucket: the top-level destination.
            expect(destA).toEqual({ site: 'dest-A', status: 'COMPLETED', dataStoreVersionId: 'v1-stored-id' });
            expect(repInfo.destination).toBe('arn:aws:s3:::bucket-a');

            // dest-B: newly queued
            expect(destB.status).toBe('PENDING');
            done();
        });
    });

    it('should copy to the new bucket of a rule with NEW, and keep the old backend', done => {
        // The object was sent to old-bucket, and the rule for dest-A now uses bucket-a. The
        // object is not on bucket-a yet, so NEW picks it.
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
        }, logger, {
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        const oldBackend = {
            site: 'dest-A',
            status: 'COMPLETED',
            destination: 'arn:aws:s3:::old-bucket',
            role: 'arn:aws:iam::999999999999:role/oldRole',
            dataStoreVersionId: '',
        };
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = {
                status: 'COMPLETED',
                role: 'arn:aws:iam::OLD:role/staleSourceRole',
                backends: [oldBackend],
                content: ['METADATA', 'DATA'],
            };
            cb(null, { Body: JSON.stringify(md) });
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            // The source role comes from the bucket config, not from the object.
            expect(repInfo.role).toBe('arn:aws:iam::8765432:role/sourceRole');
            expect(repInfo.backends).toEqual([
                oldBackend,
                {
                    site: 'dest-A',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222222222222:role/repRule',
                },
            ]);
            done();
        });
    });

    it('should not move objects to the new bucket of a rule with COMPLETED', done => {
        // Same object: bucket-a has no backend yet, so COMPLETED doesn't pick it, and no rule
        // uses old-bucket anymore. We write nothing.
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['COMPLETED'],
        }, logger, {
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = {
                status: 'COMPLETED',
                role: 'arn:aws:iam::8765432:role/sourceRole',
                backends: [{
                    site: 'dest-A', status: 'COMPLETED', dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::old-bucket', role: 'arn:aws:iam::999999999999:role/oldRole',
                }],
                content: ['METADATA', 'DATA'],
            };
            cb(null, { Body: JSON.stringify(md) });
        });
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            assert.strictEqual(crr._nSkipped, 1);
            done();
        });
    });

    it('should preserve backends for sites outside SITE_NAME when updating', done => {
        // docs/report.pdf matches both rule1 (prefix='', dest-A) and rule2 (prefix='docs/', dest-B)
        // dest-A is NEW (will be queued); dest-B is COMPLETED (must be preserved)
        // With SITE_NAME=dest-A, candidateBackends is filtered to dest-A only.
        // Without the fix, setReplicationInfo replaces backends with [dest-A], silently dropping dest-B.
        const listVersionDocsKey = {
            IsTruncated: false,
            Versions: [{
                ETag: '"abc"', ChecksumAlgorithm: [], Size: 100,
                StorageClass: 'STANDARD', Key: 'docs/report.pdf',
                VersionId: 'aJdO148N3LjN00000000001I4j3QKItW', IsLatest: true,
                LastModified: '2024-01-05T13:11:31.861Z',
                Owner: { DisplayName: 'bart', ID: '0' },
            }],
            DeleteMarkers: [], Name: 'bucket0', MaxKeys: 1000, CommonPrefixes: [],
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            siteName: 'dest-A',
        }, logger, {
            ListObjectVersionsCommand: listVersionDocsKey,
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            // Only dest-B exists in metadata (dest-A was never replicated to)
            md.replicationInfo = {
                status: 'COMPLETED',
                role: 'arn:aws:iam::8765432:role/sourceRole',
                backends: [{
                    site: 'dest-B',
                    status: 'COMPLETED',
                    destination: 'arn:aws:s3:::bucket-b',
                    role: 'arn:aws:iam::333333333333:role/repRule',
                    dataStoreVersionId: 'dest-b-version-id',
                }],
                content: ['METADATA', 'DATA'],
            };
            cb(null, { Body: JSON.stringify(md) });
        });

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const body = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            const repInfo = body.replicationInfo;

            expect(repInfo.backends).toHaveLength(2);
            const destA = repInfo.backends.find(b => b.site === 'dest-A');
            const destB = repInfo.backends.find(b => b.site === 'dest-B');

            expect(destA.status).toBe('PENDING');
            expect(destB.status).toBe('COMPLETED');
            expect(destB.dataStoreVersionId).toBe('dest-b-version-id');
            done();
        });
    });
});

describe('ReplicationStatusUpdater site validation', () => {
    // bucket rule relying on the default replication endpoint (no StorageClass), as on S3C
    const defaultEndpointReplicationRes = {
        ReplicationConfiguration: {
            Role: 'arn:aws:iam::root:role/s3-replication-role',
            Rules: [{
                ID: 'r0',
                Prefix: '',
                Status: 'Enabled',
                Destination: { Bucket: 'arn:aws:s3:::destination' },
            }],
        },
    };
    const replicatedReplicationInfo = {
        status: 'COMPLETED',
        backends: [{ site: 'destination', status: 'COMPLETED', dataStoreVersionId: '' }],
        content: ['DATA', 'METADATA'],
        destination: 'arn:aws:s3:::destination',
        storageClass: 'destination',
        role: 'arn:aws:iam::root:role/s3-replication-role',
        storageType: '',
        dataStoreVersionId: '',
    };

    function initCrr(params, replicationRes, replicationInfo) {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            ...params,
        }, logger, replicationRes ? { GetBucketReplicationCommand: replicationRes } : {});
        if (replicationInfo) {
            crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
                const md = JSON.parse(getMetadataRes.Body);
                md.replicationInfo = replicationInfo;
                cb(null, { Body: JSON.stringify(md) });
            });
        }
        return crr;
    }

    it('should not stop a FAILED run on an object that has nothing to retry', done => {
        // The rule has no StorageClass, so it gets site foo, which the object doesn't know.
        // But FAILED picks nothing on this object: we write nothing, so we don't stop.
        const crr = initCrr({ siteName: 'foo', replicationStatusToProcess: ['FAILED'] },
            defaultEndpointReplicationRes, replicatedReplicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            assert.strictEqual(crr._nSkipped, 1);
            assert.strictEqual(crr._unknownSite, null);
            done();
        });
    });

    it('should accept a never-replicated object when a rule has no StorageClass (mixed rules)', done => {
        // R6: the default site of the rule without StorageClass can't be checked
        const res = JSON.parse(JSON.stringify(defaultEndpointReplicationRes));
        res.ReplicationConfiguration.Rules.push({
            ID: 'r1',
            Prefix: 'other/',
            Status: 'Enabled',
            Destination: { Bucket: 'arn:aws:s3:::destination', StorageClass: 'primary' },
        });
        const crr = initCrr({ siteName: 'destination' }, res);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });

    it('should accept a site known by the object but not in the bucket rules', done => {
        // rule names aws-location, object already replicates to destination
        const crr = initCrr({ siteName: 'destination', replicationStatusToProcess: ['COMPLETED'] },
            null, replicatedReplicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });

    it('should accept the default endpoint site when rules mix StorageClass and no StorageClass', done => {
        const res = JSON.parse(JSON.stringify(defaultEndpointReplicationRes));
        res.ReplicationConfiguration.Rules.push({
            ID: 'r1',
            Prefix: 'other/',
            Status: 'Enabled',
            Destination: { Bucket: 'arn:aws:s3:::destination', StorageClass: 'aws-location' },
        });
        const crr = initCrr({ siteName: 'destination', replicationStatusToProcess: ['COMPLETED'] },
            res, replicatedReplicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });

    it('should accept a SITE_NAME listed with preferred_read in the bucket rules', done => {
        const res = JSON.parse(JSON.stringify(defaultEndpointReplicationRes));
        res.ReplicationConfiguration.Rules[0].Destination.StorageClass = 'aws-location:preferred_read,foo';
        const crr = initCrr({ siteName: 'aws-location' }, res);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            done();
        });
    });

    it('should stop the run when the object replicates to another site (CERSAI case)', done => {
        const crr = initCrr({ siteName: 'foo' }, defaultEndpointReplicationRes, replicatedReplicationInfo);
        crr.run(err => {
            assert(err);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            assert.strictEqual(crr._nUpdated, 0);
            done();
        });
    });

    it('should mark an object with no replication info when rules have no StorageClass', done => {
        // no site in the rule nor in the object: nothing to compare, the site is accepted
        const crr = initCrr({ siteName: 'destination' }, defaultEndpointReplicationRes);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });

    it('should stop the run at the first unknown site and keep the resume point', done => {
        // listing always returns next markers: an unstopped run would loop forever
        const endlessListing = {
            ...listVersionsRes,
            IsTruncated: true,
            NextKeyMarker: 'next-key',
            NextVersionIdMarker: 'next-version',
        };
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0', 'bucket1'],
            workers: 1,
            replicationStatusToProcess: ['NEW'],
            siteName: 'foo',
            keyMarker: 'resume-key',
            versionIdMarker: 'resume-version',
        }, logger, {
            ListObjectVersionsCommand: endlessListing,
            GetBucketReplicationCommand: defaultEndpointReplicationRes,
        });
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = replicatedReplicationInfo;
            cb(null, { Body: JSON.stringify(md) });
        });
        const errorSpy = jest.spyOn(crr.log, 'error');
        crr.run(err => {
            assert(err);
            // bucket0 only: one listing + one GetBucketReplication, first object only
            expect(crr.s3.send).toHaveBeenCalledTimes(2);
            expect(crr.cloudserverclient.getMetadata).toHaveBeenCalledTimes(1);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            // markers of the stopped page, to resume from there
            assert.strictEqual(crr._KeyMarker, 'resume-key');
            assert.strictEqual(crr._VersionIdMarker, 'resume-version');
            expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining(
                'bucket list "bucket0,bucket1" and the following environment variables set: '
                + 'KEY_MARKER=resume-key VERSION_ID_MARKER=resume-version'));
            errorSpy.mockRestore();
            done();
        });
    });

    it('should not stop on unknown sites when allowNewSite is set', done => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 1,
            replicationStatusToProcess: ['NEW'],
            siteName: 'foo',
            allowNewSite: true,
        }, logger, {
            ListObjectVersionsCommand: listVersionsRes,
            GetBucketReplicationCommand: defaultEndpointReplicationRes,
        });
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = replicatedReplicationInfo;
            cb(null, { Body: JSON.stringify(md) });
        });
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(2);
            assert.strictEqual(crr._unknownSite, null);
            done();
        });
    });

    // newer arsenal (multi-destination CRR): sites in backends, no top-level storageClass
    const backendsOnlyReplicationInfo = {
        status: 'COMPLETED',
        backends: [{ site: 'destination', status: 'COMPLETED', dataStoreVersionId: '' }],
        content: ['DATA', 'METADATA'],
    };

    it('should read the object sites from backends when storageClass is missing', done => {
        const crr = initCrr({ siteName: 'foo' }, defaultEndpointReplicationRes, backendsOnlyReplicationInfo);
        crr.run(err => {
            assert(err);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            done();
        });
    });

    it('should accept a site known only from backends when storageClass is missing', done => {
        const crr = initCrr({ siteName: 'destination', replicationStatusToProcess: ['COMPLETED'] },
            defaultEndpointReplicationRes, backendsOnlyReplicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });

    it('should add a new site when allowNewSite is set', done => {
        const crr = initCrr({ siteName: 'foo', allowNewSite: true },
            defaultEndpointReplicationRes, replicatedReplicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            const { replicationInfo } = JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body);
            expect(replicationInfo.backends.map(b => b.site)).toEqual(['destination', 'foo']);
            assert.strictEqual(crr._nUpdated, 1);
            done();
        });
    });
});

describe('ReplicationStatusUpdater writes the multi-destination format for every rule format', () => {
    const srcRole = 'arn:aws:iam::123456789012:role/src';
    const dstRole = 'arn:aws:iam::123456789012:role/dst';
    // A V1 rule (Prefix, no Filter). Without storageClass, it uses the S3C default site.
    function v1Rule(destination, storageClass, prefix = '') {
        const rule = {
            ID: `r-${destination}`,
            Prefix: prefix,
            Status: 'Enabled',
            Destination: { Bucket: `arn:aws:s3:::${destination}` },
        };
        if (storageClass) {
            rule.Destination.StorageClass = storageClass;
        }
        return rule;
    }
    const replication = (...rules) => ({ ReplicationConfiguration: { Role: `${srcRole},${dstRole}`, Rules: rules } });

    function initCrr(params, replicationRes, replicationInfo) {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            ...params,
        }, logger, { GetBucketReplicationCommand: replicationRes });
        if (replicationInfo) {
            crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
                const md = JSON.parse(getMetadataRes.Body);
                md.replicationInfo = replicationInfo;
                cb(null, { Body: JSON.stringify(md) });
            });
        }
        return crr;
    }
    const written = crr => JSON.parse(crr.cloudserverclient.putMetadata.mock.calls[0][0].Body).replicationInfo;

    // An object that cloudserver 9.4+ wrote for site us-east-2.
    const cloudserverReplicationInfo = {
        status: 'PENDING',
        backends: [{
            site: 'us-east-2', status: 'PENDING', dataStoreVersionId: '',
            destination: 'arn:aws:s3:::dst-1', role: dstRole,
        }],
        content: ['DATA', 'METADATA'],
        role: srcRole,
        isNFS: null,
    };

    it('should add a site with its bucket and role on a V1 rule (S3C-11599)', done => {
        const crr = initCrr({ siteName: 'sf' }, replication(v1Rule('dst-1', 'sf')), cloudserverReplicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(written(crr)).toEqual({
                status: 'PENDING',
                role: srcRole,
                backends: [
                    cloudserverReplicationInfo.backends[0],
                    { site: 'sf', status: 'PENDING', dataStoreVersionId: '', destination: 'arn:aws:s3:::dst-1', role: dstRole },
                ],
                content: ['DATA', 'METADATA'],
                isNFS: null,
            });
            done();
        });
    });

    it('should fail without SITE_NAME when a rule has no StorageClass', done => {
        const rule = { ...v1Rule('dst-1'), Filter: { Prefix: '' } };
        delete rule.Prefix;
        const crr = initCrr({}, replication(rule));
        crr.run(err => {
            assert(err);
            expect(err.message).toMatch(/missing SITE_NAME/);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            done();
        });
    });

    it('should not use SITE_NAME for a rule without StorageClass when another rule names it', done => {
        // r1 has no StorageClass (bucket dr-bucket), r2 uses site sf, bucket sf-bucket.
        // r2 names SITE_NAME=sf, so r1 must not get site sf.
        const crr = initCrr({ siteName: 'sf' }, replication(v1Rule('dr-bucket'), v1Rule('sf-bucket', 'sf')));
        crr.run(err => {
            assert.ifError(err);
            expect(written(crr).backends.map(b => [b.site, b.destination])).toEqual([
                ['sf', 'arn:aws:s3:::sf-bucket'],
            ]);
            done();
        });
    });

    it('should use SITE_NAME for a rule without StorageClass when no rule names it', done => {
        // Same rules, with SITE_NAME=dr: no rule names dr, so r1 gets site dr. We don't
        // requeue r2 (site sf).
        const crr = initCrr({ siteName: 'dr', allowNewSite: true },
            replication(v1Rule('dr-bucket'), v1Rule('sf-bucket', 'sf')));
        crr.run(err => {
            assert.ifError(err);
            expect(written(crr).backends).toEqual([{
                site: 'dr', status: 'PENDING', dataStoreVersionId: '', destination: 'arn:aws:s3:::dr-bucket', role: dstRole,
            }]);
            done();
        });
    });

    it('should mark every rule site when SITE_NAME is not set', done => {
        const crr = initCrr({}, replication(v1Rule('dst-1', 'sf'), v1Rule('dst-2', 'dr2', 'key')));
        crr.run(err => {
            assert.ifError(err);
            expect(written(crr).backends.map(b => [b.site, b.destination])).toEqual([
                ['sf', 'arn:aws:s3:::dst-1'],
                ['dr2', 'arn:aws:s3:::dst-2'],
            ]);
            done();
        });
    });

    it('should skip a key that no rule prefix matches', done => {
        const crr = initCrr({ siteName: 'sf' }, replication(v1Rule('dst-1', 'sf', 'other/')));
        crr.run(err => {
            assert.ifError(err);
            expect(crr.cloudserverclient.getMetadata).not.toHaveBeenCalled();
            assert.strictEqual(crr._nSkipped, 1);
            done();
        });
    });

    it('should not add a rule site that TARGET_REPLICATION_STATUS does not select', done => {
        const replicationInfo = {
            ...cloudserverReplicationInfo,
            status: 'COMPLETED',
            backends: [{ ...cloudserverReplicationInfo.backends[0], site: 'sf', status: 'COMPLETED' }],
        };
        const crr = initCrr({ replicationStatusToProcess: ['COMPLETED'] },
            replication(v1Rule('dst-1', 'sf'), v1Rule('dst-2', 'dr2')), replicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(written(crr).backends.map(b => [b.site, b.status])).toEqual([['sf', 'PENDING']]);
            done();
        });
    });

    describe('one site with two buckets (one rule per bucket)', () => {
        const twoDestinations = replication(v1Rule('d1', 'sf'), v1Rule('d2', 'sf'));
        const backend = (destination, status) => ({
            site: 'sf', status, dataStoreVersionId: '', destination: `arn:aws:s3:::${destination}`, role: dstRole,
        });
        const withBackends = backends => ({ ...cloudserverReplicationInfo, status: 'FAILED', backends });

        [
            [backend('d1', 'COMPLETED'), backend('d2', 'FAILED')],
            [backend('d2', 'FAILED'), backend('d1', 'COMPLETED')],
        ].forEach(backends => {
            it(`should retry only the FAILED bucket (object backends ${backends.map(b => b.destination.slice(13))})`,
                done => {
                    const crr = initCrr({ replicationStatusToProcess: ['FAILED'] }, twoDestinations,
                        withBackends(backends));
                    crr.run(err => {
                        assert.ifError(err);
                        const statuses = Object.fromEntries(
                            written(crr).backends.map(b => [b.destination.slice(13), b.status]));
                        expect(statuses).toEqual({ d1: 'COMPLETED', d2: 'PENDING' });
                        done();
                    });
                });
        });
    });

    it('should warn once per bucket when SITE_NAME is not a site of the rules', done => {
        // The rules only name sf, and the object only replicates to us-east-2: we requeue nothing.
        const crr = initCrr({ siteName: 'dr9', allowNewSite: true },
            replication(v1Rule('dst-1', 'sf')), cloudserverReplicationInfo);
        const warnSpy = jest.spyOn(crr.log, 'warn');
        crr.run(err => {
            assert.ifError(err);
            const siteWarnings = warnSpy.mock.calls.filter(c => c[0].startsWith('SITE_NAME is not a site'));
            expect(siteWarnings).toHaveLength(1);
            expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
            assert.strictEqual(crr._nSkipped, 1);
            assert.strictEqual(crr._unknownSite, null);
            warnSpy.mockRestore();
            done();
        });
    });

    describe('object written by S3C 9.5, two buckets on its site', () => {
        // The object was replicated to site sf, bucket d1. Its backend has no bucket: the
        // bucket is the top-level destination. The rules now use site sf with bucket d1,
        // and site sf with bucket d2.
        const oldFormat = {
            status: 'COMPLETED',
            backends: [{ site: 'sf', status: 'COMPLETED', dataStoreVersionId: '' }],
            content: ['DATA', 'METADATA'],
            destination: 'arn:aws:s3:::d1',
            storageClass: 'sf',
            role: `${srcRole},${dstRole}`,
            storageType: '',
            dataStoreVersionId: '',
        };
        const oldBackend = oldFormat.backends[0];
        const sites = backends => backends.map(b => `${b.site}|${(b.destination || '-').split(':::').pop()}|${b.status}`);

        it('should send the object to the new bucket with NEW, and keep the old backend first', done => {
            const crr = initCrr({},
                replication(v1Rule('d1', 'sf'), v1Rule('d2', 'sf'), v1Rule('x', 'dr2')), oldFormat);
            crr.run(err => {
                assert.ifError(err);
                // The old backend is the one for bucket d1. Bucket d2 and site dr2 have none, so
                // NEW picks them. We write the old backend once.
                expect(sites(written(crr).backends)).toEqual(['sf|-|COMPLETED', 'sf|d2|PENDING', 'dr2|x|PENDING']);
                expect(written(crr).backends[0]).toEqual(oldBackend);
                done();
            });
        });

        it('should requeue only the old bucket with COMPLETED, in place', done => {
            const crr = initCrr({ replicationStatusToProcess: ['COMPLETED'] },
                replication(v1Rule('d1', 'sf'), v1Rule('d2', 'sf')), oldFormat);
            crr.run(err => {
                assert.ifError(err);
                expect(sites(written(crr).backends)).toEqual(['sf|d1|PENDING']);
                done();
            });
        });
    });

    describe('the destination role changed after the object was sent', () => {
        // The object was sent to site sf, bucket d1, with a destination role that the bucket
        // Role no longer gives. Backbeat refused it (BadRole), so the backend stayed PENDING.
        const typoRole = 'arn:aws:iam::123456789012:role/typo';
        const backend = (role, status) => ({
            site: 'sf', status, dataStoreVersionId: '', destination: 'arn:aws:s3:::d1', role,
        });
        const withBackends = backends => ({ ...cloudserverReplicationInfo, backends });
        const withAccount = (rule, account, id) => ({
            ...rule, ID: id, Destination: { ...rule.Destination, Account: account },
        });

        it('should requeue the backend in place with the new role', done => {
            const crr = initCrr({ replicationStatusToProcess: ['PENDING'] },
                replication(v1Rule('d1', 'sf')), withBackends([backend(typoRole, 'PENDING')]));
            crr.run(err => {
                assert.ifError(err);
                expect(written(crr).backends).toEqual([backend(dstRole, 'PENDING')]);
                done();
            });
        });

        it('should not add a second backend with NEW', done => {
            const crr = initCrr({}, replication(v1Rule('d1', 'sf')),
                withBackends([backend(typoRole, 'PENDING')]));
            crr.run(err => {
                assert.ifError(err);
                expect(crr.cloudserverclient.putMetadata).not.toHaveBeenCalled();
                assert.strictEqual(crr._nSkipped, 1);
                done();
            });
        });

        it('should match each backend on its own role when two rules use the same bucket', done => {
            const roleA = 'arn:aws:iam::111111111111:role/dst';
            const roleB = 'arn:aws:iam::222222222222:role/dst';
            const crr = initCrr({ replicationStatusToProcess: ['FAILED'] },
                replication(withAccount(v1Rule('d1', 'sf'), '111111111111', 'rA'),
                    withAccount(v1Rule('d1', 'sf'), '222222222222', 'rB')),
                withBackends([backend(roleA, 'COMPLETED'), backend(roleB, 'FAILED')]));
            crr.run(err => {
                assert.ifError(err);
                expect(written(crr).backends).toEqual([backend(roleA, 'COMPLETED'), backend(roleB, 'PENDING')]);
                done();
            });
        });
    });

    it('should keep isReplica on a replica that replicates further (cascading)', done => {
        const replicationInfo = {
            ...cloudserverReplicationInfo,
            status: 'COMPLETED',
            backends: [{ ...cloudserverReplicationInfo.backends[0], site: 'sf', status: 'COMPLETED' }],
            isReplica: true,
        };
        const crr = initCrr({ siteName: 'sf', replicationStatusToProcess: ['COMPLETED'] },
            replication(v1Rule('dst-1', 'sf')), replicationInfo);
        crr.run(err => {
            assert.ifError(err);
            expect(written(crr)).toEqual(expect.objectContaining({ status: 'PENDING', isReplica: true }));
            done();
        });
    });
});
