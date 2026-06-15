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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: '',
                dataStoreVersionId: '',
            };
            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledWith(
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
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
                backends: [
                    {
                        site: 'aws-location',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA', 'DATA'],
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'azure-location,aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'azure-location,aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,aws_s3',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'azure-location,azure-location-2',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,azure',
                dataStoreVersionId: '',
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
                destination: 'arn:aws:s3:::destination',
                storageClass: 'azure-location,azure-location-2,aws-location',
                role: 'arn:aws:iam::root:role/s3-replication-role',
                storageType: 'azure,azure,aws_s3',
                dataStoreVersionId: '',
            },
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
                expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledWith(
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

describe('ReplicationStatusUpdater with forceUsingConfiguration', () => {
    it('should overwrite replication destination and role when forceUsingConfiguration is true', done => {
        // Deep copy objectMd from utils to avoid affecting the original
        const objectMdWithOldReplication = JSON.parse(JSON.stringify(objectMd));
        
        // Only modify the replicationInfo part
        objectMdWithOldReplication.replicationInfo = {
            status: 'COMPLETED',
            backends: [
                {
                    site: 'sf',
                    status: 'COMPLETED',
                    dataStoreVersionId: '',
                },
            ],
            content: ['DATA', 'METADATA'],
            destination: 'arn:aws:s3:::destination2',
            storageClass: 'sf',
            role: 'arn:aws:iam::123456789012:role/src-resource,arn:aws:iam::123456789012:role/dest-resource',
            storageType: '',
            dataStoreVersionId: '',
        };

        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['NEW'],
            forceUsingConfiguration: true,
        }, logger);

        // Override getMetadata to return object with old replication info
        crr.cloudserverclient.getMetadata = jest.fn((params, cb) => cb(null, {
            Body: JSON.stringify(objectMdWithOldReplication),
        }));

        crr.run(err => {
            assert.ifError(err);

            expect(crr.cloudserverclient.putMetadata).toHaveBeenCalledTimes(1);
            
            // Verify that putMetadata was called with updated destination and role from bucket config
            const putMetadataCall = crr.cloudserverclient.putMetadata.mock.calls[0][0];
            const updatedMetadata = JSON.parse(putMetadataCall.Body);
            
            // Check that replicationInfo contains the bucket configuration values
            expect(updatedMetadata.replicationInfo.destination).toBe('arn:aws:s3:::destination');
            expect(updatedMetadata.replicationInfo.role).toBe('arn:aws:iam::root:role/s3-replication-role');
            
            assert.strictEqual(crr._nProcessed, 1);
            assert.strictEqual(crr._nSkipped, 0);
            assert.strictEqual(crr._nUpdated, 1);
            assert.strictEqual(crr._nErrors, 0);
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

describe('ReplicationStatusUpdater _removeV1Fields', () => {
    it('should delete V1-only top-level fields from replicationInfo', () => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 1,
            replicationStatusToProcess: ['NEW'],
        }, logger);

        const repInfo = {
            status: 'PENDING',
            destination: 'arn:aws:s3:::bucket',
            storageClass: 'dest-A',
            storageType: 'aws_s3',
            dataStoreVersionId: 'v1',
            role: 'arn:aws:iam::123:role/r',
            backends: [],
        };
        crr._removeV1Fields(repInfo);
        expect(repInfo.destination).toBeUndefined();
        expect(repInfo.storageClass).toBeUndefined();
        expect(repInfo.storageType).toBeUndefined();
        expect(repInfo.dataStoreVersionId).toBeUndefined();
        expect(repInfo.status).toBe('PENDING');
        expect(repInfo.role).toBe('arn:aws:iam::123:role/r');
    });

    it('should be a no-op when V1 fields are already absent', () => {
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 1,
            replicationStatusToProcess: ['NEW'],
        }, logger);

        const repInfo = { status: 'PENDING', role: 'arn:aws:iam::123:role/r', backends: [] };
        expect(() => crr._removeV1Fields(repInfo)).not.toThrow();
        expect(repInfo.destination).toBeUndefined();
        expect(repInfo.storageClass).toBeUndefined();
    });
});

describe('ReplicationStatusUpdater._buildArsenalConfig', () => {
    it('should map AWS SDK rules to arsenal ReplicationConfigurationMetadata shape', () => {
        const crr = initializeCrrWithMocks({ buckets: [], workers: 1, replicationStatusToProcess: ['NEW'] }, logger);
        const matchingRules = [
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
        ];
        const repConfig = { Role: 'arn:aws:iam::root:role/src,arn:aws:iam::root:role/dst' };

        const result = crr._buildArsenalConfig(matchingRules, repConfig);

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

            // All pending → top-level PROCESSING
            expect(repInfo.status).toBe('PROCESSING');

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

    it('should compute PROCESSING top-level status for docs key with mixed backend statuses in V2', done => {
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

            // dest-A COMPLETED, dest-B PENDING → PROCESSING
            expect(repInfo.status).toBe('PROCESSING');
            expect(repInfo.backends).toHaveLength(2);
            const destA = repInfo.backends.find(b => b.site === 'dest-A');
            const destB = repInfo.backends.find(b => b.site === 'dest-B');
            expect(destA.status).toBe('COMPLETED');
            expect(destB.status).toBe('PENDING');
            done();
        });
    });

    it('should upgrade V1-format skipped backends to V2 shape when processing mixed status', done => {
        // dest-A is COMPLETED in V1 format (no per-backend destination/role) — skipped by NEW filter
        // dest-B is NEW and matches 'docs/' prefix — should be set to PENDING
        // After processing, dest-A must have V2-shaped destination/role and its COMPLETED status preserved
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

            // dest-A: status preserved, V2 fields populated from config
            expect(destA.status).toBe('COMPLETED');
            expect(destA.dataStoreVersionId).toBe('v1-stored-id');
            expect(destA.destination).toBe('arn:aws:s3:::bucket-a');
            expect(destA.role).toBe('arn:aws:iam::222222222222:role/repRule');

            // dest-B: newly queued
            expect(destB.status).toBe('PENDING');
            done();
        });
    });

    it('should update per-backend destination and role when forceUsingConfiguration is true in V2', done => {
        // Use COMPLETED filter so the object (dest-A COMPLETED) is re-processed with forceUsingConfiguration
        const crr = initializeCrrWithMocks({
            buckets: ['bucket0'],
            workers: 10,
            replicationStatusToProcess: ['COMPLETED'],
            forceUsingConfiguration: true,
        }, logger, {
            GetBucketReplicationCommand: getBucketReplicationV2Res,
        });

        // Object has dest-A with stale destination/role
        crr.cloudserverclient.getMetadata = jest.fn((p, cb) => {
            const md = JSON.parse(getMetadataRes.Body);
            md.replicationInfo = {
                status: 'COMPLETED',
                role: 'arn:aws:iam::8765432:role/sourceRole',
                backends: [{
                    site: 'dest-A',
                    status: 'COMPLETED',
                    destination: 'arn:aws:s3:::old-bucket',
                    role: 'arn:aws:iam::999999999999:role/oldRole',
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

            const destA = repInfo.backends.find(b => b.site === 'dest-A');
            expect(destA.destination).toBe('arn:aws:s3:::bucket-a');
            expect(destA.role).toBe('arn:aws:iam::222222222222:role/repRule');
            done();
        });
    });
});
