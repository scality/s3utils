const werelogs = require('werelogs');
const assert = require('assert');
const { models } = require('arsenal');

const {
    initializeCrrWithMocks,
    listVersionRes,
    listVersionsRes,
    listVersionWithMarkerRes,
    getMetadataRes,
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
