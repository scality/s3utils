const { BucketInfo } = require('arsenal').models;
const werelogs = require('werelogs');

const { testBucketMD } = require('../../constants');
const {
    isSOSCapacityInfoEnabled,
    isValidBucketStorageMetrics,
    isValidCapacityValue,
    collectBucketMetricsAndUpdateBucketCapacityInfo,
} = require('../../../DataReport/collectBucketMetricsAndUpdateBucketCapacityInfo');
const createMongoParams = require('../../../utils/createMongoParams');
const S3UtilsMongoClient = require('../../../utils/S3UtilsMongoClient');

const logger = new werelogs.Logger('collectBucketMetricsAndUpdateBucketCapacityInfo', 'debug', 'debug');

describe('DataReport::collectBucketMetricsAndUpdateBucketCapacityInfo', () => {
    describe('isSOSCapacityInfoEnabled', () => {
        test('should return false if _capacities property doesn\'t exist in bucket', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj(testBucketMD));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi property doesn\'t exist in bucket', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {},
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi.SystemInfo property doesn\'t exist in bucket', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {},
                },
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi.SystemInfo.SystemInfo.ProtocolCapabilities property doesn\'t exist in bucket', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {
                        SystemInfo: {},
                    },
                },
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi.SystemInfo.SystemInfo.ProtocolCapabilities.CapacityInfo property doesn\'t exist in bucket', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {
                        SystemInfo: {
                            ProtocolCapabilities: {},
                        },
                    },
                },
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi.SystemInfo.SystemInfo.ProtocolCapabilities.CapacityInfo property is not boolean', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {
                        SystemInfo: {
                            ProtocolCapabilities: {
                                CapacityInfo: 'true',
                            },
                        },
                    },
                },
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi.SystemInfo.SystemInfo.ProtocolCapabilities.CapacityInfo property is false', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {
                        SystemInfo: {
                            ProtocolCapabilities: {
                                CapacityInfo: false,
                            },
                        },
                    },
                },
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return false if _capacities.VeeamSOSApi.SystemInfo.SystemInfo.ProtocolCapabilities.CapacityInfo property is true'
            + 'and _capacities.VeeamSOSApi.CapacityInfo doesn\'t exist', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {
                        SystemInfo: {
                            ProtocolCapabilities: {
                                CapacityInfo: false,
                            },
                        },
                    },
                },
            }));
            expect(enabled).toBeFalsy();
        });

        test('should return true if _capacities.VeeamSOSApi.SystemInfo.SystemInfo.ProtocolCapabilities.CapacityInfo property is true'
            + 'and _capacities.VeeamSOSApi.CapacityInfo exists', () => {
            const enabled = isSOSCapacityInfoEnabled(BucketInfo.fromObj({
                ...testBucketMD,
                _capabilities: {
                    VeeamSOSApi: {
                        SystemInfo: {
                            ProtocolCapabilities: {
                                CapacityInfo: true,
                            },
                        },
                        CapacityInfo: {},
                    },
                },
            }));
            expect(enabled).toBeTruthy();
        });
    });

    describe('isValidBucketStorageMetrics', () => {
        test('should return false if bucket metrics is empty', () => {
            const valid = isValidBucketStorageMetrics(null);
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity property doesn\'t exist', () => {
            const valid = isValidBucketStorageMetrics({});
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity.current property doesn\'t exist', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { nonCurrent: 0 },
            });
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity.nonCurrent property doesn\'t exist', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { current: 0 },
            });
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity.current value is negative', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { current: -1, nonCurrent: 0 },
            });
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity.nonCurrent value is negative', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { current: 0, nonCurrent: -1 },
            });
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity.current value is not a number', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { current: 'not-a-number', nonCurrent: 0 },
            });
            expect(valid).toBeFalsy();
        });

        test('should return false if usedCapacity.nonCurrent value is not a number', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { current: 0, nonCurrent: 'not-a-number' },
            });
            expect(valid).toBeFalsy();
        });

        test('should return true if usedCapacity.current and usedCapacity.nonCurrent value are both valid', () => {
            const valid = isValidBucketStorageMetrics({
                usedCapacity: { current: 0, nonCurrent: 0 },
            });
            expect(valid).toBeTruthy();
        });
    });

    describe('isValidCapacityValue', () => {
        test('should return false if capacity is not a number', () => {
            const valid = isValidCapacityValue('not-a-number');
            expect(valid).toBeFalsy();
        });

        test('should return false if capacity is not a integer', () => {
            const valid = isValidCapacityValue(0.2);
            expect(valid).toBeFalsy();
        });

        test('should return false if capacity is negative', () => {
            const valid = isValidCapacityValue(-1);
            expect(valid).toBeFalsy();
        });

        test('should return false if capacity is not a safe integer', () => {
            const valid = isValidCapacityValue(2 ** 53);
            expect(valid).toBeFalsy();
        });

        test('should return true if capacity is a valid integer', () => {
            const valid = isValidCapacityValue(1);
            expect(valid).toBeTruthy();
        });
    });

    describe('collectBucketMetricsAndUpdateBucketCapacityInfo', () => {
        let mongoClient;
        beforeEach(done => {
            mongoClient = new S3UtilsMongoClient(createMongoParams(logger));
            mongoClient.setup = jest.fn();
            mongoClient.readStorageConsumptionMetrics = jest.fn();
            mongoClient.getBucketInfos = jest.fn();
            mongoClient.updateBucketCapacityInfo = jest.fn();
            mongoClient.setup.mockImplementation(cb => cb(null));
            return mongoClient.setup(done);
        });

        afterEach(() => jest.resetAllMocks());

        test('should not pass if mongo getBucketInfos() returns error', done => {
            mongoClient.getBucketInfos
                .mockImplementation((log, cb) => cb(new Error('error getting bucket list')));
            return collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, logger, err => {
                expect(err).toEqual(new Error('error getting bucket list'));
                done();
            });
        });

        test('should not pass if mongo updateBucketCapacityInfo() returns error', done => {
            mongoClient.getBucketInfos
                .mockImplementation((log, cb) => cb(null, {
                    bucketInfos: [BucketInfo.fromObj({
                        ...testBucketMD,
                        _capabilities: {
                            VeeamSOSApi: {
                                SystemInfo: {
                                    ProtocolCapabilities: {
                                        CapacityInfo: true,
                                    },
                                },
                                CapacityInfo: {},
                            },
                        },
                    })],
                }));
            mongoClient.readStorageConsumptionMetrics
                .mockImplementation((bucketName, log, cb) => cb(null, {}));
            mongoClient.updateBucketCapacityInfo
                .mockImplementation((bucketName, bucket, log, cb) => cb(new Error('an error occurred during put bucket CapacityInfo attributes')));
            return collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, logger, err => {
                expect(err).toEqual(new Error('an error occurred during put bucket CapacityInfo attributes'));
                done();
            });
        });

        test('should pass with empty bucket', done => {
            mongoClient.getBucketInfos
                .mockImplementation((log, cb) => cb(null, {
                    bucketInfos: [],
                }));
            return collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, logger, err => {
                expect(err).toBeNull();
                done();
            });
        });

        test('should pass with SOSAPI disabled buckets', done => {
            mongoClient.readStorageConsumptionMetrics
                .mockImplementation((bucketName, log, cb) => cb(null, {
                    usedCapacity: { current: 0, nonCurrent: 0 },
                }));
            mongoClient.getBucketInfos
                .mockImplementation((log, cb) => cb(null, {
                    bucketInfos: [BucketInfo.fromObj({
                        ...testBucketMD,
                        _capabilities: {},
                    })],
                }));
            return collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, logger, err => {
                expect(err).toBeNull();
                done();
            });
        });

        test('should pass with SOSAPI enabled buckets and update bucket md', done => {
            mongoClient.readStorageConsumptionMetrics
                .mockImplementation((bucketName, log, cb) => cb(null, {
                    usedCapacity: { current: 0, nonCurrent: 0 },
                }));
            mongoClient.getBucketInfos
                .mockImplementation((log, cb) => cb(null, {
                    bucketInfos: [BucketInfo.fromObj({
                        ...testBucketMD,
                        _capabilities: {
                            VeeamSOSApi: {
                                SystemInfo: {
                                    ProtocolCapabilities: {
                                        CapacityInfo: true,
                                    },
                                },
                                CapacityInfo: {},
                            },
                        },
                    })],
                }));
            mongoClient.updateBucketCapacityInfo
                .mockImplementation((bucketName, bucket, log, cb) => cb(null));
            return collectBucketMetricsAndUpdateBucketCapacityInfo(mongoClient, logger, err => {
                expect(err).toBeNull();
                expect(mongoClient.updateBucketCapacityInfo).toHaveBeenCalled();
                done();
            });
        });
    });
});
