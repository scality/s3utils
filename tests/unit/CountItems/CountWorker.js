const CountWorker = require('../../../CountItems/CountWorker');

const mongoMock = require('../../mocks/mongoClient');
const DummyLogger = require('../../mocks/DummyLogger');

const { testBucketMD } = require('../../constants');

describe('CountItems::CountWorker', () => {
    beforeEach(() => {
        mongoMock.close.mockReset();
        mongoMock.setup.mockReset();
        mongoMock.client.isConnected.mockReset();
        mongoMock.getObjectMDStats.mockReset();
        mongoMock.getObject.mockReset();
    });

    const t = [
        [
            'should correctly handle successful setup',
            {
                mock: {
                    setup: [null],
                    close: [],
                    isConnected: false,
                    getIsTransient: [],
                    getObjectMDStats: [],
                },
                incomingMessage: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'setup',
                },
            },
            {
                message: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'setup',
                    status: 'passed',
                },
            },
        ],
        [
            'should correctly handle failed setup',
            {
                mock: {
                    setup: [new Error('failed setup')],
                    close: [],
                    isConnected: false,
                    getIsTransient: [],
                    getObjectMDStats: [],
                },
                incomingMessage: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'setup',
                },
            },
            {
                message: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'setup',
                    status: 'failed',
                    error: 'failed setup',
                },
            },
        ],
        [
            'should correctly handle successful teardown',
            {
                mock: {
                    setup: [],
                    close: [null],
                    isConnected: true,
                    getIsTransient: [],
                    getObjectMDStats: [],
                },
                incomingMessage: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'teardown',
                },
            },
            {
                message: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'teardown',
                    status: 'passed',
                },
            },
        ],
        [
            'should correctly handle failed teardown',
            {
                mock: {
                    setup: [],
                    close: [new Error('failed teardown')],
                    isConnected: true,
                    getIsTransient: [],
                    getObjectMDStats: [],
                },
                incomingMessage: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'teardown',
                },
            },
            {
                message: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'teardown',
                    status: 'failed',
                    error: 'failed teardown',
                },
            },
        ],
        [
            'should correctly handle successful count task',
            {
                mock: {
                    setup: [],
                    close: [],
                    isConnected: true,
                    getIsTransient: [null, true],
                    getObjectMDStats: [null, { value: 42 }],
                },
                incomingMessage: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'count',
                    bucketInfo: testBucketMD,
                },
            },
            {
                message: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'count',
                    status: 'passed',
                    results: { value: 42 },
                },
            },
        ],
        [
            'should correctly handle failed count task',
            {
                mock: {
                    setup: [],
                    close: [],
                    isConnected: true,
                    getIsTransient: [null, true],
                    getObjectMDStats: [new Error('count error')],
                },
                incomingMessage: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'count',
                    bucketInfo: testBucketMD,
                },
            },
            {
                message: {
                    id: 'test-id',
                    owner: 'scality',
                    type: 'count',
                    status: 'failed',
                    error: 'count error',
                },
            },
        ],
    ];

    test.each(t)('%s', (msg, tc, e, done) => {
        const testSendFn = jest.fn();
        const w = new CountWorker({
            log: new DummyLogger(),
            sendFn: testSendFn,
            client: mongoMock,
        });
        w.getIsTransient = jest.fn((bucketInfo, cb) => cb(...tc.mock.getIsTransient));
        mongoMock.setup.mockImplementationOnce(cb => cb(...tc.mock.setup));
        mongoMock.close.mockImplementationOnce(cb => cb(...tc.mock.close));
        mongoMock.client.isConnected
            .mockImplementationOnce(() => tc.mock.isConnected);
        mongoMock.getObjectMDStats.mockImplementationOnce(
            (_a, _b, _c, _d, cb) => cb(...tc.mock.getObjectMDStats),
        );
        w.handleMessage(tc.incomingMessage);
        setTimeout(() => {
            expect(testSendFn).toHaveBeenCalledWith(e.message);
            done();
        }, 100);
    });

    // test that the countworker's "countItems" method properly handles
    // buckets with website
    test('should correctly handle buckets with website', done => {
        const testSendFn = jest.fn();
        const w = new CountWorker({
            log: new DummyLogger(),
            sendFn: testSendFn,
            client: mongoMock,
        });
        const bucketInfo = {
            _name: 'test-bucket',
            _owner: 'any',
            _ownerDisplayName: 'any',
            _creationDate: Date.now().toString(),
            website: { indexDocument: 'index.html' },
        };
        w.getIsTransient = jest.fn((bucketInfo, cb) => cb(null, true));
        mongoMock.setup.mockImplementationOnce(cb => cb());
        mongoMock.close.mockImplementationOnce(cb => cb());
        mongoMock.client.isConnected.mockImplementationOnce(() => false);
        mongoMock.getObjectMDStats.mockImplementationOnce((_a, _b, _c, _d, cb) => cb(null, { value: 42 }));
        w.countItems(bucketInfo, (err, results) => {
            expect(err).toBeNull();
            expect(results).toEqual({ value: 42 });
            done();
        });
    });

    test('should nullify unsupported serialized bucket info fields', done => {
        const testSendFn = jest.fn();
        const w = new CountWorker({
            log: new DummyLogger(),
            sendFn: testSendFn,
            client: mongoMock,
        });
        const bucketInfo = {
            _name: 'test-bucket',
            _owner: 'any',
            _ownerDisplayName: 'any',
            _creationDate: Date.now().toString(),
            _websiteConfiguration: {
                _indexDocument: 'index.html',
            },
            _bucketLoggingStatus: {
                _loggingEnabled: {
                    TargetBucket: 'target-bucket',
                    TargetPrefix: 'logs/',
                },
            },
        };
        w.getIsTransient = jest.fn((bucketInfo, cb) => cb(null, true));
        mongoMock.setup.mockImplementationOnce(cb => cb());
        mongoMock.close.mockImplementationOnce(cb => cb());
        mongoMock.client.isConnected.mockImplementationOnce(() => false);
        mongoMock.getObjectMDStats.mockImplementationOnce((_a, _b, _c, _d, cb) => cb(null, { value: 42 }));
        w.countItems(bucketInfo, (err, results) => {
            expect(err).toBeNull();
            expect(results).toEqual({ value: 42 });
            expect(bucketInfo._websiteConfiguration).toBeNull();
            expect(bucketInfo._bucketLoggingStatus).toBeNull();
            done();
        });
    });

    describe('CountWorker.getIsTransient method', () => {
        let worker;
        let mockBucketInfo;

        beforeEach(() => {
            worker = new CountWorker({
                log: new DummyLogger(),
                sendFn: jest.fn(),
                client: mongoMock,
            });
            mockBucketInfo = {
                _name: 'test-bucket',
                _owner: 'any',
                _ownerDisplayName: 'any',
                _creationDate: Date.now().toString(),
                getLocationConstraint: jest.fn(() => 'test-location'),
            };
        });

        test('should use client.isLocationTransient if available and return true', done => {
            mongoMock.isLocationTransient = jest.fn((loc, lg, cb) => cb(null, true));
            worker.getIsTransient(mockBucketInfo, (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(true);
                expect(mongoMock.isLocationTransient).toHaveBeenCalledWith('test-location', worker.log, expect.any(Function));
                done();
            });
        });

        test('should use client.isLocationTransient if available and return false', done => {
            mongoMock.isLocationTransient = jest.fn((loc, lg, cb) => cb(null, false));
            worker.getIsTransient(mockBucketInfo, (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(false);
                done();
            });
        });

        test('should propagate error from client.isLocationTransient', done => {
            const testError = new Error('client.isLocationTransient error');
            mongoMock.isLocationTransient = jest.fn((loc, lg, cb) => cb(testError));
            worker.getIsTransient(mockBucketInfo, (err, isTransient) => {
                expect(err).toBe(testError);
                expect(isTransient).toBeUndefined();
                done();
            });
        });

        describe('when client.isLocationTransient is not available (fallback to pensieve)', () => {
            beforeEach(() => {
                mongoMock.isLocationTransient = undefined;
            });

            test('should fallback to pensieveLocationIsTransient and return true', done => {
                mongoMock.getObject
                    .mockImplementationOnce((bucket, key, params, log, cb) => cb(null, 'v1')) // 1st call
                    .mockImplementationOnce((bucket, key, params, log, cb) => cb(null, { // 2nd call
                        locations: { 'test-location': { isTransient: true } },
                    }));
                worker.getIsTransient(mockBucketInfo, (err, isTransient) => {
                    expect(err).toBeNull();
                    expect(isTransient).toBe(true);
                    expect(mongoMock.getObject).toHaveBeenCalledTimes(2);
                    done();
                });
            });

            test('should fallback to pensieveLocationIsTransient and return false', done => {
                mongoMock.getObject
                    .mockImplementationOnce((bucket, key, params, log, cb) => cb(null, 'v1'))
                    .mockImplementationOnce((bucket, key, params, log, cb) => cb(null, {
                        locations: { 'test-location': { isTransient: false } },
                    }));
                worker.getIsTransient(mockBucketInfo, (err, isTransient) => {
                    expect(err).toBeNull();
                    expect(isTransient).toBe(false);
                    done();
                });
            });

            test('should propagate error from pensieveLocationIsTransient (e.g., first getObject fails)', done => {
                const pensieveError = new Error('Pensieve getObject error');
                mongoMock.getObject.mockImplementationOnce((b, k, p, l, cb) => cb(pensieveError));
                worker.getIsTransient(mockBucketInfo, (err, isTransient) => {
                    expect(err).toBe(pensieveError);
                    expect(isTransient).toBeUndefined();
                    done();
                });
            });
        });
    });

    describe('CountWorker.pensieveLocationIsTransient method', () => {
        let worker;
        const PENSIEVE_BUCKET_NAME = 'PENSIEVE';

        beforeEach(() => {
            worker = new CountWorker({
                log: new DummyLogger(),
                sendFn: jest.fn(),
                client: mongoMock,
            });
        });

        test('should return true if location is transient in Pensieve config', done => {
            mongoMock.getObject
                .mockImplementationOnce((bucket, key, params, log, cb) => {
                    expect(bucket).toBe(PENSIEVE_BUCKET_NAME);
                    expect(key).toBe('configuration/overlay-version');
                    cb(null, 'v-p-1');
                })
                .mockImplementationOnce((bucket, key, params, log, cb) => {
                    expect(bucket).toBe(PENSIEVE_BUCKET_NAME);
                    expect(key).toBe('configuration/overlay/v-p-1');
                    cb(null, { locations: { 'loc-A': { isTransient: true } } });
                });

            worker.pensieveLocationIsTransient('loc-A', (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(true);
                done();
            });
        });

        test('should return false if location is not transient in Pensieve config', done => {
            mongoMock.getObject
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, 'v-p-2'))
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, { locations: { 'loc-B': { isTransient: false } } }));

            worker.pensieveLocationIsTransient('loc-B', (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(false);
                done();
            });
        });

        test('should return false if isTransient property is missing', done => {
            mongoMock.getObject
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, 'v-p-3'))
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, { locations: { 'loc-C': {} } }));

            worker.pensieveLocationIsTransient('loc-C', (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(false);
                done();
            });
        });

        test('should propagate error from first getObject call', done => {
            const testError = new Error('getObject overlay-version failed');
            mongoMock.getObject.mockImplementationOnce((b, k, p, l, cb) => cb(testError));

            worker.pensieveLocationIsTransient('loc-D', (err, isTransient) => {
                expect(err).toBe(testError);
                expect(isTransient).toBeUndefined();
                done();
            });
        });

        test('should propagate error from second getObject call', done => {
            const testError = new Error('getObject overlay-config failed');
            mongoMock.getObject
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, 'v-p-4'))
                .mockImplementationOnce((b, k, p, l, cb) => cb(testError));

            worker.pensieveLocationIsTransient('loc-E', (err, isTransient) => {
                expect(err).toBe(testError);
                expect(isTransient).toBeUndefined();
                done();
            });
        });

        test('should handle case when locConstraint is not in locations', done => {
            mongoMock.getObject
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, 'v-p-5'))
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, { locations: { 'other-loc': { isTransient: true } } }));

            worker.pensieveLocationIsTransient('loc-F-not-in-response', (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(false);
                done();
            });
        });

        test('should handle case where no location is provided', done => {
            mongoMock.getObject
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, 'v-p-6'))
                .mockImplementationOnce((b, k, p, l, cb) => cb(null, { locations: {} }));

            worker.pensieveLocationIsTransient('loc-G', (err, isTransient) => {
                expect(err).toBeNull();
                expect(isTransient).toBe(false);
                done();
            });
        });
    });
});
