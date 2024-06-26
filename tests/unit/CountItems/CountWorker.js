const CountWorker = require('../../../CountItems/CountWorker');

const mongoMock = require('../../mocks/mongoClient');
const DummyLogger = require('../../mocks/DummyLogger');

const { testBucketMD } = require('../../constants');

describe('CountItems::CountWorker', () => {
    beforeEach(() => {
        mongoMock.close.mockReset();
        mongoMock.setup.mockReset();
        mongoMock.client.isConnected.mockReset();
        mongoMock._getIsTransient.mockReset();
        mongoMock.getObjectMDStats.mockReset();
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
        mongoMock.setup.mockImplementationOnce(cb => cb(...tc.mock.setup));
        mongoMock.close.mockImplementationOnce(cb => cb(...tc.mock.close));
        mongoMock.client.isConnected
            .mockImplementationOnce(() => tc.mock.isConnected);
        mongoMock._getIsTransient.mockImplementationOnce(
            (_a, _b, cb) => cb(...tc.mock.getIsTransient),
        );
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
        mongoMock.setup.mockImplementationOnce(cb => cb());
        mongoMock.close.mockImplementationOnce(cb => cb());
        mongoMock.client.isConnected.mockImplementationOnce(() => false);
        mongoMock._getIsTransient.mockImplementationOnce((_a, _b, cb) => cb(null, true));
        mongoMock.getObjectMDStats.mockImplementationOnce((_a, _b, _c, _d, cb) => cb(null, { value: 42 }));
        w.countItems(bucketInfo, (err, results) => {
            expect(err).toBeNull();
            expect(results).toEqual({ value: 42 });
            done();
        });
    });
});
