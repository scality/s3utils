const werelogs = require('werelogs');

const {
    StalledRequestHandler,
} = require('../../../StalledRetry/StalledRequestHandler');

const {
    generateRequestArray,
} = require('../../utils');

const loggerConfig = {
    level: 'debug',
    dump: 'error',
};

werelogs.configure(loggerConfig);

const log = new werelogs.Logger('S3Utils::StalledRetry::StalledRequestHandler');

const mockClient = {
    retryFailedObjects: jest.fn(),
};

const mockSource = {
    getBatch: jest.fn(),
    getInfo: jest.fn(),
};


const mockSourceGetBatchResponses = respList => {
    respList.forEach(resp => {
        mockSource.getBatch.mockImplementationOnce((_, cb) => cb(...resp));
    });
};

describe('StalledRequestHandler', () => {
    let handler;

    beforeEach(() => {
        jest.resetModules();
        mockClient.retryFailedObjects.mockReset();
        mockSource.getBatch.mockReset();
        mockSource.getInfo.mockReset();

        handler = new StalledRequestHandler(mockClient, {
            dryRun: false,
            batchSize: 10,
            concurrentRequests: 5,
            log,
        });
    });

    afterEach(() => {
        handler.kill();
    });

    describe('::handleRequests', () => {
        test('error if called using a terminated handler', done => {
            mockSource.getBatch.mockImplementation((_, cb) => cb(null, null));

            handler.kill();
            handler.handleRequests(mockSource, err => {
                expect(err).toEqual(new Error('terminated handler'));
                done();
            });
        });

        test('error if source is missing the getBatch method', done => {
            handler = new StalledRequestHandler({}, {
                dryRun: false,
                batchSize: 10,
                concurrentRequests: 5,
                log,
            });

            handler.handleRequests({}, err => {
                expect(err).toEqual(new Error('Invalid cursor source'));
                done();
            });
        });

        test('should return error from queue', done => {
            mockClient.retryFailedObjects.mockImplementation((param, cb) => cb(new Error('test client error')));
            mockSourceGetBatchResponses([
                [null, generateRequestArray('bucket', 'key', 'loc-1')],
                [null, generateRequestArray('bucket', 'key', 'loc-2')],
            ]);
            mockSource.getBatch.mockImplementation((_, cb) => cb(null, null));

            handler.handleRequests(mockSource, err => {
                expect(err).toEqual(new Error('test client error'));
                expect(handler.killed).toEqual(true);
                expect(handler.queueError)
                    .toEqual(new Error('test client error'));
                done();
            });
        });

        test('should wait until queue is completed', done => {
            mockClient.retryFailedObjects.mockImplementation((_, cb) => setTimeout(() => cb(), 500));
            mockSource.getInfo.mockImplementation(() => ({ stalled: 1 }));
            mockSourceGetBatchResponses([
                [null, generateRequestArray('bucket', 'key', 'loc-1')],
            ]);
            mockSource.getBatch.mockImplementation((_, cb) => cb(null, null));

            handler.handleRequests(mockSource, err => {
                expect(err).toBeNull();
                expect(handler.isInProgress()).toEqual(false);
                done();
            });

            // early check
            setTimeout(() => {
                expect(handler.isInProgress()).toEqual(true);
            }, 100);
        });

        test('should complete successfully', done => {
            mockClient.retryFailedObjects.mockImplementation((_, cb) => setTimeout(() => cb(), 500));
            mockSource.getInfo.mockImplementation(() => ({ stalled: 1 }));
            mockSourceGetBatchResponses([
                [null, generateRequestArray('bucket', 'key', 'loc-1')],
            ]);
            mockSource.getBatch.mockImplementation((_, cb) => cb(null, null));

            handler.handleRequests(mockSource, err => {
                expect(err).toBeNull();
                expect(handler.queueError).toEqual(null);
                done();
            });
        });
    });
});
