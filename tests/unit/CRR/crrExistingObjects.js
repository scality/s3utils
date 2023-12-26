const werelogs = require('werelogs');
// Mock the entire werelogs module
const mockFatal = jest.fn();
const mockInfo = jest.fn();
const mockError = jest.fn();

jest.mock('werelogs', () => ({
    Logger: jest.fn().mockImplementation(() => ({
        fatal: mockFatal,
        info: mockInfo,
        error: mockError,
    })),
    configure: jest.fn(),
}));

const mockCrrRun = jest.fn(cb => cb(null)); // Simulate successful run
const mockCrrStop = jest.fn();

jest.mock('../../../CRR/ReplicationStatusUpdater', () => jest.fn().mockImplementation(() => ({
    run: mockCrrRun,
    stop: mockCrrStop,
})));

describe('crrExistingObjects', () => {
    let originalEnv;

    beforeAll(() => {
        process.exit = jest.fn();
    });

    beforeEach(() => {
        // Save the original process.env
        originalEnv = { ...process.env };

        // Reset the argv array to its default state
        process.argv = ['node', 'yourscript.js'];
        // Reset the mock to clear previous call history
        process.exit.mockReset();

        // Clear the mocks before each test
        // ReplicationStatusUpdater.mockClear();
        mockFatal.mockClear();
        mockInfo.mockClear();
        mockError.mockClear();
        mockCrrRun.mockClear();
        mockCrrStop.mockClear();

        // Clear the module cache before each test
        jest.resetModules();
    });

    afterEach(() => {
        // Restore the original process.env after each test
        process.env = originalEnv;
    });

    afterAll(() => {
        // Restore the original process.exit
        process.exit = jest.requireActual('process').exit;
    });

    test('should run successfully', () => {
        process.argv[2] = 'bucket1,bucket2';

        process.env.TARGET_REPLICATION_STATUS = 'NEW,PENDING';
        process.env.WORKERS = '10';
        process.env.ACCESS_KEY = 'testAccessKey';
        process.env.SECRET_KEY = 'testSecretKey';
        process.env.ENDPOINT = 'http://fake.endpoint';
        process.env.SITE_NAME = 'testSite';
        process.env.STORAGE_TYPE = 'testStorage';
        process.env.TARGET_PREFIX = 'testPrefix';
        process.env.LISTING_LIMIT = '2000';
        process.env.MAX_UPDATES = '100';
        process.env.MAX_SCANNED = '1000';
        process.env.KEY_MARKER = 'testKeyMarker';
        process.env.VERSION_ID_MARKER = 'testVersionIdMarker';
        process.env.DEBUG = '0';

        require('../../../crrExistingObjects');

        const ReplicationStatusUpdater = require('../../../CRR/ReplicationStatusUpdater');

        expect(ReplicationStatusUpdater).toHaveBeenCalledTimes(1);
        expect(ReplicationStatusUpdater).toHaveBeenCalledWith({
            buckets: ['bucket1', 'bucket2'],
            replicationStatusToProcess: ['NEW', 'PENDING'],
            workers: 10,
            accessKey: 'testAccessKey',
            secretKey: 'testSecretKey',
            endpoint: 'http://fake.endpoint',
            siteName: 'testSite',
            storageType: 'testStorage',
            targetPrefix: 'testPrefix',
            listingLimit: 2000,
            maxUpdates: 100, // or a more specific expectation
            maxScanned: 1000,
            keyMarker: 'testKeyMarker',
            versionIdMarker: 'testVersionIdMarker',
        }, expect.anything());

        expect(mockFatal).not.toHaveBeenCalled();
        expect(mockError).not.toHaveBeenCalled();
        expect(process.exit).not.toHaveBeenCalled();
        expect(mockCrrRun).toHaveBeenCalled();
    });

    test('should set the default parameter when unspecified', () => {
        process.argv[2] = 'bucket1,bucket2';

        process.env.WORKERS = '10';
        process.env.ACCESS_KEY = 'testAccessKey';
        process.env.SECRET_KEY = 'testSecretKey';
        process.env.ENDPOINT = 'http://fake.endpoint';

        require('../../../crrExistingObjects');

        const ReplicationStatusUpdater = require('../../../CRR/ReplicationStatusUpdater');

        expect(ReplicationStatusUpdater).toHaveBeenCalledTimes(1);
        expect(ReplicationStatusUpdater).toHaveBeenCalledWith({
            buckets: ['bucket1', 'bucket2'],
            replicationStatusToProcess: ['NEW'],
            workers: 10,
            accessKey: 'testAccessKey',
            secretKey: 'testSecretKey',
            endpoint: 'http://fake.endpoint',
            storageType: '',
            targetPrefix: undefined,
            listingLimit: 1000,
            maxUpdates: undefined,
            maxScanned: undefined,
            keyMarker: undefined,
            versionIdMarker: undefined,
        }, expect.anything());

        expect(mockFatal).not.toHaveBeenCalled();
        expect(mockError).not.toHaveBeenCalled();
        expect(process.exit).not.toHaveBeenCalled();
        expect(mockCrrRun).toHaveBeenCalled();
    });

    test('should exit if no bucket is provided', () => {
        process.argv[2] = '';
        process.env.ENDPOINT = 'http://example.com';
        process.env.ACCESS_KEY = 'accesskey';
        process.env.SECRET_KEY = 'secretkey';

        require('../../../crrExistingObjects');

        expect(mockFatal).toHaveBeenCalledWith('No buckets given as input! Please provide a comma-separated list of buckets');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    test('should exit if no endpoint is provided', () => {
        process.argv[2] = 'bucket0';
        process.env.ACCESS_KEY = 'accesskey';
        process.env.SECRET_KEY = 'secretkey';

        require('../../../crrExistingObjects');

        expect(mockFatal).toHaveBeenCalledWith('ENDPOINT not defined!');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    test('should exit if no access key is provided', () => {
        process.argv[2] = 'bucket0';
        process.env.ENDPOINT = 'http://example.com';
        process.env.SECRET_KEY = 'secretkey';

        require('../../../crrExistingObjects');

        expect(mockFatal).toHaveBeenCalledWith('ACCESS_KEY not defined');
        expect(process.exit).toHaveBeenCalledWith(1);
    });

    test('should exit if no secret key is provided', () => {
        process.argv[2] = 'bucket0';
        process.env.ENDPOINT = 'http://example.com';
        process.env.ACCESS_KEY = 'accesskey';

        require('../../../crrExistingObjects');

        expect(mockFatal).toHaveBeenCalledWith('SECRET_KEY not defined');
        expect(process.exit).toHaveBeenCalledWith(1);
    });
});
