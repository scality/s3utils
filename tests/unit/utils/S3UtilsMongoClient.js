const sinon = require('sinon');
const async = require('async');
const util = require('util');
const assert = require('assert');
const werelogs = require('werelogs');
const { BucketInfo, ObjectMD, ObjectMDArchive } = require('arsenal').models;
const { MongoMemoryReplSet } = require('mongodb-memory-server');
const { constants, errors } = require('arsenal');
const S3UtilsMongoClient = require('../../../utils/S3UtilsMongoClient');
const {
    mongoMemoryServerParams,
    createMongoParamsFromMongoMemoryRepl,
} = require('../../utils/mongoUtils');
const getLocationConfig = require('../../../utils/locationConfig');
const {
    testBucketMD, testAccountCanonicalId, testBucketCreationDate, testUserBucketInfo,
} = require('../../constants');

const logger = new werelogs.Logger('S3UtilsMongoClient', 'debug', 'debug');
const USERSBUCKET = '__usersbucket';

const mongoTestClient = new S3UtilsMongoClient({});

describe('S3UtilsMongoClient::_handleResults', () => {
    const testInput = {
        bucket: {
            bucket1: {
                masterCount: 2n,
                masterData: 20n,
                nullCount: 2n,
                nullData: 20n,
                versionCount: 4n,
                versionData: 40n,
                deleteMarkerCount: 2n,
            },
        },
        location: {
            location1: {
                masterCount: 1n,
                masterData: 10n,
                nullCount: 1n,
                nullData: 10n,
                versionCount: 2n,
                versionData: 20n,
                deleteMarkerCount: 1n,
            },
            location2: {
                masterCount: 1n,
                masterData: 10n,
                nullCount: 1n,
                nullData: 10n,
                versionCount: 2n,
                versionData: 20n,
                deleteMarkerCount: 1n,
            },
        },
        account: {
            account1: {
                masterCount: 2n,
                masterData: 20n,
                nullCount: 2n,
                nullData: 20n,
                versionCount: 4n,
                versionData: 40n,
                deleteMarkerCount: 2n,
            },
        },
    };
    it('should return zero-result when input is empty', () => {
        const testInputEmpty = {
            bucket: {},
            location: {},
            account: {},
        };
        const testResults = mongoTestClient._handleResults(testInputEmpty, true);
        const expectedRes = {
            versions: 0,
            objects: 0,
            dataManaged: {
                total: { curr: 0, prev: 0 },
                locations: {},
            },
            dataMetrics: {
                bucket: {},
                location: {},
                account: {},
            },
        };
        assert.deepStrictEqual(testResults, expectedRes);
    });

    it('should return zero-result when input metric keys are not valid', () => {
        const testInputWithInvalidMetricKeys = {
            InvalidMetric0: testInput.bucket,
            InvalidMetric1: testInput.location,
            InvalidMetric2: testInput.account,
        };
        const testResults = mongoTestClient._handleResults(testInputWithInvalidMetricKeys, true);
        const expectedRes = {
            versions: 0,
            objects: 0,
            dataManaged: {
                total: { curr: 0, prev: 0 },
                locations: {},
            },
            dataMetrics: {
                bucket: {},
                location: {},
                account: {},
            },
        };
        assert.deepStrictEqual(testResults, expectedRes);
    });

    it('should return correct value if isVer is false', () => {
        const testResults = mongoTestClient._handleResults(testInput, false);
        const expectedRes = {
            versions: 0,
            objects: 4,
            dataManaged: {
                total: { curr: 40, prev: 0 },
                locations: {
                    location1: { curr: 20, prev: 0 },
                    location2: { curr: 20, prev: 0 },
                },
            },
            dataMetrics: {
                bucket: {
                    bucket1: {
                        objectCount: {
                            current: 4n,
                            deleteMarker: 0n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 40n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                        locations: {
                            location1: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 0n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                            location2: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 0n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                        },
                    },
                },
                location: {
                    location1: {
                        objectCount: {
                            current: 2n,
                            deleteMarker: 0n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 20n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                    },
                    location2: {
                        objectCount: {
                            current: 2n,
                            deleteMarker: 0n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 20n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                    },
                },
                account: {
                    account1: {
                        objectCount: {
                            current: 4n,
                            deleteMarker: 0n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 40n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                        locations: {
                            location1: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 0n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                            location2: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 0n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                        },
                    },
                },
            },
        };
        assert.deepStrictEqual(testResults, expectedRes);
    });

    it('should return correct value if isVer is true', () => {
        const testResults = mongoTestClient._handleResults(testInput, true);
        const expectedRes = {
            versions: 0,
            objects: 4,
            dataManaged: {
                total: { curr: 40, prev: 20 },
                locations: {
                    location1: { curr: 20, prev: 10 },
                    location2: { curr: 20, prev: 10 },
                },
            },
            dataMetrics: {
                bucket: {
                    bucket1: {
                        objectCount: {
                            current: 4n,
                            deleteMarker: 2n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 40n,
                            nonCurrent: 20n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                        locations: {
                            location1: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 1n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 10n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                            location2: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 1n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 10n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                        },
                    },
                },
                location: {
                    location1: {
                        objectCount: {
                            current: 2n,
                            deleteMarker: 1n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 20n,
                            nonCurrent: 10n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                    },
                    location2: {
                        objectCount: {
                            current: 2n,
                            deleteMarker: 1n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 20n,
                            nonCurrent: 10n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                    },
                },
                account: {
                    account1: {
                        objectCount: {
                            current: 4n,
                            deleteMarker: 2n,
                            nonCurrent: 0n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUUploads: 0n,
                        },
                        usedCapacity: {
                            current: 40n,
                            nonCurrent: 20n,
                            _currentCold: 0n,
                            _nonCurrentCold: 0n,
                            _currentRestored: 0n,
                            _currentRestoring: 0n,
                            _nonCurrentRestored: 0n,
                            _nonCurrentRestoring: 0n,
                            _incompleteMPUParts: 0n,
                        },
                        locations: {
                            location1: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 1n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 10n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                            location2: {
                                objectCount: {
                                    current: 2n,
                                    deleteMarker: 1n,
                                    nonCurrent: 0n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUUploads: 0n,
                                },
                                usedCapacity: {
                                    current: 20n,
                                    nonCurrent: 10n,
                                    _currentCold: 0n,
                                    _nonCurrentCold: 0n,
                                    _currentRestored: 0n,
                                    _currentRestoring: 0n,
                                    _nonCurrentRestored: 0n,
                                    _nonCurrentRestoring: 0n,
                                    _incompleteMPUParts: 0n,
                                },
                            },
                        },
                    },
                },
            },
        };
        assert.deepStrictEqual(testResults, expectedRes);
    });

    it('should calculate dataManaged based on input location metrics', () => {
        const testInputOnlyContainsLocation = {
            bucket: {},
            location: testInput.location,
            account: {},
        };
        const testResults = mongoTestClient._handleResults(testInputOnlyContainsLocation, true);
        const expectedRes = {
            dataManaged: {
                total: { curr: 40, prev: 20 },
                locations: {
                    location1: { curr: 20, prev: 10 },
                    location2: { curr: 20, prev: 10 },
                },
            },
        };
        assert.deepStrictEqual(testResults.dataManaged, expectedRes.dataManaged);
    });

    it('should calculate total current and nonCurrent counts based on input bucket metrics', () => {
        const testInputOnlyContainsLocation = {
            bucket: testInput.bucket,
            location: {},
            account: {},
        };
        const testResults = mongoTestClient._handleResults(testInputOnlyContainsLocation, true);
        const expectedRes = {
            versions: 0, objects: 4,
        };
        assert.deepStrictEqual(testResults.versions, expectedRes.versions);
        assert.deepStrictEqual(testResults.objects, expectedRes.objects);
    });
});

describe('S3UtilsMongoClient::_processEntryData', () => {
    const testBucketName = 'testBucket';
    const objectMdTemp = {
        'replicationInfo': {
            status: 'PENDING',
            backends: [],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
            isNFS: null,
        },
        'transient': false,
        'dataStoreName': 'us-east-1',
        'content-length': 42,
        'versionId': '0123456789abcdefg',
        'owner-id': testAccountCanonicalId,
    };
    const bucketInfo = BucketInfo.fromObj({
        ...testBucketMD,
        _name: testBucketName,
    });
    const locationConfig = getLocationConfig(logger);
    const tests = [
        [
            'should add content-length to current dataStore but not replication destination '
            + 'if replication status != COMPLETED and transient == true',
            testBucketName,
            true,
            {
                _id: 'testkey0',
                value: {
                    ...objectMdTemp,
                    replicationInfo: {
                        ...objectMdTemp.replicationInfo,
                        backends: [{
                            site: 'not-completed',
                            status: 'PENDING',
                        }],
                        status: 'PENDING',
                    },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: { 'us-east-1': 42 },
                },
            },
        ],
        [
            'should not add content-length to replication destination but not in current dataStore '
            + 'if replication status == COMPLETED and transient == true',
            testBucketName,
            true,
            {
                _id: 'testkey1',
                value: {
                    ...objectMdTemp,
                    replicationInfo: {
                        ...objectMdTemp.replicationInfo,
                        backends: [{
                            site: 'completed',
                            status: 'COMPLETED',
                        }],
                        status: 'COMPLETED',
                    },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: { completed: 42 },
                },
            },
        ],
        [
            'should add content-length to current dataStore but not replication destination '
            + 'if replication status != COMPLETED and transient == false',
            testBucketName,
            false,
            {
                _id: 'testkey2',
                value: {
                    ...objectMdTemp,
                    replicationInfo: {
                        ...objectMdTemp.replicationInfo,
                        backends: [{
                            site: 'not-completed',
                            status: 'PENDING',
                        }],
                        status: 'PENDING',
                    },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: { 'us-east-1': 42 },
                },
            },
        ],
        [
            'should add content-length to current dataStore and replication destination '
            + 'if replication status == COMPLETED and transient == false',
            testBucketName,
            false,
            {
                _id: 'testkey3',
                value: {
                    ...objectMdTemp,
                    replicationInfo: {
                        ...objectMdTemp.replicationInfo,
                        backends: [{
                            site: 'completed',
                            status: 'COMPLETED',
                        }],
                        status: 'COMPLETED',
                    },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: { 'us-east-1': 42, 'completed': 42 },
                },
            },
        ],
        [
            'should add content-length to each COMPLETED replication destination but not current dataStore '
            + '(object replication status: COMPLETED)',
            testBucketName,
            true,
            {
                _id: 'testkey4',
                value: {
                    ...objectMdTemp,
                    replicationInfo: {
                        ...objectMdTemp.replicationInfo,
                        status: 'COMPLETED',
                        backends: [
                            {
                                status: 'COMPLETED',
                                site: 'completed-1',
                            },
                            {
                                status: 'COMPLETED',
                                site: 'completed-2',
                            },
                            {
                                status: 'COMPLETED',
                                site: 'completed-3',
                            },
                        ],
                    },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: {
                        'completed-1': 42,
                        'completed-2': 42,
                        'completed-3': 42,
                    },
                },
            },
        ],
        [
            'should add content-length to each COMPLETED replications destination and current dataStore '
            + '(object replication status: PENDING)',
            testBucketName,
            true,
            {
                _id: 'testkey5',
                value: {
                    ...objectMdTemp,
                    replicationInfo: {
                        ...objectMdTemp.replicationInfo,
                        status: 'PENDING',
                        backends: [
                            {
                                status: 'PENDING',
                                site: 'not-completed',
                            },
                            {
                                status: 'COMPLETED',
                                site: 'completed-1',
                            },
                            {
                                status: 'COMPLETED',
                                site: 'completed-2',
                            },
                        ],
                    },
                    transient: true,
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: {
                        'us-east-1': 42,
                        'completed-1': 42,
                        'completed-2': 42,
                    },
                },
            },
        ],
        [
            'should only add content-length to cold location when object is archived',
            testBucketName,
            true,
            {
                _id: 'testkey6',
                value: {
                    ...objectMdTemp,
                    dataStoreName: 'cold-location',
                    replicationInfo: {
                        backends: [],
                    },
                    location: [{
                        key: 1,
                        size: 10,
                        start: 0,
                        dataStoreName: 'cold-location',
                        dataStoreETag: '1:6c840340c3c297ca02bce0900fcfd214',
                    }],
                    archive: { archiveInfo: {} },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: {
                        'cold-location': 42,
                    },
                },
            },
        ],
        [
            'should add content-length to cold storage location and current dataStore '
            + 'when object is restoring',
            testBucketName,
            true,
            {
                _id: 'testkey6',
                value: {
                    ...objectMdTemp,
                    dataStoreName: 'cold-location',
                    replicationInfo: {
                        backends: [],
                    },
                    archive: {
                        archiveInfo: {},
                        restoreRequestedAt: new Date(Date.now() - 1000),
                        restoreCompletedAt: null,
                        restoreWillExpireAt: null,
                    },
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: {
                        'us-east-1': 42,
                        'cold-location': 42,
                    },
                },
            },
        ],
        [
            'should add content-length to cold storage location and current dataStore '
            + 'when object is restored',
            testBucketName,
            true,
            {
                _id: 'testkey6',
                value: {
                    ...objectMdTemp,
                    'replicationInfo': {
                        backends: [],
                    },
                    'archive': {
                        archiveInfo: {},
                        restoreCompletedAt: new Date(Date.now() - 1000),
                        restoreWillExpireAt: new Date(Date.now() + 1000),
                    },
                    'x-amz-storage-class': 'cold-location',
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: {
                        'us-east-1': 42,
                        'cold-location': 42,
                    },
                },
            },
        ],
        [
            'should return error if content-length is invalid',
            testBucketName,
            true,
            {
                _id: 'testkey7',
                value: {
                    ...objectMdTemp,
                    'content-length': 'not-a-number',
                },
            },
            locationConfig,
            { error: new Error('invalid content length') },
        ],
        [
            'should correctly process entry with string typed content-length',
            testBucketName,
            true,
            {
                _id: 'testkey8',
                value: {
                    ...objectMdTemp,
                    'content-length': '42',
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: { 'us-east-1': 42 },
                },
            },
        ],
        [
            'should return error if bucketName is empty',
            undefined,
            true,
            {
                _id: 'testkey9',
                value: objectMdTemp,
            },
            locationConfig,
            { error: new Error('no bucket name provided') },
        ],
        [
            'should return error if locationConfig is empty',
            testBucketName,
            true,
            {
                _id: 'testkey10',
                value: objectMdTemp,
            },
            null,
            { error: new Error('empty locationConfig') },
        ],
        [
            'should ignore the location if bucket\'s location is not in locationConfig',
            testBucketName,
            true,
            {
                _id: 'testkey11',
                value: {
                    ...objectMdTemp,
                    dataStoreName: 'not-in-location-config',
                },
            },
            locationConfig,
            {
                data: {
                    account: { [testAccountCanonicalId]: 42 },
                    bucket: { [`${testBucketName}_${testBucketCreationDate}`]: 42 },
                    location: {},
                },
            },
        ],
        [
            'should ignore PHD',
            testBucketName,
            true,
            {
                _id: 'testkey12',
                value: {
                    versionId: '0123456789abcdefg',
                    isPHD: true,
                },
            },
            locationConfig,
            {},
        ],
    ];
    tests.forEach(([msg, bucketName, isTransient, params, locationConfig, expected]) => it(msg, () => {
        assert.deepStrictEqual(
            mongoTestClient._processEntryData(bucketName, bucketInfo, params, testBucketCreationDate, isTransient, locationConfig, {
                isCold: mongoTestClient._isObjectCold(params),
                isRestoring: mongoTestClient._isObjectRestoring(params),
                isRestored: mongoTestClient._isObjectRestored(params),
            }),
            expected,
        );
    }));
});

describe('S3UtilsMongoClient::_getUsersBucketCreationDates', () => {
    let client;
    let repl;
    beforeAll(async () => {
        repl = await MongoMemoryReplSet.create(mongoMemoryServerParams);
        client = new S3UtilsMongoClient({
            ...createMongoParamsFromMongoMemoryRepl(repl),
            logger,
        });
        const setupPromise = util.promisify(client.setup).bind(client);
        await setupPromise();
    });

    afterAll(done => async.series([
        next => client.close(next),
        next => repl.stop()
            .then(() => next())
            .catch(next),
    ], done));

    it('should return empty array when no users bucket', async () => {
        const testResults = await client._getUsersBucketCreationDates(logger);
        assert.deepStrictEqual(testResults, {});
    });

    it('should list existing buckets and return their creation dates', async () => {
        const bucketName = 'bucket1';
        await new Promise((resolve, reject) => {
            client.putObject(
                USERSBUCKET,
                `${testAccountCanonicalId}${constants.splitter}${bucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                },
            );
        });
        const testResults = await client._getUsersBucketCreationDates(logger);
        const expectedRes = [
            `${testAccountCanonicalId}${constants.splitter}${bucketName}`,
        ];
        assert.deepStrictEqual(Object.keys(testResults), expectedRes);
    });
});

function createBucket(client, bucketName, isVersioned, callback) {
    const bucketMD = BucketInfo.fromObj({
        ...testBucketMD,
        _name: bucketName,
        _versioningConfiguration: isVersioned
            ? { Status: 'Enabled' }
            : null,
    });
    client.createBucket(bucketName, bucketMD, logger, callback);
}

function uploadObjects(client, bucketName, objectList, callback) {
    async.eachSeries(objectList, (obj, done) => {
        const length = obj.contentLength >= 0 ? obj.contentLength : 100;
        const objMD = new ObjectMD()
            .setKey(obj.name)
            .setDataStoreName(obj.dataStore || 'us-east-1')
            .setContentLength(length)
            .setLastModified(obj.lastModified)
            .setOwnerId(obj.ownerId)
            .setIsNull(obj.isNull)
            .setArchive(obj.archive)
            .setIsDeleteMarker(obj.isDeleteMarker);
        if (obj.repInfo) {
            objMD.setReplicationInfo(obj.repInfo);
        }
        client.putObject(bucketName, obj.name, objMD.getValue(), {
            versionId: obj.versionId,
            versioning: obj.versioning,
        }, logger, done);
    }, callback);
}

describe('S3UtilsMongoClient, tests', () => {
    const hr = 1000 * 60 * 60;
    let client;
    let repl;
    beforeAll(async () => {
        repl = await MongoMemoryReplSet.create(mongoMemoryServerParams);
        client = new S3UtilsMongoClient({
            ...createMongoParamsFromMongoMemoryRepl(repl),
            logger,
        });
        await util.promisify(client.setup).bind(client)();
    });

    afterAll(done => async.series([
        next => client.close(next),
        next => repl.stop()
            .then(() => next())
            .catch(next),
        next => {
            sinon.restore();
            next();
        },
    ], done));

    const nonVersionedObjectMdTemp = {
        name: 'testkey',
        versioning: false,
        versionId: null,
        lastModified: new Date(Date.now()),
        ownerId: testAccountCanonicalId,
    };
    const objectMdTemp = {
        name: 'testkey',
        versioning: true,
        versionId: null,
        lastModified: new Date(Date.now()),
        ownerId: testAccountCanonicalId,
        repInfo: {
            status: 'COMPLETED',
            backends: [
                {
                    status: 'COMPLETED',
                    site: 'rep-loc-1',
                },
            ],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
            isNFS: null,
        },
    };

    const coldObjectMdTemp = {
        name: 'coldkey',
        versioning: true,
        versionId: null,
        lastModified: new Date(Date.now()),
        ownerId: testAccountCanonicalId,
        archive: new ObjectMDArchive({}),
        dataStore: 'cold-location',
    };

    const restoringObjectMdTemp = {
        name: 'restoringkey',
        versioning: true,
        versionId: null,
        lastModified: new Date(Date.now()),
        ownerId: testAccountCanonicalId,
        archive: new ObjectMDArchive({}, new Date(Date.now() - 5000), 10),
        dataStore: 'cold-location',
    };

    const restoredObjectMdTemp = {
        name: 'restoredkey',
        versioning: true,
        versionId: null,
        lastModified: new Date(Date.now()),
        ownerId: testAccountCanonicalId,
        archive: new ObjectMDArchive(
            {},
            new Date(Date.now() - 5000),
            10,
            new Date(Date.now() - 1000),
            new Date(Date.now() + 10000),
        ),
        dataStore: 'us-east-1',
    };

    const tests = [
        [
            'getObjectMDStats() should return zero-result when no objects in the bucket',
            {
                bucketName: 'test-bucket',
                isVersioned: false,
                objectList: [],
            },
            {
                dataManaged: {
                    locations: {},
                    total: { curr: 0, prev: 0 },
                },
                objects: 0,
                stalled: 0,
                versions: 0,
                dataMetrics: {
                    bucket: {},
                    location: {},
                    account: {},
                },
            },
        ],
        [
            'getObjectMDStats() should return correct results',
            {
                bucketName: 'test-bucket',
                isVersioned: true,
                objectList: [
                    // versioned object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // versioned object 2,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // stalled object 1
                    {
                        ...objectMdTemp,
                        versioning: true,
                        lastModified: new Date(Date.now() - hr),
                        repInfo: {
                            ...objectMdTemp.repInfo,
                            status: 'PENDING',
                            backends: [
                                {
                                    status: 'PENDING',
                                    site: 'rep-loc-1',
                                },
                            ],
                        },
                    },
                    // null versioned object
                    {
                        name: 'nullkey',
                        isNull: true,
                        ownerId: testAccountCanonicalId,
                        lastModified: new Date(Date.now() - hr),
                    },
                ],
            },
            {
                dataManaged: {
                    locations: {
                        'rep-loc-1': {
                            curr: 0,
                            prev: 200,
                        },
                        'us-east-1': {
                            curr: 200,
                            prev: 200,
                        },
                    },
                    total: {
                        curr: 200,
                        prev: 400,
                    },
                },
                objects: 2,
                stalled: 1,
                versions: 2,
                dataMetrics: {
                    account: {
                        [testAccountCanonicalId]: {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 200n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    bucket: {
                        [`test-bucket_${testBucketCreationDate}`]: {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 200n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    location: {
                        'rep-loc-1': {
                            objectCount: {
                                current: 0n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                        'us-east-1': {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                    },
                },
            },
        ],
        [
            'getObjectMDStats() should return correct results for non versioned bucket',
            {
                bucketName: 'test-bucket',
                isVersioned: false,
                objectList: [
                    // non versioned object 1,
                    {
                        ...nonVersionedObjectMdTemp,
                        name: 'testkey1',
                    },
                    // non versioned object 1,
                    {
                        ...nonVersionedObjectMdTemp,
                        name: 'testkey1',
                    },
                    // non versioned object 2
                    {
                        ...nonVersionedObjectMdTemp,
                        name: 'testkey2',
                    },
                ],
            },
            {
                dataManaged: {
                    locations: {
                        'us-east-1': {
                            curr: 200,
                            prev: 0,
                        },
                    },
                    total: {
                        curr: 200,
                        prev: 0,
                    },
                },
                objects: 2,
                stalled: 0,
                versions: 0,
                dataMetrics: {
                    account: {
                        [testAccountCanonicalId]: {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 0n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 200n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    bucket: {
                        [`test-bucket_${testBucketCreationDate}`]: {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 0n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 200n,
                                        nonCurrent: 0n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    location: {
                        'us-east-1': {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 0n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                    },
                },
            },
        ],
        [
            'getObjectMDStats() should return correct results for versioned bucket',
            {
                bucketName: 'test-bucket',
                isVersioned: true,
                objectList: [
                    // a version of object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // a version of object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // deleteMarker of object 1
                    {
                        ...objectMdTemp,
                        versioning: true,
                        isDeleteMarker: true,
                    },
                    // a version of object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                        repInfo: {
                            ...objectMdTemp.repInfo,
                            status: 'PENDING',
                            backends: [
                                {
                                    status: 'PENDING',
                                    site: 'rep-loc-1',
                                },
                            ],
                        },
                    },
                ],
            },
            {
                dataManaged: {
                    locations: {
                        'rep-loc-1': {
                            curr: 0,
                            prev: 300,
                        },
                        'us-east-1': {
                            curr: 100,
                            prev: 300,
                        },
                    },
                    total: {
                        curr: 100,
                        prev: 600,
                    },
                },
                objects: 1,
                stalled: 0,
                versions: 2,
                dataMetrics: {
                    account: {
                        [testAccountCanonicalId]: {
                            objectCount: {
                                current: 1n,
                                deleteMarker: 1n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 100n,
                                nonCurrent: 300n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 300n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 1n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 100n,
                                        nonCurrent: 300n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    bucket: {
                        [`test-bucket_${testBucketCreationDate}`]: {
                            objectCount: {
                                current: 1n,
                                deleteMarker: 1n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 100n,
                                nonCurrent: 300n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 300n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 1n,
                                        deleteMarker: 1n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 100n,
                                        nonCurrent: 300n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    location: {
                        'rep-loc-1': {
                            objectCount: {
                                current: 0n,
                                deleteMarker: 1n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                nonCurrent: 300n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                        'us-east-1': {
                            objectCount: {
                                current: 1n,
                                deleteMarker: 1n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 100n,
                                nonCurrent: 300n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                    },
                },
            },
        ],
        [
            'getObjectMDStats() should return correct results for versioned bucket with objects in cold storage, restoring and restored',
            {
                bucketName: 'test-bucket',
                isVersioned: true,
                objectList: [
                    // a version of object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // deleteMarker of object 1
                    {
                        ...objectMdTemp,
                        versioning: true,
                        isDeleteMarker: true,
                        contentLength: 0,
                    },
                    // a cold object
                    {
                        ...coldObjectMdTemp,
                        versioning: true,
                    },
                    // a version for the cold object
                    {
                        ...coldObjectMdTemp,
                        versioning: true,
                    },
                    // a restoring object, still part of cold location
                    {
                        ...restoringObjectMdTemp,
                        versioning: true,
                    },
                    // a restored object: part of current data in us-east-1 location
                    {
                        ...restoredObjectMdTemp,
                        versioning: true,
                    },

                    // a version of object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                        repInfo: {
                            ...objectMdTemp.repInfo,
                            status: 'PENDING',
                            backends: [
                                {
                                    status: 'PENDING',
                                    site: 'rep-loc-1',
                                },
                            ],
                        },
                    },
                ],
            },
            {
                dataManaged: {
                    locations: {
                        'rep-loc-1': {
                            curr: 0,
                            prev: 100,
                        },
                        'us-east-1': {
                            curr: 300,
                            prev: 100,
                        },
                        'cold-location': {
                            curr: 200,
                            prev: 100,
                        },
                    },
                    total: {
                        curr: 500,
                        prev: 300,
                    },
                },
                objects: 4,
                stalled: 0,
                versions: 2,
                dataMetrics: {
                    account: {
                        [testAccountCanonicalId]: {
                            objectCount: {
                                current: 1n,
                                _currentCold: 1n,
                                deleteMarker: 1n,
                                nonCurrent: 1n,
                                _nonCurrentCold: 1n,
                                _currentRestored: 1n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 100n,
                                _currentCold: 100n,
                                nonCurrent: 100n,
                                _nonCurrentCold: 100n,
                                _currentRestored: 100n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 1n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 100n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 1n,
                                        _currentCold: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 1n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 100n,
                                        _currentCold: 0n,
                                        nonCurrent: 100n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 100n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'cold-location': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 1n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 1n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 100n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 100n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    bucket: {
                        [`test-bucket_${testBucketCreationDate}`]: {
                            objectCount: {
                                current: 1n,
                                _currentCold: 1n,
                                deleteMarker: 1n,
                                nonCurrent: 1n,
                                _nonCurrentCold: 1n,
                                _currentRestored: 1n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 100n,
                                _currentCold: 100n,
                                nonCurrent: 100n,
                                _nonCurrentCold: 100n,
                                _currentRestored: 100n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 1n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 100n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 1n,
                                        _currentCold: 0n,
                                        deleteMarker: 1n,
                                        nonCurrent: 1n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 100n,
                                        _currentCold: 0n,
                                        nonCurrent: 100n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 100n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'cold-location': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 1n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 1n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 100n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 100n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    location: {
                        'rep-loc-1': {
                            objectCount: {
                                current: 0n,
                                _currentCold: 0n,
                                deleteMarker: 1n,
                                nonCurrent: 1n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                _currentCold: 0n,
                                nonCurrent: 100n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                        'us-east-1': {
                            objectCount: {
                                current: 1n,
                                _currentCold: 0n,
                                deleteMarker: 1n,
                                nonCurrent: 1n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 1n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 100n,
                                _currentCold: 0n,
                                nonCurrent: 100n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 100n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                        'cold-location': {
                            objectCount: {
                                current: 0n,
                                _currentCold: 1n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 1n,
                                _currentRestored: 0n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                _currentCold: 100n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 100n,
                                _currentRestored: 0n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                    },
                },
            },
        ],
        [
            'restoring and restored objects must be part of the destination location and not the cold location',
            {
                bucketName: 'test-bucket',
                isVersioned: true,
                objectList: [
                    // a restoring object, still part of cold location
                    {
                        ...restoringObjectMdTemp,
                        versioning: true,
                    },
                    // a restored object: part of current data in us-east-1 location
                    {
                        ...restoredObjectMdTemp,
                        versioning: true,
                    },
                ],
            },
            {
                dataManaged: {
                    locations: {
                        'us-east-1': {
                            curr: 200,
                            prev: 0,
                        },
                        'cold-location': {
                            curr: 100,
                            prev: 0,
                        },
                    },
                    total: {
                        curr: 300,
                        prev: 0,
                    },
                },
                objects: 2,
                stalled: 0,
                versions: 0,
                dataMetrics: {
                    account: {
                        [testAccountCanonicalId]: {
                            objectCount: {
                                current: 0n,
                                _currentCold: 0n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 1n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                _currentCold: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 100n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'cold-location': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 100n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    bucket: {
                        [`test-bucket_${testBucketCreationDate}`]: {
                            objectCount: {
                                current: 0n,
                                _currentCold: 0n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 1n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                _currentCold: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 100n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'cold-location': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 1n,
                                        _currentRestoring: 1n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        _currentCold: 0n,
                                        nonCurrent: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 100n,
                                        _currentRestoring: 100n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    location: {
                        'us-east-1': {
                            objectCount: {
                                current: 0n,
                                _currentCold: 0n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 1n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                _currentCold: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 100n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                        'cold-location': {
                            objectCount: {
                                current: 0n,
                                _currentCold: 0n,
                                deleteMarker: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 1n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                _currentCold: 0n,
                                nonCurrent: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 100n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                    },
                },
            },
        ],

        [
            'getObjectMDStats() should return correct results for buckets with inflights',
            {
                bucketName: 'test-bucket-inflights',
                isVersioned: true,
                inflights: 1000n,
                objectList: [
                    // versioned object 1,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // versioned object 2,
                    {
                        ...objectMdTemp,
                        versioning: true,
                    },
                    // stalled object 1
                    {
                        ...objectMdTemp,
                        versioning: true,
                        lastModified: new Date(Date.now() - hr),
                        repInfo: {
                            ...objectMdTemp.repInfo,
                            status: 'PENDING',
                            backends: [
                                {
                                    status: 'PENDING',
                                    site: 'rep-loc-1',
                                },
                            ],
                        },
                    },
                    // null versioned object
                    {
                        name: 'nullkey',
                        isNull: true,
                        ownerId: testAccountCanonicalId,
                        lastModified: new Date(Date.now() - hr),
                    },
                ],
            },
            {
                dataManaged: {
                    locations: {
                        'rep-loc-1': {
                            curr: 0,
                            prev: 200,
                        },
                        'us-east-1': {
                            curr: 200,
                            prev: 200,
                        },
                    },
                    total: {
                        curr: 200,
                        prev: 400,
                    },
                },
                objects: 2,
                stalled: 1,
                versions: 2,
                dataMetrics: {
                    account: {
                        [testAccountCanonicalId]: {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 200n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    bucket: {
                        [`test-bucket-inflights_${testBucketCreationDate}`]: {
                            accountOwnerID: 'd1d40abd2bd8250962f7f5774af1bbbeaec9b77a0853749d41ec46f142e66fe4',
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 200n,
                                _inflightsPreScan: 1000n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                            locations: {
                                'rep-loc-1': {
                                    objectCount: {
                                        current: 0n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 0n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                                'us-east-1': {
                                    objectCount: {
                                        current: 2n,
                                        deleteMarker: 0n,
                                        nonCurrent: 2n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUUploads: 0n,
                                    },
                                    usedCapacity: {
                                        current: 200n,
                                        nonCurrent: 200n,
                                        _currentCold: 0n,
                                        _nonCurrentCold: 0n,
                                        _currentRestored: 0n,
                                        _currentRestoring: 0n,
                                        _nonCurrentRestored: 0n,
                                        _nonCurrentRestoring: 0n,
                                        _incompleteMPUParts: 0n,
                                    },
                                },
                            },
                        },
                    },
                    location: {
                        'rep-loc-1': {
                            objectCount: {
                                current: 0n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 0n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                        'us-east-1': {
                            objectCount: {
                                current: 2n,
                                deleteMarker: 0n,
                                nonCurrent: 2n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUUploads: 0n,
                            },
                            usedCapacity: {
                                current: 200n,
                                nonCurrent: 200n,
                                _currentCold: 0n,
                                _nonCurrentCold: 0n,
                                _currentRestored: 0n,
                                _currentRestoring: 0n,
                                _nonCurrentRestored: 0n,
                                _nonCurrentRestoring: 0n,
                                _incompleteMPUParts: 0n,
                            },
                        },
                    },
                },
            },
        ],
    ];
    tests.forEach(([msg, testCase, expected]) => it(msg, done => {
        const {
            bucketName,
            isVersioned,
            objectList,
            inflights,
        } = testCase;
        return async.waterfall([
            next => createBucket(client, bucketName, isVersioned, err => next(err)),
            next => client.putObject(
                USERSBUCKET,
                `${testBucketMD._owner}${constants.splitter}${bucketName}`,
                testUserBucketInfo.value,
                {
                    versioning: false,
                    versionId: null,
                },
                logger,
                next,
            ),
            next => uploadObjects(client, bucketName, objectList, err => next(err)),
            next => client.getBucketAttributes(bucketName, logger, next),
            (bucketInfo, next) => {
                if (inflights) {
                    const mock = sinon.stub(client, 'readStorageConsumptionInflights');
                    mock.onFirstCall().returns(Promise.resolve(inflights));
                    mock.onSecondCall().returns(Promise.resolve((inflights * 3n) / 2n));
                }
                return client.getObjectMDStats(
                    bucketName,
                    BucketInfo.fromObj(bucketInfo),
                    false,
                    logger,
                    (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        assert.deepStrictEqual(res, expected);
                        return next();
                    },
                );
            },
            next => client.deleteBucket(bucketName, logger, next),
        ], done);
    }));
});

describe('S3UtilsMongoClient, update inflight deltas', () => {
    let client;
    let repl;

    const metrics = [
        {
            _id: 'bucket_bucket1_1715849127256',
            measuredOn: '2024-05-17T16:08:04.113Z',
            accountOwnerID: '1234',
            usedCapacity: {
                current: 1000n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUParts: 0n,
                _inflightsPreScan: 100n,
                _inflight: 100n,
            },
            objectCount: {
                current: 10n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUUploads: 0n,
                deleteMarker: 0n,
            },
        },
        {
            _id: 'bucket_bucket2_1715849127257',
            measuredOn: '2024-05-17T16:08:04.113Z',
            accountOwnerID: '1234',
            usedCapacity: {
                current: 1000n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUParts: 0n,
                _inflightsPreScan: 1500n,
                _inflight: 1500n,
            },
            objectCount: {
                current: 10n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUUploads: 0n,
                deleteMarker: 0n,
            },
        },
        {
            _id: 'account_1234',
            measuredOn: '2024-05-17T16:08:04.113Z',
            usedCapacity: {
                current: 2000n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUParts: 0n,
                _inflight: 0n,
            },
            objectCount: {
                current: 10n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUUploads: 0n,
                deleteMarker: 0n,
            },
        },
    ];

    beforeAll(async () => {
        repl = await MongoMemoryReplSet.create(mongoMemoryServerParams);
        client = new S3UtilsMongoClient({
            ...createMongoParamsFromMongoMemoryRepl(repl),
            logger,
        });
        await util.promisify(client.setup).bind(client)();
    });

    afterEach(done => {
        sinon.restore();
        done();
    });

    afterAll(done => async.series([
        next => client.close(next),
        next => repl.stop()
            .then(() => next())
            .catch(next),
    ], done));

    it('should do nothing if the metrics are empty', async () => {
        const input = [];
        const output = await client.updateInflightDeltas(input, logger);
        assert.strictEqual(input, output);
    });

    it('should return the metrics in case of error', async () => {
        sinon.stub(client, 'getCollection').rejects(errors.InternalError);
        const output = await client.updateInflightDeltas(metrics, logger);
        assert.equal(metrics, output);
    });

    it('should properly compute the inflights deltas', async () => {
        sinon.stub(client, 'getCollection').returns({
            find: async () => ({
                toArray: async () => [
                    {
                        _id: 'bucket_bucket1_1715849127256',
                        usedCapacity: {
                            _inflight: 3000n,
                        },
                    },
                    {
                        _id: 'bucket_bucket2_1715849127257',
                        usedCapacity: {
                            _inflight: 5000n,
                        },
                    },
                ],
                close: async () => {},
            }),
        });
        const output = await client.updateInflightDeltas([
            ...metrics,
        ], logger);
        // first bucket: 1000 current + (3000 post scan - 100 pre scan) = 3900
        assert.strictEqual(output[0].usedCapacity.current, 3900n);
        // second bucket: 1000 current + (5000 post scan - 1500 pre scan) = 4500
        assert.strictEqual(output[1].usedCapacity.current, 4500n);
        // for account, we have the current and the sum of bucket's inflight deltas
        // as they belong to this account: 2000 + 2900 + 3500
        assert.strictEqual(output[2].usedCapacity.current, 8400n);
    });
});

describe('S3UtilsMongoClient, cold object helpers', () => {
    const coldObjectMdTemp = {
        name: 'coldkey',
        versioning: true,
        versionId: null,
        lastModified: new Date(Date.now()),
        ownerId: testAccountCanonicalId,
        archive: new ObjectMDArchive({}),
        dataStore: 'cold-location',
    };

    it('should detect a cold object', () => {
        const coldObject = {
            value: {
                ...coldObjectMdTemp,
                dataStoreName: 'cold-location',
            },
        };
        assert.strictEqual(mongoTestClient._isObjectCold(coldObject), true);
    });

    it('should detect a restoring object', () => {
        const restoringObject = {
            value: {
                ...coldObjectMdTemp,
                archive: new ObjectMDArchive({}, new Date(Date.now() - 5000), 10),
                dataStoreName: 'cold-location',
            },
        };
        assert.strictEqual(mongoTestClient._isObjectRestoring(restoringObject), true);
    });

    it('should detect a restored object', () => {
        const restoredObject = {
            value: {
                ...coldObjectMdTemp,
                archive: new ObjectMDArchive(
                    {},
                    new Date(Date.now() - 5000),
                    10,
                    new Date(Date.now() - 1000),
                    new Date(Date.now() + 10000),
                ),
                dataStoreName: 'us-east-1',
            },
        };
        assert.strictEqual(mongoTestClient._isObjectRestored(restoredObject), true);
    });
});
