const { BucketInfo } = require('arsenal').models;

const CountManager = require('../../../CountItems/CountManager');

const DummyLogger = require('../../mocks/DummyLogger');
const CountWorkerObj = require('../../mocks/CountWorkerObj');

const { testBucketMD } = require('../../constants');

const createWorkers = numWorkers => {
    const workers = {};
    for (let i = 0; i < numWorkers; ++i) {
        workers[i] = new CountWorkerObj(i);
    }
    return workers;
};

describe('CountItems::CountManager', () => {
    test('should setup then pause queue on start', () => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        expect(m.q.paused).toBeTruthy();
    });

    test('should update store', () => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        expect(m.store).toEqual({
            objects: BigInt(0),
            versions: BigInt(0),
            buckets: 0,
            bucketList: [],
            dataManaged: {
                total: { curr: BigInt(0), prev: BigInt(0) },
                byLocation: {},
            },
            stalled: 0,
        });
        m._consolidateData({
            versions: BigInt(10),
            objects: BigInt(10),
            stalled: 10,
            dataManaged: {
                total: { curr: BigInt(100), prev: BigInt(100) },
                locations: { location1: { curr: BigInt(100), prev: BigInt(100) } },
            },
        });
        expect(m.store).toEqual({
            objects: BigInt(10),
            versions: BigInt(10),
            buckets: 0,
            bucketList: [],
            dataManaged: {
                total: { curr: BigInt(100), prev: BigInt(100) },
                byLocation: { location1: { curr: BigInt(100), prev: BigInt(100) } },
            },
            stalled: 10,
        });
    });

    test('should update dataMetrics', () => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        expect(m.dataMetrics).toEqual({
            account: {},
            bucket: {},
            location: {},
        });
        m._consolidateData({
            dataMetrics: {
                account: {
                    account1: {
                        objectCount: {
                            current: BigInt(10),
                            deleteMarker: BigInt(0),
                            nonCurrent: BigInt(10),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(1),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUUploads: BigInt(0),
                        },
                        usedCapacity: {
                            current: BigInt(100),
                            nonCurrent: BigInt(100),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(100),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUParts: BigInt(0),
                        },
                        locations: {
                            location1: {
                                objectCount: {
                                    current: BigInt(10),
                                    deleteMarker: BigInt(0),
                                    nonCurrent: BigInt(10),
                                    _currentCold: BigInt(0),
                                    _nonCurrentCold: BigInt(0),
                                    _currentRestored: BigInt(1),
                                    _currentRestoring: BigInt(0),
                                    _nonCurrentRestored: BigInt(0),
                                    _nonCurrentRestoring: BigInt(0),
                                    _incompleteMPUUploads: BigInt(0),
                                },
                                usedCapacity: {
                                    current: BigInt(100),
                                    nonCurrent: BigInt(100),
                                    _currentCold: BigInt(0),
                                    _nonCurrentCold: BigInt(0),
                                    _currentRestored: BigInt(100),
                                    _currentRestoring: BigInt(0),
                                    _nonCurrentRestored: BigInt(0),
                                    _nonCurrentRestoring: BigInt(0),
                                    _incompleteMPUParts: BigInt(0),
                                },
                            },
                        },
                    },
                },
                bucket: {
                    bucket1: {
                        objectCount: {
                            current: BigInt(10),
                            deleteMarker: BigInt(0),
                            nonCurrent: BigInt(10),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(1),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUUploads: BigInt(0),
                        },
                        usedCapacity: {
                            current: BigInt(100),
                            nonCurrent: BigInt(100),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(100),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUParts: BigInt(0),
                        },
                    },
                },
                location: {
                    location1: {
                        objectCount: {
                            current: BigInt(10),
                            deleteMarker: BigInt(0),
                            nonCurrent: BigInt(10),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(1),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUUploads: BigInt(0),
                        },
                        usedCapacity: {
                            current: BigInt(100),
                            nonCurrent: BigInt(100),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(100),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUParts: BigInt(0),
                        },
                    },
                },
            },
        });
        expect(m.dataMetrics).toEqual({
            account: {
                account1: {
                    objectCount: {
                        current: BigInt(11),
                        deleteMarker: BigInt(0),
                        nonCurrent: BigInt(10),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(1),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                    },
                    usedCapacity: {
                        current: BigInt(200),
                        nonCurrent: BigInt(100),
                        _inflightsPreScan: BigInt(0),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(100),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    },
                    locations: {
                        location1: {
                            objectCount: {
                                current: BigInt(11),
                                deleteMarker: BigInt(0),
                                nonCurrent: BigInt(10),
                                _currentCold: BigInt(0),
                                _nonCurrentCold: BigInt(0),
                                _currentRestored: BigInt(1),
                                _currentRestoring: BigInt(0),
                                _nonCurrentRestored: BigInt(0),
                                _nonCurrentRestoring: BigInt(0),
                                _incompleteMPUUploads: BigInt(0),
                            },
                            usedCapacity: {
                                current: BigInt(200),
                                nonCurrent: BigInt(100),
                                _inflightsPreScan: BigInt(0),
                                _currentCold: BigInt(0),
                                _nonCurrentCold: BigInt(0),
                                _currentRestored: BigInt(100),
                                _currentRestoring: BigInt(0),
                                _nonCurrentRestored: BigInt(0),
                                _nonCurrentRestoring: BigInt(0),
                                _incompleteMPUParts: BigInt(0),
                            },
                        },
                    },
                },
            },
            bucket: {
                bucket1: {
                    objectCount: {
                        current: BigInt(11),
                        deleteMarker: BigInt(0),
                        nonCurrent: BigInt(10),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(1),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                    },
                    usedCapacity: {
                        current: BigInt(200),
                        nonCurrent: BigInt(100),
                        _inflightsPreScan: BigInt(0),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(100),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    },
                },
            },
            location: {
                location1: {
                    objectCount: {
                        current: BigInt(11),
                        deleteMarker: BigInt(0),
                        nonCurrent: BigInt(10),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(1),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                    },
                    usedCapacity: {
                        current: BigInt(200),
                        nonCurrent: BigInt(100),
                        _inflightsPreScan: BigInt(0),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(100),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    },
                },
            },
        });
    });

    test('should update dataMetrics with inflights', () => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        expect(m.dataMetrics).toEqual({
            account: {},
            bucket: {},
            location: {},
        });
        m._consolidateData({
            dataMetrics: {
                account: {
                    account1: {
                        objectCount: {
                            current: BigInt(10),
                            deleteMarker: BigInt(0),
                            nonCurrent: BigInt(10),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(1),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUUploads: BigInt(0),
                        },
                        usedCapacity: {
                            current: BigInt(100),
                            nonCurrent: BigInt(100),
                            _inflightsPreScan: BigInt(1000),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(100),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUParts: BigInt(0),
                        },
                        locations: {
                            location1: {
                                objectCount: {
                                    current: BigInt(10),
                                    deleteMarker: BigInt(0),
                                    nonCurrent: BigInt(10),
                                    _currentCold: BigInt(0),
                                    _nonCurrentCold: BigInt(0),
                                    _currentRestored: BigInt(1),
                                    _currentRestoring: BigInt(0),
                                    _nonCurrentRestored: BigInt(0),
                                    _nonCurrentRestoring: BigInt(0),
                                    _incompleteMPUUploads: BigInt(0),
                                },
                                usedCapacity: {
                                    current: BigInt(100),
                                    nonCurrent: BigInt(100),
                                    _inflightsPreScan: BigInt(1000),
                                    _currentCold: BigInt(0),
                                    _nonCurrentCold: BigInt(0),
                                    _currentRestored: BigInt(100),
                                    _currentRestoring: BigInt(0),
                                    _nonCurrentRestored: BigInt(0),
                                    _nonCurrentRestoring: BigInt(0),
                                    _incompleteMPUParts: BigInt(0),
                                },
                            },
                        },
                    },
                },
                bucket: {
                    bucket1: {
                        objectCount: {
                            current: BigInt(10),
                            deleteMarker: BigInt(0),
                            nonCurrent: BigInt(10),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(1),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUUploads: BigInt(0),
                        },
                        usedCapacity: {
                            current: BigInt(100),
                            nonCurrent: BigInt(100),
                            _inflightsPreScan: BigInt(1000),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(100),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUParts: BigInt(0),
                        },
                    },
                },
                location: {
                    location1: {
                        objectCount: {
                            current: BigInt(10),
                            deleteMarker: BigInt(0),
                            nonCurrent: BigInt(10),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(1),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUUploads: BigInt(0),
                        },
                        usedCapacity: {
                            current: BigInt(100),
                            nonCurrent: BigInt(100),
                            _inflightsPreScan: BigInt(1000),
                            _currentCold: BigInt(0),
                            _nonCurrentCold: BigInt(0),
                            _currentRestored: BigInt(100),
                            _currentRestoring: BigInt(0),
                            _nonCurrentRestored: BigInt(0),
                            _nonCurrentRestoring: BigInt(0),
                            _incompleteMPUParts: BigInt(0),
                        },
                    },
                },
            },
        });
        expect(m.dataMetrics).toEqual({
            account: {
                account1: {
                    objectCount: {
                        current: BigInt(11),
                        deleteMarker: BigInt(0),
                        nonCurrent: BigInt(10),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(1),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                    },
                    usedCapacity: {
                        current: BigInt(200),
                        nonCurrent: BigInt(100),
                        _inflightsPreScan: BigInt(1000),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(100),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    },
                    locations: {
                        location1: {
                            objectCount: {
                                current: BigInt(11),
                                deleteMarker: BigInt(0),
                                nonCurrent: BigInt(10),
                                _currentCold: BigInt(0),
                                _nonCurrentCold: BigInt(0),
                                _currentRestored: BigInt(1),
                                _currentRestoring: BigInt(0),
                                _nonCurrentRestored: BigInt(0),
                                _nonCurrentRestoring: BigInt(0),
                                _incompleteMPUUploads: BigInt(0),
                            },
                            usedCapacity: {
                                current: BigInt(200),
                                nonCurrent: BigInt(100),
                                _inflightsPreScan: BigInt(1000),
                                _currentCold: BigInt(0),
                                _nonCurrentCold: BigInt(0),
                                _currentRestored: BigInt(100),
                                _currentRestoring: BigInt(0),
                                _nonCurrentRestored: BigInt(0),
                                _nonCurrentRestoring: BigInt(0),
                                _incompleteMPUParts: BigInt(0),
                            },
                        },
                    },
                },
            },
            bucket: {
                bucket1: {
                    objectCount: {
                        current: BigInt(11),
                        deleteMarker: BigInt(0),
                        nonCurrent: BigInt(10),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(1),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                    },
                    usedCapacity: {
                        current: BigInt(200),
                        nonCurrent: BigInt(100),
                        _inflightsPreScan: BigInt(1000),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(100),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    },
                },
            },
            location: {
                location1: {
                    objectCount: {
                        current: BigInt(11),
                        deleteMarker: BigInt(0),
                        nonCurrent: BigInt(10),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(1),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUUploads: BigInt(0),
                    },
                    usedCapacity: {
                        current: BigInt(200),
                        nonCurrent: BigInt(100),
                        _inflightsPreScan: BigInt(1000),
                        _currentCold: BigInt(0),
                        _nonCurrentCold: BigInt(0),
                        _currentRestored: BigInt(100),
                        _currentRestoring: BigInt(0),
                        _nonCurrentRestored: BigInt(0),
                        _nonCurrentRestoring: BigInt(0),
                        _incompleteMPUParts: BigInt(0),
                    },
                },
            },
        });
    });

    test('should add tasks to queue', () => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        const bucketList = {
            bucketCount: 10,
            bucketInfos: Array(10)
                .map(() => BucketInfo.deSerialize(testBucketMD)),
        };
        m.addWork(bucketList);
        expect(m.q.length()).toEqual(10);
        expect(m.q.paused).toBeTruthy();
    });

    test('should only allow queue to be started once', done => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        const bucketList = {
            bucketCount: 10,
            bucketInfos: Array(10)
                .map(() => BucketInfo.deSerialize(testBucketMD)),
        };
        m.addWork(bucketList);
        const testCB = jest.fn();
        m.start(testCB);
        m.start(err => {
            expect(testCB).toHaveBeenCalledTimes(0);
            expect(err).toEqual(new Error('countInProgress'));
            return done();
        });
    });

    test('should pause/empty queue on error', done => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        const bucketList = {
            bucketCount: 1,
            bucketInfos: Array(1)
                .map(() => BucketInfo.deSerialize(testBucketMD)),
        };
        m.addWork(bucketList);
        m.start(err => {
            expect(err).toEqual(new Error('test error'));
            expect(m.q.paused).toBeTruthy();
            expect(m.q.length()).toEqual(0);
            done();
        });
        setTimeout(() => {
            workers[0].callbacks[0](new Error('test error'));
        }, 100);
    });

    test('should pause/empty queue on completion', done => {
        const workers = createWorkers(1);
        const m = new CountManager({
            log: new DummyLogger(),
            workers,
            maxConcurrent: 1,
        });
        const bucketList = {
            bucketCount: 1,
            bucketInfos: Array(1)
                .map(() => BucketInfo.deSerialize(testBucketMD)),
        };
        m.addWork(bucketList);
        m.start(err => {
            expect(err).toBeFalsy();
            expect(m.q.paused).toBeTruthy();
            expect(m.q.length()).toEqual(0);
            done();
        });
        setTimeout(() => {
            workers[0].callbacks[0]();
        }, 100);
    });

    test(
        'should remove/queue worker for each count task',
        done => {
            const workers = createWorkers(1);
            const m = new CountManager({
                log: new DummyLogger(),
                workers,
                maxConcurrent: 2,
            });
            const bucketList = {
                bucketCount: 1,
                bucketInfos: Array(1)
                    .map(() => BucketInfo.deSerialize(testBucketMD)),
            };
            m.addWork(bucketList);
            m.start(err => {
                expect(err).toBeFalsy();
                expect(m.workerList.length).toEqual(2);
                expect(m.q.paused).toBeTruthy();
                done();
            });
            setTimeout(() => {
                expect(m.workerList.length).toEqual(1);
                expect(m.q.running()).toEqual(1);
                workers[0].callbacks[0]();
            }, 100);
        },
    );
});
