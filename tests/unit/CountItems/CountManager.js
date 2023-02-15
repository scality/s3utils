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
            objects: 0,
            versions: 0,
            buckets: 0,
            bucketList: [],
            dataManaged: {
                total: { curr: 0, prev: 0 },
                byLocation: {},
            },
            stalled: 0,
        });
        m._consolidateData({
            versions: 10,
            objects: 10,
            stalled: 10,
            dataManaged: {
                total: { curr: 100, prev: 100 },
                locations: { location1: { curr: 100, prev: 100 } },
            },
        });
        expect(m.store).toEqual({
            objects: 10,
            versions: 10,
            buckets: 0,
            bucketList: [],
            dataManaged: {
                total: { curr: 100, prev: 100 },
                byLocation: { location1: { curr: 100, prev: 100 } },
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
                        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                        usedCapacity: { current: 100, nonCurrent: 100 },
                        locations: {
                            location1: {
                                objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                                usedCapacity: { current: 100, nonCurrent: 100 },
                            },
                        },
                    },
                },
                bucket: {
                    bucket1: {
                        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                        usedCapacity: { current: 100, nonCurrent: 100 },
                    },
                },
                location: {
                    location1: {
                        objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                        usedCapacity: { current: 100, nonCurrent: 100 },
                    },
                },
            },
        });
        expect(m.dataMetrics).toEqual({
            account: {
                account1: {
                    objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                    usedCapacity: { current: 100, nonCurrent: 100 },
                    locations: {
                        location1: {
                            objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                            usedCapacity: { current: 100, nonCurrent: 100 },
                        },
                    },
                },
            },
            bucket: {
                bucket1: {
                    objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                    usedCapacity: { current: 100, nonCurrent: 100 },
                },
            },
            location: {
                location1: {
                    objectCount: { current: 10, deleteMarker: 0, nonCurrent: 10 },
                    usedCapacity: { current: 100, nonCurrent: 100 },
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
