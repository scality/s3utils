const { consolidateDataMetrics } = require('../../../../CountItems/utils/utils');

describe('CountItems::utils::consolidateDataMetrics', () => {
    const zeroValueRes = {
        usedCapacity: {
            current: BigInt(0),
            nonCurrent: BigInt(0),
            _inflightsPreScan: BigInt(0),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(0),
        },
        objectCount: {
            current: BigInt(0),
            nonCurrent: BigInt(0),
            deleteMarker: BigInt(0),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(0),
        },
    };

    const example1 = {
        usedCapacity: {
            current: BigInt(10),
            nonCurrent: BigInt(10),
            _inflightsPreScan: BigInt(0),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(0),
        },
        objectCount: {
            current: BigInt(10),
            nonCurrent: BigInt(10),
            deleteMarker: BigInt(10),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(0),
        },
    };

    const example2 = {
        usedCapacity: {
            current: BigInt(20),
            nonCurrent: BigInt(20),
            _inflightsPreScan: BigInt(0),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(0),
        },
        objectCount: {
            current: BigInt(20),
            nonCurrent: BigInt(20),
            deleteMarker: BigInt(20),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(0),
        },
    };

    const exampleWithInflights = {
        usedCapacity: {
            current: BigInt(20),
            nonCurrent: BigInt(20),
            _inflightsPreScan: BigInt(1000),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(0),
        },
        objectCount: {
            current: BigInt(20),
            nonCurrent: BigInt(20),
            deleteMarker: BigInt(20),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(0),
        },
    };

    const expectedResponseWithInflights = {
        usedCapacity: {
            current: BigInt(40),
            nonCurrent: BigInt(40),
            _inflightsPreScan: BigInt(1000),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(0),
        },
        objectCount: {
            current: BigInt(40),
            nonCurrent: BigInt(40),
            deleteMarker: BigInt(40),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(0),
        },
    };

    const exampleWithMPU = {
        usedCapacity: {
            current: BigInt(20),
            nonCurrent: BigInt(20),
            _inflightsPreScan: BigInt(0),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(100),
        },
        objectCount: {
            current: BigInt(20),
            nonCurrent: BigInt(20),
            deleteMarker: BigInt(20),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(10),
        },
    };

    const expectedConsolidatedMPU = {
        usedCapacity: {
            current: BigInt(220),
            nonCurrent: BigInt(30),
            _inflightsPreScan: BigInt(0),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUParts: BigInt(200),
        },
        objectCount: {
            current: BigInt(40),
            nonCurrent: BigInt(21),
            deleteMarker: BigInt(21),
            _currentCold: BigInt(0),
            _nonCurrentCold: BigInt(0),
            _currentRestored: BigInt(0),
            _currentRestoring: BigInt(0),
            _nonCurrentRestored: BigInt(0),
            _nonCurrentRestoring: BigInt(0),
            _incompleteMPUUploads: BigInt(20),
        },
    };

    test('should return zero-value if target and source are both undefined', () => {
        const res = consolidateDataMetrics(undefined, undefined);
        expect(res).toEqual(zeroValueRes);
    });

    test('should return zero-value if target and source are both empty', () => {
        const res = consolidateDataMetrics({}, {});
        expect(res).toEqual(zeroValueRes);
    });

    test('should return value of target if source are empty', () => {
        const target = example1;
        const res = consolidateDataMetrics(target, {});
        expect(res).toEqual(target);
    });

    test('should return value of source if target are empty', () => {
        const source = example1;
        const res = consolidateDataMetrics({}, source);
        expect(res).toEqual(source);
    });

    test('should correctly consolidate source and target', () => {
        const source = example1;
        const target = example1;
        const res = consolidateDataMetrics(target, source);
        expect(res).toEqual(example2);
    });

    test('should not consolidate data other than usedCapacity and objectCount', () => {
        const source = {
            usedCapacity123: example1.usedCapacity,
            objectCount456: example1.objectCount,
        };
        const target = {
            usedCapacity123: example1.usedCapacity,
            objectCount456: example1.objectCount,
        };
        const res = consolidateDataMetrics(target, source);
        expect(res).toEqual(zeroValueRes);
    });

    test('should consolidate inflight delta metrics', () => {
        const source = exampleWithInflights;
        const target = example1;
        const res = consolidateDataMetrics(target, source);
        expect(res).toEqual(expectedResponseWithInflights);
    });

    test('should consolidate MPUs', () => {
        const source = {
            usedCapacity: {
                current: BigInt(100),
                nonCurrent: BigInt(10),
                _inflightsPreScan: BigInt(0),
                _currentCold: BigInt(0),
                _nonCurrentCold: BigInt(0),
                _currentRestored: BigInt(0),
                _currentRestoring: BigInt(0),
                _nonCurrentRestored: BigInt(0),
                _nonCurrentRestoring: BigInt(0),
                _incompleteMPUParts: BigInt(100),
            },
            objectCount: {
                current: BigInt(10),
                nonCurrent: BigInt(1),
                deleteMarker: BigInt(1),
                _currentCold: BigInt(0),
                _nonCurrentCold: BigInt(0),
                _currentRestored: BigInt(0),
                _currentRestoring: BigInt(0),
                _nonCurrentRestored: BigInt(0),
                _nonCurrentRestoring: BigInt(0),
                _incompleteMPUUploads: BigInt(10),
            },
        };
        const res = consolidateDataMetrics(exampleWithMPU, source);
        expect(res).toEqual(expectedConsolidatedMPU);
    });
});
