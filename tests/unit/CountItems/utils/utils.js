const { consolidateDataMetrics } = require('../../../../CountItems/utils/utils');

describe('CountItems::utils::consolidateDataMetrics', () => {
    const zeroValueRes = {
        usedCapacity: {
            current: 0n,
            nonCurrent: 0n,
            _inflightsPreScan: 0n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 0n,
        },
        objectCount: {
            current: 0n,
            nonCurrent: 0n,
            deleteMarker: 0n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 0n,
        },
    };

    const example1 = {
        usedCapacity: {
            current: 10n,
            nonCurrent: 10n,
            _inflightsPreScan: 0n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 0n,
        },
        objectCount: {
            current: 10n,
            nonCurrent: 10n,
            deleteMarker: 10n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 0n,
        },
    };

    const example2 = {
        usedCapacity: {
            current: 20n,
            nonCurrent: 20n,
            _inflightsPreScan: 0n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 0n,
        },
        objectCount: {
            current: 20n,
            nonCurrent: 20n,
            deleteMarker: 20n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 0n,
        },
    };

    const exampleWithInflights = {
        usedCapacity: {
            current: 20n,
            nonCurrent: 20n,
            _inflightsPreScan: 1000n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 0n,
        },
        objectCount: {
            current: 20n,
            nonCurrent: 20n,
            deleteMarker: 20n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 0n,
        },
    };

    const expectedResponseWithInflights = {
        usedCapacity: {
            current: 40n,
            nonCurrent: 40n,
            _inflightsPreScan: 1000n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 0n,
        },
        objectCount: {
            current: 40n,
            nonCurrent: 40n,
            deleteMarker: 40n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 0n,
        },
    };

    const exampleWithMPU = {
        usedCapacity: {
            current: 20n,
            nonCurrent: 20n,
            _inflightsPreScan: 0n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 100n,
        },
        objectCount: {
            current: 20n,
            nonCurrent: 20n,
            deleteMarker: 20n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 10n,
        },
    };

    const expectedConsolidatedMPU = {
        usedCapacity: {
            current: 220n,
            nonCurrent: 30n,
            _inflightsPreScan: 0n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUParts: 200n,
        },
        objectCount: {
            current: 40n,
            nonCurrent: 21n,
            deleteMarker: 21n,
            _currentCold: 0n,
            _nonCurrentCold: 0n,
            _currentRestored: 0n,
            _currentRestoring: 0n,
            _nonCurrentRestored: 0n,
            _nonCurrentRestoring: 0n,
            _incompleteMPUUploads: 20n,
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
                current: 100n,
                nonCurrent: 10n,
                _inflightsPreScan: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUParts: 100n,
            },
            objectCount: {
                current: 10n,
                nonCurrent: 1n,
                deleteMarker: 1n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _incompleteMPUUploads: 10n,
            },
        };
        const res = consolidateDataMetrics(exampleWithMPU, source);
        expect(res).toEqual(expectedConsolidatedMPU);
    });
});
