const { consolidateDataMetrics } = require('../../../../CountItems/utils/utils');

describe('CountItems::utils::consolidateDataMetrics', () => {
    const zeroValueRes = {
        usedCapacity: {
            current: 0,
            nonCurrent: 0,
            currentCold: 0,
            nonCurrentCold: 0,
            currentRestored: 0,
            currentRestoring: 0,
            nonCurrentRestored: 0,
            nonCurrentRestoring: 0,
        },
        objectCount: {
            current: 0,
            nonCurrent: 0,
            deleteMarker: 0,
            currentCold: 0,
            nonCurrentCold: 0,
            currentRestored: 0,
            currentRestoring: 0,
            nonCurrentRestored: 0,
            nonCurrentRestoring: 0,
        },
    };

    const example1 = {
        usedCapacity: {
            current: 10,
            nonCurrent: 10,
            currentCold: 0,
            nonCurrentCold: 0,
            currentRestored: 0,
            currentRestoring: 0,
            nonCurrentRestored: 0,
            nonCurrentRestoring: 0,
        },
        objectCount: {
            current: 10,
            nonCurrent: 10,
            deleteMarker: 10,
            currentCold: 0,
            nonCurrentCold: 0,
            currentRestored: 0,
            currentRestoring: 0,
            nonCurrentRestored: 0,
            nonCurrentRestoring: 0,
        },
    };

    const example2 = {
        usedCapacity: {
            current: 20,
            nonCurrent: 20,
            currentCold: 0,
            nonCurrentCold: 0,
            currentRestored: 0,
            currentRestoring: 0,
            nonCurrentRestored: 0,
            nonCurrentRestoring: 0,
        },
        objectCount: {
            current: 20,
            nonCurrent: 20,
            deleteMarker: 20,
            currentCold: 0,
            nonCurrentCold: 0,
            currentRestored: 0,
            currentRestoring: 0,
            nonCurrentRestored: 0,
            nonCurrentRestoring: 0,
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
});
