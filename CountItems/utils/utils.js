function consolidateDataMetrics(target, source) {
    let resTarget = {};
    if (target && (target instanceof Object)) {
        resTarget = {
            usedCapacity: target.usedCapacity,
            objectCount: target.objectCount,
        };
    }
    if (!resTarget.usedCapacity) {
        Object.assign(resTarget, {
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
        });
    }
    if (!resTarget.objectCount) {
        Object.assign(resTarget, {
            objectCount: {
                current: 0,
                nonCurrent: 0,
                currentCold: 0,
                nonCurrentCold: 0,
                currentRestored: 0,
                currentRestoring: 0,
                nonCurrentRestored: 0,
                nonCurrentRestoring: 0,
                deleteMarker: 0,
            },
        });
    }
    if (!source) {
        return resTarget;
    }
    const { usedCapacity, objectCount } = source;
    resTarget.usedCapacity.current += usedCapacity && usedCapacity.current ? usedCapacity.current : 0;
    resTarget.usedCapacity.nonCurrent += usedCapacity && usedCapacity.nonCurrent ? usedCapacity.nonCurrent : 0;
    resTarget.usedCapacity.currentCold += usedCapacity && usedCapacity.currentCold ? usedCapacity.currentCold : 0;
    resTarget.usedCapacity.nonCurrentCold += usedCapacity && usedCapacity.nonCurrentCold ? usedCapacity.nonCurrentCold : 0;
    resTarget.usedCapacity.currentRestoring += usedCapacity && usedCapacity.currentRestoring ? usedCapacity.currentRestoring : 0;
    resTarget.usedCapacity.currentRestored += usedCapacity && usedCapacity.currentRestored ? usedCapacity.currentRestored : 0;
    resTarget.usedCapacity.nonCurrentRestoring += usedCapacity && usedCapacity.nonCurrentRestoring ? usedCapacity.nonCurrentRestoring : 0;
    resTarget.usedCapacity.nonCurrentRestored += usedCapacity && usedCapacity.nonCurrentRestored ? usedCapacity.nonCurrentRestored : 0;

    resTarget.objectCount.current += objectCount && objectCount.current ? objectCount.current : 0;
    resTarget.objectCount.nonCurrent += objectCount && objectCount.nonCurrent ? objectCount.nonCurrent : 0;
    resTarget.objectCount.deleteMarker += objectCount && objectCount.deleteMarker ? objectCount.deleteMarker : 0;
    resTarget.objectCount.currentCold += objectCount && objectCount.currentCold ? objectCount.currentCold : 0;
    resTarget.objectCount.nonCurrentCold += objectCount && objectCount.nonCurrentCold ? objectCount.nonCurrentCold : 0;
    resTarget.objectCount.currentRestoring += objectCount && objectCount.currentRestoring ? objectCount.currentRestoring : 0;
    resTarget.objectCount.currentRestored += objectCount && objectCount.currentRestored ? objectCount.currentRestored : 0;
    resTarget.objectCount.nonCurrentRestoring += objectCount && objectCount.nonCurrentRestoring ? objectCount.nonCurrentRestoring : 0;
    resTarget.objectCount.nonCurrentRestored += objectCount && objectCount.nonCurrentRestored ? objectCount.nonCurrentRestored : 0;
    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
