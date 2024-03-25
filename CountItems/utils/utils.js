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
                restored: 0,
                restoring: 0,
                currentCold: 0,
                nonCurrentCold: 0,
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
                restored: 0,
                restoring: 0,
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
    resTarget.usedCapacity.restoring += usedCapacity && usedCapacity.restoring ? usedCapacity.restoring : 0;
    resTarget.usedCapacity.restored += usedCapacity && usedCapacity.restored ? usedCapacity.restored : 0;

    resTarget.objectCount.current += objectCount && objectCount.current ? objectCount.current : 0;
    resTarget.objectCount.nonCurrent += objectCount && objectCount.nonCurrent ? objectCount.nonCurrent : 0;
    resTarget.objectCount.deleteMarker += objectCount && objectCount.deleteMarker ? objectCount.deleteMarker : 0;
    resTarget.objectCount.currentCold += objectCount && objectCount.currentCold ? objectCount.currentCold : 0;
    resTarget.objectCount.nonCurrentCold += objectCount && objectCount.nonCurrentCold ? objectCount.nonCurrentCold : 0;
    resTarget.objectCount.restoring += objectCount && objectCount.restoring ? objectCount.restoring : 0;
    resTarget.objectCount.restored += objectCount && objectCount.restored ? objectCount.restored : 0;
    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
