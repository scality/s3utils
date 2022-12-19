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
            },
        });
    }
    if (!resTarget.objectCount) {
        Object.assign(resTarget, {
            objectCount: {
                current: 0,
                nonCurrent: 0,
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
    resTarget.objectCount.current += objectCount && objectCount.current ? objectCount.current : 0;
    resTarget.objectCount.nonCurrent += objectCount && objectCount.nonCurrent ? objectCount.nonCurrent : 0;
    resTarget.objectCount.deleteMarker += objectCount && objectCount.deleteMarker ? objectCount.deleteMarker : 0;
    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
