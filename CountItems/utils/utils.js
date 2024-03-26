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
                _currentCold: 0,
                _nonCurrentCold: 0,
                _currentRestored: 0,
                _currentRestoring: 0,
                _nonCurrentRestored: 0,
                _nonCurrentRestoring: 0,
            },
        });
    }
    if (!resTarget.objectCount) {
        Object.assign(resTarget, {
            objectCount: {
                current: 0,
                nonCurrent: 0,
                _currentCold: 0,
                _nonCurrentCold: 0,
                _currentRestored: 0,
                _currentRestoring: 0,
                _nonCurrentRestored: 0,
                _nonCurrentRestoring: 0,
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
    resTarget.usedCapacity._currentCold += usedCapacity && usedCapacity._currentCold ? usedCapacity._currentCold : 0;
    resTarget.usedCapacity._nonCurrentCold += usedCapacity && usedCapacity._nonCurrentCold ? usedCapacity._nonCurrentCold : 0;
    resTarget.usedCapacity._currentRestoring += usedCapacity && usedCapacity._currentRestoring ? usedCapacity._currentRestoring : 0;
    resTarget.usedCapacity._currentRestored += usedCapacity && usedCapacity._currentRestored ? usedCapacity._currentRestored : 0;
    resTarget.usedCapacity._nonCurrentRestoring += usedCapacity && usedCapacity._nonCurrentRestoring ? usedCapacity._nonCurrentRestoring : 0;
    resTarget.usedCapacity._nonCurrentRestored += usedCapacity && usedCapacity._nonCurrentRestored ? usedCapacity._nonCurrentRestored : 0;

    resTarget.objectCount.current += objectCount && objectCount.current ? objectCount.current : 0;
    resTarget.objectCount.nonCurrent += objectCount && objectCount.nonCurrent ? objectCount.nonCurrent : 0;
    resTarget.objectCount.deleteMarker += objectCount && objectCount.deleteMarker ? objectCount.deleteMarker : 0;
    resTarget.objectCount._currentCold += objectCount && objectCount._currentCold ? objectCount._currentCold : 0;
    resTarget.objectCount._nonCurrentCold += objectCount && objectCount._nonCurrentCold ? objectCount._nonCurrentCold : 0;
    resTarget.objectCount._currentRestoring += objectCount && objectCount._currentRestoring ? objectCount._currentRestoring : 0;
    resTarget.objectCount._currentRestored += objectCount && objectCount._currentRestored ? objectCount._currentRestored : 0;
    resTarget.objectCount._nonCurrentRestoring += objectCount && objectCount._nonCurrentRestoring ? objectCount._nonCurrentRestoring : 0;
    resTarget.objectCount._nonCurrentRestored += objectCount && objectCount._nonCurrentRestored ? objectCount._nonCurrentRestored : 0;

    // Current and NonCurrent are the total of all other metrics
    resTarget.usedCapacity.current += usedCapacity
        ? usedCapacity._currentCold + usedCapacity._currentRestored + usedCapacity._currentRestoring : 0;
    resTarget.usedCapacity.nonCurrent += usedCapacity
        ? usedCapacity._nonCurrentCold + usedCapacity._nonCurrentRestored + usedCapacity._nonCurrentRestoring : 0;
    resTarget.objectCount.current += objectCount
        ? objectCount._currentCold + objectCount._currentRestored + objectCount._currentRestoring : 0;
    resTarget.objectCount.nonCurrent += objectCount
        ? objectCount._nonCurrentCold + objectCount._nonCurrentRestored + objectCount._nonCurrentRestoring : 0;

    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
