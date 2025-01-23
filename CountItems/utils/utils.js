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
                current: 0n,
                nonCurrent: 0n,
                _currentCold: 0n,
                _nonCurrentCold: 0n,
                _currentRestored: 0n,
                _currentRestoring: 0n,
                _nonCurrentRestored: 0n,
                _nonCurrentRestoring: 0n,
                _inflightsPreScan: 0n,
                _incompleteMPUParts: 0n,
            },
        });
    }
    if (!resTarget.objectCount) {
        Object.assign(resTarget, {
            objectCount: {
                current: 0n,
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
        });
    }
    if (!source) {
        return resTarget;
    }
    const { usedCapacity, objectCount, accountOwnerID } = source;
    resTarget.usedCapacity.current += usedCapacity && usedCapacity.current ? usedCapacity.current : 0n;
    resTarget.usedCapacity.nonCurrent += usedCapacity && usedCapacity.nonCurrent ? usedCapacity.nonCurrent : 0n;
    resTarget.usedCapacity._currentCold += usedCapacity && usedCapacity._currentCold ? usedCapacity._currentCold : 0n;
    resTarget.usedCapacity._nonCurrentCold += usedCapacity && usedCapacity._nonCurrentCold ? usedCapacity._nonCurrentCold : 0n;
    resTarget.usedCapacity._currentRestoring += usedCapacity && usedCapacity._currentRestoring ? usedCapacity._currentRestoring : 0n;
    resTarget.usedCapacity._currentRestored += usedCapacity && usedCapacity._currentRestored ? usedCapacity._currentRestored : 0n;
    resTarget.usedCapacity._nonCurrentRestoring += usedCapacity && usedCapacity._nonCurrentRestoring ? usedCapacity._nonCurrentRestoring : 0n;
    resTarget.usedCapacity._nonCurrentRestored += usedCapacity && usedCapacity._nonCurrentRestored ? usedCapacity._nonCurrentRestored : 0n;
    resTarget.usedCapacity._incompleteMPUParts += usedCapacity && usedCapacity._incompleteMPUParts ? usedCapacity._incompleteMPUParts : 0n;

    resTarget.objectCount.current += objectCount && objectCount.current ? objectCount.current : 0n;
    resTarget.objectCount.nonCurrent += objectCount && objectCount.nonCurrent ? objectCount.nonCurrent : 0n;
    resTarget.objectCount.deleteMarker += objectCount && objectCount.deleteMarker ? objectCount.deleteMarker : 0n;
    resTarget.objectCount._currentCold += objectCount && objectCount._currentCold ? objectCount._currentCold : 0n;
    resTarget.objectCount._nonCurrentCold += objectCount && objectCount._nonCurrentCold ? objectCount._nonCurrentCold : 0n;
    resTarget.objectCount._currentRestoring += objectCount && objectCount._currentRestoring ? objectCount._currentRestoring : 0n;
    resTarget.objectCount._currentRestored += objectCount && objectCount._currentRestored ? objectCount._currentRestored : 0n;
    resTarget.objectCount._nonCurrentRestoring += objectCount && objectCount._nonCurrentRestoring ? objectCount._nonCurrentRestoring : 0n;
    resTarget.objectCount._nonCurrentRestored += objectCount && objectCount._nonCurrentRestored ? objectCount._nonCurrentRestored : 0n;
    resTarget.objectCount._incompleteMPUUploads += objectCount && objectCount._incompleteMPUUploads ? objectCount._incompleteMPUUploads : 0n;

    resTarget.usedCapacity._inflightsPreScan += usedCapacity && usedCapacity._inflightsPreScan ? usedCapacity._inflightsPreScan : 0n;
    if (accountOwnerID) {
        resTarget.accountOwnerID = accountOwnerID;
    }

    resTarget.usedCapacity.current += usedCapacity
        ? usedCapacity._currentCold + usedCapacity._currentRestored + usedCapacity._currentRestoring
        + usedCapacity._incompleteMPUParts : 0n;
    resTarget.usedCapacity.nonCurrent += usedCapacity
        ? usedCapacity._nonCurrentCold + usedCapacity._nonCurrentRestored + usedCapacity._nonCurrentRestoring : 0n;
    resTarget.objectCount.current += objectCount
        ? objectCount._currentCold + objectCount._currentRestored + objectCount._currentRestoring
        + objectCount._incompleteMPUUploads : 0n;
    resTarget.objectCount.nonCurrent += objectCount
        ? objectCount._nonCurrentCold + objectCount._nonCurrentRestored + objectCount._nonCurrentRestoring : 0n;

    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
