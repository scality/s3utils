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
                current: BigInt(0),
                nonCurrent: BigInt(0),
                _currentCold: BigInt(0),
                _nonCurrentCold: BigInt(0),
                _currentRestored: BigInt(0),
                _currentRestoring: BigInt(0),
                _nonCurrentRestored: BigInt(0),
                _nonCurrentRestoring: BigInt(0),
                _inflightsPreScan: BigInt(0),
                _incompleteMPUParts: BigInt(0),
            },
        });
    }
    if (!resTarget.objectCount) {
        Object.assign(resTarget, {
            objectCount: {
                current: BigInt(0),
                nonCurrent: BigInt(0),
                _currentCold: BigInt(0),
                _nonCurrentCold: BigInt(0),
                _currentRestored: BigInt(0),
                _currentRestoring: BigInt(0),
                _nonCurrentRestored: BigInt(0),
                _nonCurrentRestoring: BigInt(0),
                _incompleteMPUUploads: BigInt(0),
                deleteMarker: BigInt(0),
            },
        });
    }
    if (!source) {
        return resTarget;
    }
    const { usedCapacity, objectCount, accountOwnerID } = source;
    resTarget.usedCapacity.current += usedCapacity && usedCapacity.current ? usedCapacity.current : BigInt(0);
    resTarget.usedCapacity.nonCurrent += usedCapacity && usedCapacity.nonCurrent ? usedCapacity.nonCurrent : BigInt(0);
    resTarget.usedCapacity._currentCold += usedCapacity && usedCapacity._currentCold ? usedCapacity._currentCold : BigInt(0);
    resTarget.usedCapacity._nonCurrentCold += usedCapacity && usedCapacity._nonCurrentCold ? usedCapacity._nonCurrentCold : BigInt(0);
    resTarget.usedCapacity._currentRestoring += usedCapacity && usedCapacity._currentRestoring ? usedCapacity._currentRestoring : BigInt(0);
    resTarget.usedCapacity._currentRestored += usedCapacity && usedCapacity._currentRestored ? usedCapacity._currentRestored : BigInt(0);
    resTarget.usedCapacity._nonCurrentRestoring += usedCapacity && usedCapacity._nonCurrentRestoring ? usedCapacity._nonCurrentRestoring : BigInt(0);
    resTarget.usedCapacity._nonCurrentRestored += usedCapacity && usedCapacity._nonCurrentRestored ? usedCapacity._nonCurrentRestored : BigInt(0);
    resTarget.usedCapacity._incompleteMPUParts += usedCapacity && usedCapacity._incompleteMPUParts ? usedCapacity._incompleteMPUParts : BigInt(0);

    resTarget.objectCount.current += objectCount && objectCount.current ? objectCount.current : BigInt(0);
    resTarget.objectCount.nonCurrent += objectCount && objectCount.nonCurrent ? objectCount.nonCurrent : BigInt(0);
    resTarget.objectCount.deleteMarker += objectCount && objectCount.deleteMarker ? objectCount.deleteMarker : BigInt(0);
    resTarget.objectCount._currentCold += objectCount && objectCount._currentCold ? objectCount._currentCold : BigInt(0);
    resTarget.objectCount._nonCurrentCold += objectCount && objectCount._nonCurrentCold ? objectCount._nonCurrentCold : BigInt(0);
    resTarget.objectCount._currentRestoring += objectCount && objectCount._currentRestoring ? objectCount._currentRestoring : BigInt(0);
    resTarget.objectCount._currentRestored += objectCount && objectCount._currentRestored ? objectCount._currentRestored : BigInt(0);
    resTarget.objectCount._nonCurrentRestoring += objectCount && objectCount._nonCurrentRestoring ? objectCount._nonCurrentRestoring : BigInt(0);
    resTarget.objectCount._nonCurrentRestored += objectCount && objectCount._nonCurrentRestored ? objectCount._nonCurrentRestored : BigInt(0);
    resTarget.objectCount._incompleteMPUUploads += objectCount && objectCount._incompleteMPUUploads ? objectCount._incompleteMPUUploads : BigInt(0);

    resTarget.usedCapacity._inflightsPreScan += usedCapacity && usedCapacity._inflightsPreScan ? usedCapacity._inflightsPreScan : BigInt(0);
    if (accountOwnerID) {
        resTarget.accountOwnerID = accountOwnerID;
    }

    resTarget.usedCapacity.current += usedCapacity
        ? usedCapacity._currentCold + usedCapacity._currentRestored + usedCapacity._currentRestoring
        + usedCapacity._incompleteMPUParts : BigInt(0);
    resTarget.usedCapacity.nonCurrent += usedCapacity
        ? usedCapacity._nonCurrentCold + usedCapacity._nonCurrentRestored + usedCapacity._nonCurrentRestoring : BigInt(0);
    resTarget.objectCount.current += objectCount
        ? objectCount._currentCold + objectCount._currentRestored + objectCount._currentRestoring
        + objectCount._incompleteMPUUploads : BigInt(0);
    resTarget.objectCount.nonCurrent += objectCount
        ? objectCount._nonCurrentCold + objectCount._nonCurrentRestored + objectCount._nonCurrentRestoring : BigInt(0);

    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
