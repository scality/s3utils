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
    resTarget.usedCapacity.current += BigInt(usedCapacity?.current || 0n);
    resTarget.usedCapacity.nonCurrent += BigInt(usedCapacity?.nonCurrent || 0n);
    resTarget.usedCapacity._currentCold += BigInt(usedCapacity?._currentCold || 0n);
    resTarget.usedCapacity._nonCurrentCold += BigInt(usedCapacity?._nonCurrentCold || 0n);
    resTarget.usedCapacity._currentRestoring += BigInt(usedCapacity?._currentRestoring || 0n);
    resTarget.usedCapacity._currentRestored += BigInt(usedCapacity?._currentRestored || 0n);
    resTarget.usedCapacity._nonCurrentRestoring += BigInt(usedCapacity?._nonCurrentRestoring || 0n);
    resTarget.usedCapacity._nonCurrentRestored += BigInt(usedCapacity?._nonCurrentRestored || 0n);
    resTarget.usedCapacity._incompleteMPUParts += BigInt(usedCapacity?._incompleteMPUParts || 0n);

    resTarget.objectCount.current += BigInt(objectCount?.current || 0n);
    resTarget.objectCount.nonCurrent += BigInt(objectCount?.nonCurrent || 0n);
    resTarget.objectCount.deleteMarker += BigInt(objectCount?.deleteMarker || 0n);
    resTarget.objectCount._currentCold += BigInt(objectCount?._currentCold || 0n);
    resTarget.objectCount._nonCurrentCold += BigInt(objectCount?._nonCurrentCold || 0n);
    resTarget.objectCount._currentRestoring += BigInt(objectCount?._currentRestoring || 0n);
    resTarget.objectCount._currentRestored += BigInt(objectCount?._currentRestored || 0n);
    resTarget.objectCount._nonCurrentRestoring += BigInt(objectCount?._nonCurrentRestoring || 0n);
    resTarget.objectCount._nonCurrentRestored += BigInt(objectCount?._nonCurrentRestored || 0n);
    resTarget.objectCount._incompleteMPUUploads += BigInt(objectCount?._incompleteMPUUploads || 0n);

    resTarget.usedCapacity._inflightsPreScan += BigInt(usedCapacity?._inflightsPreScan || 0n);
    if (accountOwnerID) {
        resTarget.accountOwnerID = accountOwnerID;
    }

    resTarget.usedCapacity.current += usedCapacity
        ? BigInt(usedCapacity._currentCold) + BigInt(usedCapacity._currentRestored) + BigInt(usedCapacity._currentRestoring)
        + BigInt(usedCapacity._incompleteMPUParts) : 0n;
    resTarget.usedCapacity.nonCurrent += usedCapacity
        ? BigInt(usedCapacity._nonCurrentCold) + BigInt(usedCapacity._nonCurrentRestored) + BigInt(usedCapacity._nonCurrentRestoring) : 0n;
    resTarget.objectCount.current += objectCount
        ? BigInt(objectCount._currentCold) + BigInt(objectCount._currentRestored) + BigInt(objectCount._currentRestoring)
        + BigInt(objectCount._incompleteMPUUploads) : 0n;
    resTarget.objectCount.nonCurrent += objectCount
        ? BigInt(objectCount._nonCurrentCold) + BigInt(objectCount._nonCurrentRestored) + BigInt(objectCount._nonCurrentRestoring) : 0n;

    return resTarget;
}

module.exports = {
    consolidateDataMetrics,
};
