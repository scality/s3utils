const { ObjectMD } = require('arsenal').models;
const {
    VersionID: { encode },
    VersioningConstants,
} = require('arsenal').versioning;

const { StalledEntry } = require('../../StalledRetry/CursorWrapper');

const testVersion = '98502347359531999999RG001  5.2375.2122';

function generateKeyId(isMaster, key, versionId) {
    if (isMaster) {
        return key;
    }
    return `${key}${VersioningConstants.VersionId.Separator}${versionId}`;
}

function generateMD(isMaster, objectKey, lastModified, storageClasses) {
    const keyId = generateKeyId(isMaster, objectKey, testVersion);

    return {
        _id: {
            id: keyId,
            key: objectKey,
            storageClasses,
            versionId: testVersion,
        },
        value: new ObjectMD()
            .setKey(objectKey)
            .setLastModified(lastModified)
            .setVersionId(testVersion)
            .setReplicationStorageClass(storageClasses)
            .getValue(),
    };
}

function generateObjectMD(objectKey, lastModified, storageClasses) {
    const versionMd = generateMD(false, objectKey, lastModified, storageClasses);
    const masterMd = generateMD(true, objectKey, lastModified, storageClasses);
    // mock master and version md
    return [versionMd, masterMd];
}

function generateRequestObject(bucket, objectKey, storageClass) {
    const storageClassName = storageClass.split(':')[0];
    return new StalledEntry(
        bucket,
        objectKey,
        encode(testVersion),
        storageClassName,
        true,
    );
}

function generateRequestArray(bucket, objectKey, storageClasses) {
    return storageClasses.split(',')
        .map(i => generateRequestObject(bucket, objectKey, i));
}

function generateModifiedDateString(date, delta) {
    const mDate = new Date(date);
    mDate.setHours(mDate.getHours() + delta);
    return mDate.toUTCString();
}

module.exports = {
    generateRequestObject,
    generateRequestArray,
    generateObjectMD,
    generateModifiedDateString,
};
