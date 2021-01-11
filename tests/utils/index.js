const { ObjectMD } = require('arsenal').models;
const { encode } = require('arsenal').versioning.VersionID;

const { StalledEntry } = require('../../StalledRetry/CursorWrapper');

const testVersion = '98502347359531999999RG001  5.2375.2122';

function generateObjectMD(objectKey, lastModified, storageClasses) {
    return {
        _id: {
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

function generateRequestObject(bucket, objectKey, storageClass) {
    const storageClassName = storageClass.split(':')[0];
    return new StalledEntry(
        bucket,
        objectKey,
        encode(testVersion),
        storageClassName,
        true
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
