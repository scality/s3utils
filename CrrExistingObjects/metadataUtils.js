const { errors, versioning } = require('arsenal');
const metadataClient = require('./metadataClient');

const versionIdUtils = versioning.VersionID;

const { GENERATE_INTERNAL_VERSION_ID } = process.env;
const REPLICATION_GROUP_ID = process.env.REPLICATION_GROUP_ID || 'RG001';
// Use Arsenal function to generate a version ID used internally by metadata
// for null versions that are created before bucket versioning is configured
const nonVersionedObjId = versionIdUtils.getInfVid(REPLICATION_GROUP_ID);

function _processVersions(list) {
    /* eslint-disable no-param-reassign */
    list.NextVersionIdMarker = list.NextVersionIdMarker
        ? versionIdUtils.encode(list.NextVersionIdMarker)
        : list.NextVersionIdMarker;

    list.Versions.forEach(v => {
        v.VersionId = v.VersionId
            ? versionIdUtils.encode(v.VersionId) : v.VersionId;
    });
    /* eslint-enable no-param-reassign */
    return list;
}

function listObjectVersions(params, log, cb) {
    const bucketName = params.Bucket;
    const listingParams = {
        listingType: 'DelimiterVersions',
        maxKeys: params.MaxKeys,
        prefix: params.Prefix,
        keyMarker: params.KeyMarker,
        versionIdMarker: params.VersionIdMarker,
    };
    log.debug('listing object versions', {
        method: 'metadataUtils.listObjectVersions',
        listingParams,
    });
    return metadataClient.listObject(
        bucketName,
        listingParams,
        log,
        (err, list) => {
            if (err) {
                return cb(err);
            }
            return cb(null, _processVersions(list));
        },
    );
}

function _formatConfig(config) {
    const { role, destination, rules } = config;
    const Rules = rules.map(rule => {
        const {
            prefix, enabled, storageClass, id,
        } = rule;
        return {
            ID: id,
            Prefix: prefix,
            Status: enabled ? 'Enabled' : 'Disabled',
            Destination: {
                Bucket: destination,
                StorageClass: (storageClass || ''),
            },
        };
    });
    return {
        ReplicationConfiguration: {
            Role: role,
            Rules,
        },
    };
}

function getBucketReplication(options, log, cb) {
    const bucketName = options.Bucket;
    log.debug('getting bucket replication', {
        method: 'metadataUtils.getBucketReplication',
        bucket: bucketName,
    });
    return metadataClient.getBucket(bucketName, log, (err, data) => {
        if (err) {
            return cb(err);
        }
        const replConf = _formatConfig(data._replicationConfiguration);
        return cb(null, replConf);
    });
}

function _getNullVersion(objMD, bucketName, objectKey, log, cb) {
    const options = {};
    if (objMD.isNull || !objMD.versionId) {
        log.debug('found null version');
        return process.nextTick(() => cb(null, objMD));
    }
    if (objMD.nullVersionId) {
        log.debug('null version exists, get the null version');
        options.versionId = objMD.nullVersionId;
        return metadataClient.getObjectMD(
            bucketName,
            objectKey,
            options,
            log,
            cb,
        );
    }
    return process.nextTick(() => cb());
}

function getMetadata(params, log, cb) {
    const { Bucket, Key } = params;
    let versionId = params.VersionId;
    log.debug('getting object metadata', {
        method: 'metadataUtils.getMetadata',
        bucket: Bucket,
        objectKey: Key,
        versionId,
    });
    if (versionId && versionId !== 'null') {
        versionId = versionIdUtils.decode(versionId);
    }
    if (versionId instanceof Error) {
        const errMsg = 'Invalid version id specified';
        return cb(errors.InvalidArgument.customizeDescription(errMsg));
    }
    const mdParams = {
        versionId,
    };
    return metadataClient.getObjectMD(
        Bucket,
        Key,
        mdParams,
        log,
        (err, data) => {
            if (err) {
                return cb(err);
            }
            if (data && versionId === 'null') {
                return _getNullVersion(
                    data,
                    Bucket,
                    Key,
                    log,
                    (err, nullVer) => {
                        if (err) {
                            return cb(err);
                        }
                        return cb(null, nullVer);
                    },
                );
            }
            return cb(null, data);
        },
    );
}

function getOptions(objMD) {
    const options = {};

    if (objMD.versionId === undefined) {
        if (!GENERATE_INTERNAL_VERSION_ID) {
            return options;
        }

        objMD.setIsNull(true);
        objMD.setVersionId(nonVersionedObjId);

        options.nullVersionId = objMD.versionId;
        // non-versioned (non-null) MPU objects don't have a
        // replay ID, so don't reference their uploadId
        if (objMD.uploadId) {
            options.nullUploadId = objMD.uploadId;
        }
    }

    // specify both 'versioning' and 'versionId' to create a "new"
    // version (updating master as well) but with specified versionId
    options.versioning = true;
    options.versionId = objMD.versionId;
    return options;
}

function putMetadata(params, log, cb) {
    const { Bucket, Key, Body: objMD } = params;
    const options = getOptions(objMD);

    log.debug('updating object metadata', {
        method: 'metadataUtils.putMetadata',
        bucket: Bucket,
        objectKey: Key,
        versionId: objMD.versionId,
    });
    // If the object is from a source bucket without versioning (i.e. NFS),
    // then we want to create a version for the replica object even though
    // none was provided in the object metadata value.
    if (objMD.replicationInfo.isNFS) {
        const isReplica = objMD.replicationInfo.status === 'REPLICA';
        options.versioning = isReplica;
        objMD.replicationInfo.isNFS = !isReplica;
    }
    return metadataClient.putObjectMD(Bucket, Key, objMD, options, log, cb);
}

module.exports = {
    metadataClient,
    listObjectVersions,
    getBucketReplication,
    getMetadata,
    putMetadata,
};
