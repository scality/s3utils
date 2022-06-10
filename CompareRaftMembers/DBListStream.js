const stream = require('stream');

/**
 * List entries from a low-level Metadata database, filtered to be
 * compatible with repd listings done by verifyBucketSproxydKeys
 * script to create the block digests database, so that digests can be
 * computed from the stream and compared against the digests database.
 *
 * @class DBListStream
 */
class DBListStream extends stream.Transform {
    /**
     * @constructor
     * @param {object} params - params object
     * @param {string} params.dbName - name of the database: it is
     * used to support legacy databases, where the database name is
     * the bucket name, in which case we need to prefix the key by the
     * bucket name. If the name is of the form "storeDbX[Y]", the
     * bucket name is already prefixed to the key.
     */
    constructor(params) {
        super({ objectMode: true });

        if (typeof params !== 'object') {
            throw new Error('DBListStream.constructor(): params must be an object');
        }
        if (typeof params.dbName !== 'string') {
            throw new Error('DBListStream.constructor(): params.dbName must be a string');
        }
        this.dbName = params.dbName;
        this.isLegacyDb = !(/^storeDb[0-9]+$/.test(this.dbName));

        this.lastMasterKey = null;
        this.lastMasterVersionId = null;
    }

    _transform(item, encoding, callback) {
        const { key, value } = item;
        let bucket;
        let objectKey;
        if (this.isLegacyDb) {
            bucket = this.dbName;
            objectKey = key;
        } else {
            const slashIndex = key.indexOf('/');
            if (slashIndex === -1 || slashIndex === key.length - 1) {
                // ignore keys that do not match the expected `bucket/key` scheme
                return callback();
            }
            [bucket, objectKey] = [key.slice(0, slashIndex), key.slice(slashIndex + 1)];
        }
        // ignore both internal replay keys and metadata v1 keys, by
        // skipping any key beginning with the '\x7f' byte
        //
        // Note: metadata v1 keys may be supported in the future, in
        // which case we should still ignore replay keys
        if (objectKey.startsWith('\x7f')) {
            return callback();
        }
        const vidSepPos = objectKey.lastIndexOf('\0');
        const md = JSON.parse(value);
        if (md.isPHD) {
            // object is a Place Holder Delete (PHD)
            return callback();
        }
        if (vidSepPos === -1) {
            this.lastMasterKey = objectKey;
            this.lastMasterVersionId = md.versionId;
        } else {
            const masterKey = objectKey.slice(0, vidSepPos);
            if (masterKey === this.lastMasterKey
                && md.versionId === this.lastMasterVersionId) {
                // we have already processed this versioned key as
                // the master key, so skip it
                return callback();
            }
        }

        if (this.isLegacyDb) {
            this.push({ key: `${bucket}/${objectKey}`, value });
        } else {
            this.push(item);
        }
        return callback();
    }
}

module.exports = DBListStream;
