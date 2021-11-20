const { MultiMap } = require('./DuplicateKeysWindow');
const { repairObject } = require('../repairDuplicateVersionsSuite');
const getObjectURL = require('../VerifyBucketSproxydKeys/getObjectURL');
const { Logger } = require('werelogs');
const log = new Logger('s3utils:DuplicateKeysIngestion');

const subscribers = new MultiMap();

/**
 * @class
 * @classdesc Handler for duplicate sproxyd key found events.
 */
class DuplicateSproxydKeyFoundHandler {
    constructor() {
        this._repairObject = repairObject;
        this._getObjectURL = getObjectURL;
    }

    /**
     * Helper to compare precedence of two strings.
     * The largest string is first (which is the older version).
     * Older version is chosen to repair.
     * @param {string} a - Versioned Object key.
     * @param {string} b - Versioned Object key.
     * @returns {number} 0, 1, -1 to determine order.
     */
    _cmp(a, b) {
        if (a > b) { return -1; }
        if (b > a) { return 1; }
        return 0;
    }

    /**
     * Takes two object keys with an erroneously shared sproxyd key and repairs the older version of the two.
     * @param {Object} params - input params.
     * @param {string} params.objectKey - Object Key which was attempted to be inserted into the Map.
     * @param {string} params.existingBucketMD - Object Key with the same sproxyd Key that was already in Map.
     * @param {string} sparams.proxydKey - Share sproxyd key between the existing and new object key.
     * @param {Class} params.context - Instance of SproxydKeysProcessor from which handle was called.
     * @param {string} params.bucket - bucket name.
     * @returns {undefined}
     */
    handle(params) {
        const [objectUrl, objectUrl2] =
            [params.objectKey, params.existingObjectKey]
                .map(objectKey => this._getObjectURL(params.bucket, objectKey))
                .sort(this._cmp);

        const objInfo = {
            objectUrl,
            objectUrl2,
        };

        return this._repairObject(objInfo, err => {
            if (err) {
                log.error('an error occurred repairing object', {
                    objInfo,
                    error: { message: err.message },
                });
            }
        });
    }
}

subscribers.set('duplicateSproxyKeyFound', new DuplicateSproxydKeyFoundHandler());
module.exports = { subscribers, DuplicateSproxydKeyFoundHandler };
