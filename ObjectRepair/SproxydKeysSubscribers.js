const { MultiMap } = require('./DuplicateKeysWindow');
const { repairObject } = require('../repairDuplicateVersionsSuite');
const { Logger } = require('werelogs');
const { ProxyLoggerCreator } = require('./Logging');
const getObjectURL = require('../VerifyBucketSproxydKeys/getObjectURL');
const { queue } = require('async');

const log = new ProxyLoggerCreator(new Logger('ObjectRepair:SproxydKeysSubscribers'));
const subscribers = new MultiMap();

/**
 * @class
 * @classdesc Handler for duplicate sproxyd key found events.
 */
class DuplicateSproxydKeyFoundHandler {
    constructor() {
        this._repairObject = repairObject;
        this._getObjectURL = getObjectURL;
        this.queue = queue(this._repairObject, 1);
    }

    /**
     * Takes two object keys with an erroneously shared sproxyd key and repairs the older version of the two.
     * @param {Object} params - input params.
     * @param {string} params.objectKey - Object Key which was attempted to be inserted into the Map.
     * @param {string} params.existingObjectKey - Object Key with the same sproxyd Key that was already in Map.
     * @param {string} params.sproxydKey - Shared sproxyd key between the existing and new object key.
     * @param {Class} params.context - Instance of SproxydKeysProcessor from which handle was called.
     * @param {string} params.bucket - bucket name.
     * @returns {undefined}
     */
    handle(params) {
        // The largest string is last (which is the older version).
        // Older version is chosen to repair.
        const [newerVersionKey, olderVersionKey] =
            [params.objectKey, params.existingObjectKey]
            .sort();

        // remove older version, insert newer version into sproxyd key map
        params.context.sproxydKeys.set(params.sproxydKey, newerVersionKey);

        const [objectUrl, objectUrl2] =
        [olderVersionKey, newerVersionKey]
                .map(objectKey => this._getObjectURL(params.bucket, objectKey));

        const objInfo = {
            objectUrl,
            objectUrl2,
        };


        this.queue.push(objInfo, err => {
            if (err) {
                if (err.code && err.code === 404) {
                    log.info('object deleted before repair', {
                        objInfo,
                        error: { message: err.message },
                        eventMessage: 'objectDeletedBeforeRepair',
                    });
                } else {
                    log.error('an error occurred repairing object', {
                        objInfo,
                        error: { message: err.message },
                        eventMessage: 'repairObjectFailure',
                    });
                }
            } else {
                log.info('object repaired', { objInfo, eventMessage: 'repairObjectSuccess' });
            }
        });
    }
}

subscribers.set('duplicateSproxydKeyFound', new DuplicateSproxydKeyFoundHandler());
module.exports = { subscribers, DuplicateSproxydKeyFoundHandler };
