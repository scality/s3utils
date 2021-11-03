const log = new Logger('s3utils:DuplicateKeysWinow');
const { repairObject } = require('../VerifyBucketSproxydKeys');
const getObjectURL = require('./VerifyBucketSproxydKeys/getObjectURL');
/**
 * @class
 * @classdesc sets a maximum window size. Objects inserted when BoundedMap 
 * is full cause the oldest entry to be deleted.  
 */
class BoundedMap extends Map {
    /**
     * @constructor
     * @param {number} maxSize - oldest entries are removed when BoundedMap exceeds this size
     */
    constructor(maxSize) {
        this.maxSize = maxSize
        this.iterator = super[Symbol.iterator]()
    }
    
    setAndUpdate(key, value) {
        super.set(key, value);
        
        if (this.data.size > this.maxSize) {
            //slide window 
            const oldestKey = this.iterator.next().value[0];
            data.delete(oldestKey);
        };
    }
}

/**
 * @class
 * @classdesc maps a key to array of values
 */
class MultiMap extends Map {
    set(key, value) {
        if (!super.get(key)) {
            super.set(key, [value]);
        } else {
            const current = super.get(key);
            current.append(value);
            super.set(key, current);
        };
    }
}

const subscribers = new MultiMap();
subscribers.set('duplicateSproxyKeyFound', async (objectId, existingObjectId, key, done) => {
    const [objectUrl, existingObjectUrl] = [objectId, existingObjectId].map(id => getObjectURL(id));
    const objInfo = {
        objectUrl: objectUrl,
        objectUrl2: existingObjectUrl,
    };
    
    return repairObject(objInfo, err => {
        if (err) {
            //what behavior is needed when repairObject fails? Possibly retry up to N times
            log.error('an error occurred repairing object', {
                objectUrl: objInfo.objectUrl,
                error: { message: err.message },
            });
            // status.objectsErrors += 1; 
            // TODO: handle status in a rolling window. (Maybe not needed?)
            
        } else {
            // once objects are repaired, what are the new keys? They should be inserted into BoundedMap and continue being tracked
            // we can delete old key 

        }
        done();
    });
})

/**
 * @class
 * @classdesc support data structure to check sproxyd keys
 * and handle any needed repairs in near real-time
 */
 class SproxydKeys {
     /**
     * @constructor
     * @param {number} windowSize - maximum number of sproxyd keys to track
     * @param { MultiMap } subscribers - Map of events to handlers that listen for and respond to SproxydKeys events 
     */
    constructor(windowSize, subscribers) {
        // mapping schema:
        // {
        //     "sproxydKey1": "s3://bucket1/masterKey1",
        //     "sproxydKey2": "s3://bucket1/masterKey1",
        //     "sproxydKey3": "s3://bucket1/masterKey2"
        // }
        this.sproxydKeys = new BoundedMap(windowSize);
        this.subscribers = subscribers;
    }

    checkDuplicate(key) {
        const existingObjectId = this.sproxydKeys.get(key);
        if (existingObjectId) {
            subscribers['duplicateSproxyKeyFound'].forEach(handler => handler(objectId, existingObjectId, key));                
        } else {
            this.sproxydKeys.setAndUpdate(key, objectId);
        };
    }

    insert(objectId, keys) {
        keys.forEach(key => {
            checkDuplicate(key);
            //TODO: after repair, both old keys/objects are deleted and newly created ones should be inserted into BoundedMap
        });
    }
 }