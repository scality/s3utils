const log = new Logger('s3utils:DuplicateKeysWinow');

class BoundedMap {
    /**
     * @constructor
     * @param {number} maxSize - oldest entries are removed when BoundedMap exceeds this size
     */
    constructor(maxSize, initialKeys) {
        this.data = new Map()
        this.maxSize = maxSize
        this.iterator = this.data[Symbol.iterator]()
    }
    
    setAndUpdate(key, value) {
        this.data.set(key, value)
        if (this.data.size > this.maxSize) {
            const oldestKey = this.iterator.next().value[0]
            data.delete(oldestKey)
        }
    }

    get(key) {
        return this.data.get(key)
    }

    has(key) {
        return this.data.has.key()
    }
}

/**
 * @class
 * @classdesc support data structure to check duplicate sproxyd keys:
 * when a sproxyd key that we found and want to insert already
 * exists in this mapping, it's a duplicate.
 */
 class SproxydKeys {
     /**
     * @constructor
     * @param {number} windowSize - maximum number of sproxyd keys to track
     * @param {Map { <string>: Array<Function>} } subscribersMap - Map of events to functions that listen for and respond to SproxydKeys events 
     */
    constructor(windowSize, subscribers) {
        // mapping schema:
        // {
        //     "sproxydKey1": "s3://bucket1/masterKey1",
        //     "sproxydKey2": "s3://bucket1/masterKey1",
        //     "sproxydKey3": "s3://bucket1/masterKey2"
        // }
        this.sproxydKeys = new BoundedMap(windowSize);
        this.subscribersMap = subscribersMap;
    }

    checkDuplicate(key) {
        const objectId2 = this.sproxydKeys.get(key);
            if (objectId2) {
                // duplicate found
                dupInfo = { objectId: objectId2, key };
                
                if (dupInfo) {
                    // log.error('duplicate sproxyd key found', {
                    //     objectUrl: objectId,
                    //     objectUrl2: dupInfo.objectId,
                    //     sproxydKey: dupInfo.key,
                    // });

                    //TODO: pass directly into repairDuplicateVersions:repairObject bypassing logging/reading from log
                }
            } else {
                this.sproxydKeys.setAndUpdate(key, objectId);
            }
    }

    insert(objectId, keys) {
        keys.forEach(key => {
            checkDuplicate(key);
            //TODO: what's the behavior after repair process is started? Does setAndUpdate get called when repair is finished or earlier?
        });
    }
 }