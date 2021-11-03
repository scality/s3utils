const log = new Logger('s3utils:DuplicateKeysWinow');

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
        const objectId2 = this.sproxydKeys.get(key);
            if (objectId2) {
                // duplicate found
                const dupInfo = { objectId: objectId2, key };
                
                // log.error('duplicate sproxyd key found', {
                //     objectUrl: objectId,
                //     objectUrl2: dupInfo.objectId,
                //     sproxydKey: dupInfo.key,
                // });
                subscribers['duplicateSproxyKeyFound'].forEach(handler => handler(dupInfo));
                //TODO: pass directly into repairDuplicateVersions:repairObject bypassing logging/reading from log
                
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