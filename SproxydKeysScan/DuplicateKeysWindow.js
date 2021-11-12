const { Logger } = require('werelogs');
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
        super();
        this.maxSize = maxSize;
        this.iterator = super[Symbol.iterator]();
    }

    setAndUpdate(key, value) {
        super.set(key, value);

        let removedKey = null;
        if (super.size > this.maxSize) {
            // slide window and set key to remove
            const oldestKey = this.iterator.next().value[0];
            super.delete(oldestKey);
            removedKey = oldestKey;
        }
        return removedKey;
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
            current.push(value);
            super.set(key, current);
        }
    }
}

/**
 * @class
 * @classdesc support data structure to check sproxyd keys
 * and handle any needed repairs in near real-time
 */
class SproxydKeysProcessor {
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

    checkDuplicate(key, objectId) {
        const existingObjectId = this.sproxydKeys.get(key);
        if (existingObjectId && existingObjectId !== objectId) {
            log.info(`existing object key found: Existing { key: ${key}, id: ${existingObjectId} } 
                Current { key: ${key}, id: ${objectId}}`);
            const params = {
                objectId,
                existingObjectId,
                key,
                context: this,
            };
            if (this.subscribers.get('duplicateSproxyKeyFound')) {
                this.subscribers.get('duplicateSproxyKeyFound').forEach(handler => handler.handle(params));
            }
        } else {
            this.sproxydKeys.setAndUpdate(key, objectId);
        }
    }

    insert(objectId, keys) {
        keys.forEach(key => {
            this.checkDuplicate(key, objectId);
        });
    }
 }

module.exports = { SproxydKeysProcessor, MultiMap, BoundedMap };
