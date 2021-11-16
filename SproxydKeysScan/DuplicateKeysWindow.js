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
     * @param { number } windowSize - maximum number of sproxyd keys to track
     * @param { MultiMap } subscribers - Map of events to handlers that listen for and respond to SproxydKeys events
     */
    constructor(windowSize, subscribers) {
        // { sproxydKey: objectKey }
        this.sproxydKeys = new BoundedMap(windowSize);
        this.subscribers = subscribers;
    }

    checkDuplicate(sproxydKey, objectKey, bucket) {
        const existingObjectKey = this.sproxydKeys.get(sproxydKey);
        if (existingObjectKey && existingObjectKey !== objectKey) {
            log.info(`existing object key found: 
                Existing { sproxydKey: ${sproxydKey}, objectKey: ${existingObjectKey} } 
                Current { sproxydKey: ${sproxydKey}, objectKey: ${objectKey}}`
            );
            const params = {
                objectKey,
                existingObjectKey,
                sproxydKey,
                context: this,
                bucket,
            };
            const handlers = this.subscribers.get('duplicateSproxyKeyFound');

            if (handlers) {
                for (const handler of handlers) {
                    handler.handle(params);
                }
            }
        } else {
            this.sproxydKeys.setAndUpdate(sproxydKey, objectKey);
        }
    }

    insert(objectKey, sproxydKeys, bucket) {
        for (const sproxydKey of sproxydKeys) {
            this.checkDuplicate(sproxydKey, objectKey, bucket);
        }
    }
 }

module.exports = { SproxydKeysProcessor, MultiMap, BoundedMap };
