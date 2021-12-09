const { Logger } = require('werelogs');
const { ProxyLoggerCreator } = require('./Logging');
const log = new ProxyLoggerCreator(new Logger('ObjectRepair:DuplicateKeysWindow'));

/**
 * @class
 * @classdesc Sets a maximum window size for a Map. Objects inserted when BoundedMap
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

    /**
     * Sets new item in Map, and deletes oldest if Map.size > maxSize
     * @param {string} key - Map key.
     * @param {any} value - Value associated to key.
     * @returns {(string|null)} - Removed key or null if no key was removed.
     */
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
    /**
     * Sets a value to a list of values that share the same key.
     * @example - { key: [v1, v2, v3.. ]}
     * @param {string} key - Map key.
     * @param {any} value - Value associated to key.
     * @returns {undefined}
     */
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
    /**
     * If a duplicate Sproxyd key is found, a handler is called to generate new keys.
     * @param {string} sproxydKey - sproxyd Key.
     * @param {string} objectKey - Versioned object Key.
     * @param {string} bucket - Bucket name.
     * @returns {undefined}
     */
    checkDuplicate(sproxydKey, objectKey, bucket) {
        const existingObjectKey = this.sproxydKeys.get(sproxydKey);
        if (existingObjectKey && existingObjectKey !== objectKey) {
            log.warn('object keys with a duplicate sproxyd key found', {
                objectKeys: [existingObjectKey, objectKey],
                sproxydKey,
                eventMessage: 'duplicateFound',
            });
            const params = {
                objectKey,
                existingObjectKey,
                sproxydKey,
                context: this,
                bucket,
            };
            const handlers = this.subscribers.get('duplicateSproxydKeyFound');

            if (handlers) {
                for (const handler of handlers) {
                    handler.handle(params);
                }
            }
        } else {
            this.sproxydKeys.setAndUpdate(sproxydKey, objectKey);
        }
    }

    /**
     * Checks if a duplicate Exists for each Sproxyd Key.
     * @param {string} objectKey - Object Key.
     * @param {Array.<string>} sproxydKeys - Array of Sproxyd Keys for associated Object key.
     * @param {string} bucket - bucket name.
     * @returns {undefined}
     */
    insert(objectKey, sproxydKeys, bucket) {
        for (const sproxydKey of sproxydKeys) {
            this.checkDuplicate(sproxydKey, objectKey, bucket);
        }
    }
 }

module.exports = { SproxydKeysProcessor, MultiMap, BoundedMap };
