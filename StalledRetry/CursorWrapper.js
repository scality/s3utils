const { MongoError } = require('mongodb');
const { isMasterKey, VersionID: { encode } } = require('arsenal').versioning;
const { once } = require('arsenal').jsutil;

class StalledEntry {
    constructor(bucketName, key, versionId, storageClass, forceRetry) {
        this.bucketName = bucketName;
        this.key = key;
        this.versionId = versionId;
        this.storageClass = storageClass;
        this.forceRetry = forceRetry;
    }

    toObject() {
        return {
            Bucket: this.bucketName,
            Key: this.key,
            VersionId: this.versionId,
            StorageClass: this.storageClass,
            ForceRetry: this.forceRetry,
        };
    }
}

function objectToEntries(bucketName, cmpDate, data) {
    if (!data || typeof data !== 'object'
        || !data.value || typeof data.value !== 'object') {
        return [];
    }

    // skip if master
    const docId = data._id.id;
    if (!docId || isMasterKey(docId)) {
        return [];
    }

    const time = data.value['last-modified'] || null;
    if (Number.isNaN(Date.parse(time))) {
        return [];
    }

    const testDate = new Date(time);
    const withinRange = testDate <= cmpDate;
    if (!withinRange) {
        return [];
    }

    const storageClasses = data._id.storageClasses.split(',');
    return storageClasses.map(i => {
        const storageClass = i.split(':')[0];
        return new StalledEntry(
            bucketName,
            data._id.key,
            encode(data._id.versionId),
            storageClass,
            true,
        );
    });
}

class RateLimitingCursor {
    constructor(cursor, params) {
        this.info = {
            stalled: 0,
        };

        this.bucketName = params.bucketName;
        this.cmpDate = params.cmpDate;
        this.log = params.log;

        this.queueLimit = params.queueLimit || 1000;

        this.buffer = [];
        this.getBatchCallbacks = [];
        this.cursor = cursor;
        this.cursorErr = null;
        this.cursorEnd = false;

        this.completed = false;

        this._init();
    }

    _init() {
        this.cursor.on('data', data => {
            const entries = objectToEntries(
                this.bucketName,
                this.cmpDate,
                data,
            ) || [];

            if (entries.length === 0) {
                return;
            }

            this.info.stalled += entries.length;
            this.buffer.push(...entries);

            this._fulfillGetBatch();

            if (this.buffer.length >= this.queueLimit) {
                this.cursor.pause();
            }
        });

        this.cursor.on('end', () => {
            this.log.debug('reached end of cursor', {
                inQueue: this.buffer.length,
            });
            this.cursorEnd = true;
            this._cleanUp();
        });

        this.cursor.on('error', err => {
            this.log.error('encountered error while reading cursor', {
                error: err,
            });

            if (
                err instanceof MongoError
                && err.errorLabels.includes('TransientTransactionError')
            ) {
                this.log.info('transient error, continue reading from cursor');
                return;
            }

            this.cursorEnd = true;
            this.cursorErr = err;

            this.buffer = [];
            this._cleanUp();
        });
    }

    _fulfillGetBatch() {
        if (this.getBatchCallbacks.length === 0) {
            return;
        }

        while (this.buffer.length > 0 && this.getBatchCallbacks.length > 0) {
            const i = this.getBatchCallbacks.length - 1;
            // TODO: implement a min-heap for callback on size
            // atm, size is the same for all getBatch requests, so only the
            // last items is checked
            if (this.getBatchCallbacks[i].size <= this.buffer.length) {
                const { size, cb } = this.getBatchCallbacks.pop();
                const batch = this.buffer.splice(0, size);
                if (batch.length === 0) {
                    cb(null, null);
                } else {
                    cb(null, batch);
                }
            } else {
                break;
            }
        }

        if (this.cursor.isPaused() && this.buffer.length < this.queueLimit) {
            this.cursor.resume();
        }

        if (this.cursorEnd && this.buffer.length === 0) {
            this._cleanUp();
        }
    }

    getInfo() {
        return this.info;
    }

    _cleanUp() {
        if (this.cursor && typeof this.cursor.destroy === 'function') {
            this.cursor.destroy();
            this.cursor = null;
        }

        this.getBatchCallbacks.forEach(({ size, cb }) => {
            if (this.cursorErr !== null) {
                return cb(this.cursorErr, null);
            }

            if (this.completed) {
                return cb(null, null);
            }

            const batch = this.buffer.splice(0, size);
            if (this.buffer.length === 0) {
                this.completed = true;
            }

            if (batch.length === 0) {
                return cb(null, null);
            }

            return cb(null, batch);
        });

        this.getBatchCallbacks = [];
        if (this.buffer.length === 0) {
            this.completed = true;
        }
    }

    getBatch(size, cb) {
        if (this.cursorErr !== null) {
            cb(this.cursorErr, null);
            return;
        }

        if (this.completed) {
            cb(null, null);
            return;
        }

        if (size === 0) {
            cb(null, []);
            return;
        }

        this.getBatchCallbacks.push({ size, cb: once(cb) });

        if (this.cursor && this.cursor.isPaused()) {
            this._fulfillGetBatch();
        }

        if (this.cursorEnd) {
            this._cleanUp();
        }
    }
}

module.exports = {
    objectToEntries,
    RateLimitingCursor,
    StalledEntry,
};
