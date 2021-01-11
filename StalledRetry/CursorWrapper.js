const { encode } = require('arsenal').versioning.VersionID;

function objectToEntries(bucketName, cmpDate, data) {
    if (!data || typeof data !== 'object' ||
        !data.value || typeof data.value !== 'object') {
        return [];
    }

    const time = data.value['last-modified'] || null;
    if (isNaN(Date.parse(time))) {
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
        return {
            Bucket: bucketName,
            Key: data._id.key,
            VersionId: encode(data._id.versionId),
            StorageClass: storageClass,
            ForceRetry: true,
        };
    });
}

function randomNumber(min, max) {
    return Math.floor(Math.random() * (max - min) + min);
}

class StalledCursorWrapper {
    constructor(cursor, params) {
        this.info = {
            stalled: 0,
        };

        this.bucketName = params.bucketName;
        this.cmpDate = params.cmpDate;
        this.log = params.log;

        this.queueLimit = params.queueLimit || 1000;

        this.queue = [];
        this.cursor = cursor;
        this.cursorEnd = false;
        this.cursorErr = null;

        this.completed = false;

        this._init();
    }

    _init() {
        this.cursor.on('data', data => {
            const entries = objectToEntries(
                this.bucketName,
                this.cmpDate,
                data) || [];

            if (entries.length > 0) {
                this.info.stalled += entries.length;

                this.queue.push(...entries);

                if (this.queue.length >= this.queueLimit) {
                    this.cursor.pause();
                }
            }
        });

        this.cursor.on('end', () => {
            this.log.debug('reached end of cursor', {
                inQueue: this.queue.length,
            });
            this.cursorEnd = true;
        });

        this.cursor.on('error', err => {
            this.log.error('encountered error while reading cursor', {
                error: err,
            });

            this.cursorEnd = true;
            this.cursorErr = err;
            this.cleanUp();
        });
    }

    getInfo() {
        return this.info;
    }

    cleanUp() {
        if (this.cursor && typeof this.cursor.destroy === 'function') {
            this.cursor.destroy();
        }

        this.cursor = null;
        this.queue = [];
        this.completed = true;
    }

    getBatch(size, cb) {
        if (this.cursorErr !== null) {
            return cb(this.cursorErr, null);
        }

        if (this.completed) {
            return cb(null, null);
        }

        if (this.cursorEnd || this.queue.length >= size) {
            const batch = this.queue.splice(0, size);

            // resume cursor if it is paused
            if (this.queue.length < this.queueLimit && this.cursor.isPaused()) {
                this.cursor.resume();
            }

            // set wrapper to completed
            if (this.cursorEnd && this.queue.length === 0) {
                this.cleanUp();
            }

            return cb(null, batch);
        }

        return setTimeout(
            () => this.getBatch(size, cb),
            randomNumber(100, 500));
    }
}

module.exports = {
    objectToEntries,
    StalledCursorWrapper,
};
