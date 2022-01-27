const assert = require('assert');
const events = require('events');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class Flusher {

    // params:
    // - stopAt: for tests
    // - flusherTimeout: Flush timeout
    // - interactive: if true flush is done manually
    constructor(bucketName, persist, persistData, params) {
        this.bucketName = bucketName;
        this.persist = persist;
        this.persistData = persistData;
        // params
        this.stopAt = -1;
        this.flusherTimeout = 5000;
        this.interactive = false;
        if (params) {
            if (params.stopAt !== undefined) {
                this.stopAt = params.stopAt;
            }
            if (params.flusherTimeout !== undefined) {
                this.flusherTimeout = params.flusherTimeout;
            }
            if (params.interactive !== undefined) {
                this.interactive = params.interactive;
            }
        }
        this.events = new events.EventEmitter();
        this.addedQueue = [];
        this.deletedQueue = [];
        this.prevOffset = null;
        this.offset = null;
        this.pending = false;
        this.logger = new werelogs.Logger('Daemon');
    }

    addEvent(item, offset, opType) {
        // console.log('addEvent', this.bucketName, item, opType);
        if (opType === 'put') {
            this.addedQueue.push(item);
        } else if (opType === 'del') {
            this.deletedQueue.push(item);
        } else {
            assert(false);
        }
        this.offset = offset;
    }

    _flushQueue(cb) {
        // console.log(this.prevOffset, this.offset);
        if (this.offset === null ||
            this.prevOffset === this.offset) {
            if (cb) {
                return process.nextTick(cb);
            }
            return undefined;
        }
        if (this.pending) {
            if (cb) {
                return process.nextTick(cb);
            }
            return undefined;
        }
        this.pending = true;
        const addedQueue = this.addedQueue;
        this.addedQueue = [];
        const deletedQueue = this.deletedQueue;
        this.deletedQueue = [];
        const offset = this.offset;
        this.prevOffset = this.offset;
        // console.log('offset/queue', this.bucketName, offset, addedQueue, deletedQueue);
        this.persistData.updateState(
            addedQueue, deletedQueue,
            err => {
                if (err) {
                    this.logger.error('error updating state', { err });
                    this.pending = false;
                    if (cb) {
                        cb(err);
                    }
                    return undefined;
                }
                this.persist.save(
                    this.bucketName,
                    this.persistData,
                    offset,
                    err => {
                        this.pending = false;
                        if (err) {
                            if (cb) {
                                cb(err);
                            }
                            return undefined;
                        }
                        if (this.stopAt !== -1) {
                            if (offset >= this.stopAt) {
                                this.events.emit('stop');
                            }
                        }
                        if (cb) {
                            return cb();
                        }
                        return undefined;
                    });
                return undefined;
            });
        return undefined;
    }

    flushQueue(cb) {
        if (this.interactive) {
            return this._flushQueue(cb);
        }
        return process.nextTick(cb);
    }

    doFlush() {
        this._flushQueue();
        setTimeout(this.doFlush.bind(this), this.flusherTimeout);
    }

    startFlusher() {
        if (!this.interactive) {
            setTimeout(this.doFlush.bind(this), this.flusherTimeout);
        }
    }
}

module.exports = Flusher;
