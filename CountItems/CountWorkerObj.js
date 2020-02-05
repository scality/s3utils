const uuid = require('node-uuid');
const { once } = require('arsenal').jsutil;

class CountWorkerObj {
    constructor(id, worker) {
        this.id = id;
        this.ready = false;
        this.clientConnected = false;
        this.callbacks = new Map();
        this._worker = worker;
        this._init();
    }

    _init() {
        this._worker.on('online', () => {
            this.ready = true;
            const cb = this._getCallback('readyCheck');
            if (typeof cb === 'function') {
                cb();
            }
        });
        this._worker.on('message', data => {
            if (data.owner !== 'scality') return;
            const cb = this._getCallback(data.id);
            if (!cb) return;
            switch (data.type) {
            case 'setup':
                if (data.status === 'passed') {
                    this.clientConnected = true;
                    cb();
                } else if (data.status === 'failed') {
                    this.clientConnected = false;
                    cb(new Error(data.error));
                }
                break;
            case 'teardown':
                if (data.status === 'passed') {
                    this.clientConnected = false;
                    cb();
                } else if (data.status === 'failed') {
                    cb(new Error(data.error));
                }
                break;
            case 'count':
                if (data.status === 'passed') {
                    cb(null, data.results);
                } else if (data.status === 'failed') {
                    cb(new Error(data.error));
                }
                break;
            default:
                break;
            }
        });
        this._worker.on('exit', (code, signal) => {
            this.clientConnected = false;
            this.callbacks.forEach(({ type, callback }) => {
                switch (type) {
                case 'readyCheck': // fallthrough
                case 'setup': // fallthrough
                case 'count': // fallthrough
                    if ((code !== null && code !== 0) || signal !== null) {
                        callback(new Error('unexpectedExit'));
                    } else {
                        callback();
                    }
                    break;
                case 'teardown': // fallthrough
                default:
                    callback();
                    break;
                }
            });
            this.callbacks.clear();
        });
    }

    _addCallback(id, type, callback) {
        this.callbacks.set(id, { type, callback: once(callback) });
    }

    _getCallback(id) {
        if (!this.callbacks.has(id)) return null;
        const ret = this.callbacks.get(id);
        this.callbacks.delete(id);
        return ret.callback;
    }

    init(callback) {
        if (this.ready) {
            return callback();
        }
        return this._addCallback('readyCheck', 'readyCheck', callback);
    }

    setup(callback) {
        const id = uuid.v4();
        this._addCallback(id, 'setup', callback);
        this._worker.send({
            id,
            owner: 'scality',
            type: 'setup',
        });
    }

    teardown(callback) {
        if (!this.clientConnected || !this._worker.isConnected()) {
            return callback();
        }
        const id = uuid.v4();
        this._addCallback(id, 'teardown', callback);
        return this._worker.send({
            id,
            owner: 'scality',
            type: 'teardown',
        });
    }

    count(bucketInfo, callback) {
        const id = uuid.v4();
        this._addCallback(id, 'count', callback);
        this._worker.send({
            id,
            owner: 'scality',
            type: 'count',
            bucketInfo,
        });
    }

    kill() {
        if (this._worker.process &&
            typeof this._worker.process.kill === 'function') {
            this._worker.process.kill();
            return;
        }
        if (typeof this._worker.kill === 'function') {
            this._worker.kill();
            return;
        }
        return;
    }
}

module.exports = CountWorkerObj;
