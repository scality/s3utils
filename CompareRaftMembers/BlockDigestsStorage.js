const async = require('async');
const stream = require('stream');

const { ClassicLevel } = require('classic-level');

const MAX_BATCH_SIZE = 100;
const MAX_QUEUE_SIZE = 1000;

/**
 * Writable stream to store block digests coming from a
 * BlockDigestsStream instance into a LevelDB database or other
 * API-compatible storage layer
 *
 * @class BlockDigestsStorage
 */
class BlockDigestsStorage extends stream.Writable {
    /**
     * @constructor
     * @param {object} params - constructor params, must contain
     * either "levelPath" or "db" attribute
     * @param {string} [params.levelPath] - path to LevelDB database
     * to write to
     * @param {string} [params.db] - custom db object, must contain
     * the promise-based methods "batch" and "close" of classic-level -
     * currently used for unit tests
     */
    constructor(params) {
        super({ objectMode: true });
        const { levelPath, db } = params;
        if (levelPath) {
            this.db = new ClassicLevel(params.levelPath);
        } else {
            this.db = db;
        }
        this.cargo = async.cargo((tasks, cb) => {
            this.db.batch(tasks).then(() => cb(), cb);
        }, MAX_BATCH_SIZE);
    }

    _write(blockInfo, encoding, callback) {
        const { size, digest, lastKey } = blockInfo;
        this.cargo.push({
            type: 'put',
            key: lastKey, // index by last block key for efficient lookup
            value: JSON.stringify({ size, digest }),
        });
        // heuristic to have basic flow control: delay the callback
        // while queue size is above a reasonable size
        async.whilst(
            async () => this.cargo.length() > MAX_QUEUE_SIZE,
            cb => setTimeout(cb, 100),
            () => callback(),
        );
    }

    _final(callback) {
        if (this.cargo.idle()) {
            this.db.close().then(() => callback(), callback);
        } else {
            this.cargo.drain(() => {
                this.db.close().then(() => callback(), callback);
            });
        }
    }
}

module.exports = BlockDigestsStorage;
