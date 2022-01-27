// fake backend for unit tests
const assert = require('assert');
const MemoryStream = require('memorystream');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class PersistMemInterface {

    constructor() {
        this.memoryStreams = {};
        this.offsets = {};
        this.logger = new werelogs.Logger('PersistMemInterface');
    }

    getOffset(bucketName) {
        if (!this.offsets[bucketName]) {
            return {};
        }
        return this.offsets[bucketName];
    }

    setOffset(bucketName, offset) {
        if (!this.memoryStreams[bucketName]) {
            this.memoryStreams[bucketName] = new MemoryStream();
        }
        if (!this.offsets[bucketName]) {
            this.offsets[bucketName] = {};
        }
        Object.assign(
            this.offsets[bucketName],
            offset);
    }

    // cb(err, offset)
    load(bucketName, persistData, cb) {
        this.logger.info(`loading ${bucketName}`);
        const stream = this.memoryStreams[bucketName];
        const offset = this.offsets[bucketName];
        if (stream === undefined) {
            this.logger.info(`${bucketName} non-existent`);
            return cb(null, undefined);
        }
        assert(offset !== undefined);
        persistData.loadState(stream, err => {
            if (err) {
                return cb(err);
            }
            this.logger.info(`${bucketName} loaded: offset ${offset}`);
            return cb(null, offset);
        });
        return undefined;
    }

    // cb(err)
    save(bucketName, persistData, offset, cb) {
        this.logger.info(`saving ${bucketName} offset ${JSON.stringify(offset)}`);
        const stream = new MemoryStream();
        this.memoryStreams[bucketName] = stream;
        persistData.saveState(stream, err => {
            if (err) {
                return cb(err);
            }
            this.offsets[bucketName] = offset;
            this.logger.info(`${bucketName} saved: offset ${offset}`);
            return cb();
        });
    }
}

module.exports = PersistMemInterface;

