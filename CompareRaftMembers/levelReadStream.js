const { Readable } = require('stream');

// classic-level has no createReadStream(): stream the database entries as
// the { key, value } objects the comparison streams consume. Readable.from()
// would do, but it still emits 'end' when destroyed from a 'data' handler,
// which DiffStream does when it drops a digests stream.
class LevelReadStream extends Readable {
    constructor(db, options) {
        super({ objectMode: true });
        this.dbIterator = db.iterator(options);
    }

    _read() {
        this.dbIterator.next().then(entry => {
            if (this.destroyed) {
                return;
            }
            if (entry === undefined) {
                this.push(null);
            } else {
                const [key, value] = entry;
                this.push({ key, value });
            }
        }, err => this.destroy(err));
    }

    _destroy(err, callback) {
        this.dbIterator.close().then(() => callback(err), callback);
    }
}

module.exports = (db, options) => new LevelReadStream(db, options);
