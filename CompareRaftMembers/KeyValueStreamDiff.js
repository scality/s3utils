const async = require('async');
const stream = require('stream');

const { jsutil } = require('arsenal');

/**
 * Output differences between two streams, where each stream emits
 * { key: string, value: string } objects sorted by key.
 *
 * The output consists of entries of one of the following types:
 *
 * - [{ key, value }, null]: this key is present on stream #1 but not
 *   on stream #2
 *
 * - [null, { key, value }]: this key is not present on stream #1 but
 *   is present on stream #2
 *
 * - [{ key, value: "{value1}" }, { key, value: "{value2}" }]: this key
 *   has a different value between stream #1 and stream #2: "value1"
 *   is the value seen on stream #1 and "value2" the value seen on
 *   stream #2.
 *
 * @class KeyValueStreamDiff
 */
class KeyValueStreamDiff extends stream.Readable {
    /**
     * @constructor
     * @param {stream.Readable} stream1 - first stream to compare
     * @param {stream.Readable} stream2 - second stream to compare
     */
    constructor(stream1, stream2) {
        super({ objectMode: true });

        this._streams = [stream1, stream2];
        this._lastItems = [null, null];
        this._eof = [false, false];
        this._reading = false;

        this._streams.forEach((stream, streamIndex) => {
            stream
                .on('data', data => {
                    this._lastItems[streamIndex] = data;
                    stream.pause();
                    this._checkDiff();
                })
                .on('end', () => {
                    this._eof[streamIndex] = true;
                    this._checkDiff();
                })
                .on('error', err => {
                    // eslint-disable-next-line no-console
                    console.error(`error from stream #${streamIndex}: ${err.message}`);
                    this.destroy(err);
                });
        });
    }

    _read() {
        if (!this._reading) {
            this._reading = true;
            this._checkDiff();
        }
    }

    _consumedItem(streamIndex) {
        this._lastItems[streamIndex] = null;
        this._streams[streamIndex].resume();
    }

    _pushDiff(item1, item2) {
        this.push([item1, item2]);
        this._reading = false;
    }

    _checkDiff() {
        // bail out if:
        // - consumer not actively reading
        // - or at least one of the input streams is waiting for an item to read
        if (!this._reading
            || (!this._eof[0] && !this._lastItems[0])
            || (!this._eof[1] && !this._lastItems[1])) {
            return undefined;
        }
        // compute the difference between the last item read from each
        // stream (or null if EOF was reached)
        if (this._lastItems[0] && this._lastItems[1]) {
            if (this._lastItems[0].key < this._lastItems[1].key) {
                this._pushDiff(this._lastItems[0], null);
                this._consumedItem(0);
            } else if (this._lastItems[0].key > this._lastItems[1].key) {
                this._pushDiff(null, this._lastItems[1]);
                this._consumedItem(1);
            } else {
                if (this._lastItems[0].value !== this._lastItems[1].value) {
                    this._pushDiff(this._lastItems[0], this._lastItems[1]);
                }
                this._consumedItem(0);
                this._consumedItem(1);
            }
        } else if (this._lastItems[0]) {
            this._pushDiff(this._lastItems[0], null);
            this._consumedItem(0);
        } else if (this._lastItems[1]) {
            this._pushDiff(null, this._lastItems[1]);
            this._consumedItem(1);
        } else {
            this.push(null);
        }
        return undefined;
    }
}

module.exports = KeyValueStreamDiff;
