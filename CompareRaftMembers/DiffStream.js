const stream = require('stream');

const BucketStream = require('./BucketStream');

/**
 * Output differences between the input stream, consisting of
 * { key: string, value: string } objects sorted by key, and a bucketd
 * interface that can be queried via HTTP for ranges of keys from
 * given buckets.
 *
 * Input items are meant to be coming from a raw leveldb database
 * listing, transformed via a DBListStream object to be comparable with
 * bucketd listing.
 *
 * @class DiffStream
 */
class DiffStream extends stream.Transform {
    /**
     * @constructor
     * @param {object} params - constructor params
     * @param {string} params.bucketdHost - bucketd host name or IP address
     * @param {number} params.bucketdPort - bucketd API port
     * @param {Level} [params.digestsDb] - optional leveldb database
     * handle where block digests precomputed from the leader's
     * bucketd listing are stored, to speed up comparisons
     * @param {number} [params.maxBufferSize=1000] - maximum number of
     * items to bufferize
     * @param {class} [params.BucketStreamClass=BucketStream] -
     * override the BucketStream class, used for unit tests only
     */
    constructor(params) {
        super({ objectMode: true });
        const {
            bucketdHost, bucketdPort,
            digestsDb,
            maxBufferSize,
            BucketStreamClass,
        } = params;

        this.bucketdHost = bucketdHost;
        this.bucketdPort = bucketdPort;
        this.digestsDb = digestsDb || null;
        this.maxBufferSize = maxBufferSize || 1000;
        this.BucketStreamClass = BucketStreamClass || BucketStream;

        this.inputBuffer = [];
        this.inputBufferBucket = null;
        this.currentMarker = null;

        this.currentItem = null;
        this.currentDbDigestsStream = null;
        this.currentDbDigestBlock = null;
        this.currentDbDigestBlockWaitCallback = null;
        this.noMoreDigestsForCurrentBucket = false;
    }

    cleanup(callback) {
        if (this.currentDbDigestsStream) {
            this.currentDbDigestsStream.destroy();
            this.currentDbDigestsStream.on('close', callback);
        } else {
            callback();
        }
    }

    _transform(item, encoding, callback) {
        const itemInfo = this._parseItem(item);
        // synchronize the input key with the digests database
        this._getDigestBlockForItem(itemInfo, digestBlock => {
            this._processItem(itemInfo, digestBlock, callback);
        });
    }

    _flush(callback) {
        if (this.inputBuffer.length > 0) {
            this._compareInputBufferWithBucketd(null, callback);
        } else {
            callback();
        }
    }

    /**
     * Parse the item coming from the stream input into a series of
     * fields useful for later processing
     *
     * @param {object} item - item from DBListStream
     * @return {object} - { fullKey, bucketName, objectKey, value }
     */
    _parseItem(item) {
        const { key: fullKey, value } = item;
        const slashIndex = fullKey.indexOf('/');
        const [bucketName, objectKey] = [fullKey.slice(0, slashIndex), fullKey.slice(slashIndex + 1)];
        return {
            fullKey,
            bucketName,
            objectKey,
            value,
        };
    }

    /**
     * Process a parsed item from the database input stream
     *
     * @param {object} itemInfo - parsed item (see _parseItem())
     * @param {object} digestBlock - digest block used for efficient
     * comparison purpose
     * @param {function} callback - called with no argument when the
     * transform stream is ready to accept more input
     * @return {undefined}
     */
    _processItem(itemInfo, digestBlock, callback) {
        const { bucketName } = itemInfo;

        const isNewBucket = (this.inputBufferBucket && bucketName !== this.inputBufferBucket);
        if (isNewBucket || this.inputBuffer.length === this.maxBufferSize) {
            const lastObjectKey = isNewBucket
                ? null
                : this.inputBuffer[this.inputBuffer.length - 1].objectKey;
            this._compareInputBufferWithBucketd(lastObjectKey, () => {
                this.inputBufferBucket = bucketName;
                this.currentMarker = lastObjectKey;
                this._ingestItem(itemInfo, digestBlock, callback);
            });
        } else {
            this.inputBufferBucket = bucketName;
            // due to the workaround described above, we may be
            // processing a new bucket's key here and need to reset
            // the marker in this case only, without querying bucketd
            if (isNewBucket) {
                this.currentMarker = null;
            }
            this._ingestItem(itemInfo, digestBlock, callback);
        }
    }

    /**
     * Compare the current input buffer populated from the database
     * input stream with bucketd, by requesting bucketd on the
     * appropriate ranges for key-by-key comparison, and output
     * differences as readable stream data (see _compareBuffers() for
     * details)
     *
     * @param {string|null} lastKey - key where the comparison should
     * stop, or null for checking the rest of the bucket
     * @param {function} callback - called with no argument when the
     * comparison is finished
     * @return {undefined}
     */
    _compareInputBufferWithBucketd(lastKey, callback) {
        const bucketStream = new this.BucketStreamClass({
            bucketdHost: this.bucketdHost,
            bucketdPort: this.bucketdPort,
            bucketName: this.inputBufferBucket,
            marker: this.currentMarker,
            lastKey,
        });
        let bucketBuffer = [];
        bucketStream
            .on('data', item => {
                bucketBuffer.push(item);
                // split the comparison job in chunks if the bucket
                // buffer gets too big: for this we may also need to
                // split the input buffer at the last bucket buffer's
                // key, so that the comparison is meaningful
                if (bucketBuffer.length === this.maxBufferSize) {
                    const maxKey = bucketBuffer[bucketBuffer.length - 1].key;
                    let inputBufferLimit = this.inputBuffer.findIndex(item => item.fullKey > maxKey);
                    if (inputBufferLimit === -1) {
                        inputBufferLimit = this.inputBuffer.length;
                    }
                    const inputBuffer = this.inputBuffer.splice(0, inputBufferLimit);
                    this._compareBuffers(inputBuffer, bucketBuffer);
                    bucketBuffer = [];
                }
            })
            .on('end', () => {
                const { inputBuffer } = this;
                this.inputBuffer = [];
                this._compareBuffers(inputBuffer, bucketBuffer);
                callback();
            })
            .on('error', err => {
                // unrecoverable error after retries: raise the error
                this.emit('error', err);
            });
    }

    /**
     * Compare the input buffer built from streamed input coming from
     * the database against the bucket buffer built from querying
     * bucketd for ranges of keys, and output the differences as
     * readable stream data.
     *
     * Each output data event is an array in one of the following forms:
     *
     * - [{ key, value }, null]: this key is only present in inputBuffer
     *
     * - [null, { key, value}]: this key is only present in the bucketBuffer
     *
     * - [{ key, value: 'value1' }, { key, value: 'value2' }]: this
     *   key is present in both buffers but has a different value,
     *   where 'value1' is the inputBuffer value and 'value2' the
     *   bucketBuffer value
     *
     * @param {Array} inputBuffer - entries coming from the database input stream
     * @param {Array} bucketBuffer - entries coming from a listing
     * request to bucketd (via BucketStream)
     * @return {undefined}
     */
    _compareBuffers(inputBuffer, bucketBuffer) {
        let [inputIndex, bucketIndex] = [0, 0];
        while (inputIndex < inputBuffer.length
               && bucketIndex < bucketBuffer.length) {
            const [inputItem, bucketItem] = [inputBuffer[inputIndex], bucketBuffer[bucketIndex]];
            if (inputItem.fullKey < bucketItem.key) {
                this.push([{ key: inputItem.fullKey, value: inputItem.value }, null]);
                inputIndex += 1;
            } else if (inputItem.fullKey > bucketItem.key) {
                this.push([null, bucketItem]);
                bucketIndex += 1;
            } else {
                if (inputItem.value !== bucketItem.value) {
                    this.push([{ key: inputItem.fullKey, value: inputItem.value }, bucketItem]);
                }
                inputIndex += 1;
                bucketIndex += 1;
            }
        }
        while (inputIndex < inputBuffer.length) {
            const inputItem = inputBuffer[inputIndex];
            this.push([{ key: inputItem.fullKey, value: inputItem.value }, null]);
            inputIndex += 1;
        }
        while (bucketIndex < bucketBuffer.length) {
            const bucketItem = bucketBuffer[bucketIndex];
            this.push([null, bucketItem]);
            bucketIndex += 1;
        }
    }

    _ingestItem(itemInfo, digestBlock, callback) {
        const { fullKey } = itemInfo;
        this.inputBuffer.push(itemInfo);
        // TODO compute block digests and compare with digestBlock
        return callback();
    }

    /**
     * Retrieve the digest block corresponding to the item to process,
     * i.e. the block available from the digests database with the
     * closest higher or equal "lastKey" attribute that belongs to the
     * same bucket
     *
     * @param {object} itemInfo - parsed item
     * @param {function} callback - callback(digestBlock
     * {object|null}): called when the digest block is found and
     * retrieved, with attributes { lastKey, digest, size }, or null
     * if no digest block matches the provided itemInfo
     * @return {undefined}
     */
    _getDigestBlockForItem(itemInfo, callback) {
        if (this.currentItem && this.currentItem.bucketName !== itemInfo.bucketName) {
            this.noMoreDigestsForCurrentBucket = false;
        }
        this.currentItem = itemInfo;
        if (!this.digestsDb) {
            return callback(null);
        }
        const digestBlock = this.currentDbDigestBlock;
        if (digestBlock && itemInfo.fullKey <= digestBlock.lastKey) {
            if (itemInfo.fullKey === digestBlock.lastKey) {
                this.currentDbDigestBlock = null;
                if (this.currentDbDigestsStream) {
                    // resume the current digests stream to start
                    // fetching the next block asynchronously to
                    // prepare for the next key
                    this.currentDbDigestsStream.resume();
                }
            }
            return callback(digestBlock);
        }
        this.currentDbDigestBlock = null;
        if (this.noMoreDigestsForCurrentBucket) {
            return callback(null);
        }
        // register this callback to be called when the next block comes in
        this.currentDbDigestBlockWaitCallback = callback;

        if (!this.currentDbDigestsStream) {
            // no digests stream exist yet: create it
            this._createDigestsDbStream(itemInfo);
        } else if (this.currentDbDigestsStream.isPaused()) {
            // there is a digests stream but it's not actively
            // fetching, which means that we did not see a key
            // matching the current block's last key: we need to
            // re-create a new stream starting from the current key
            this.currentDbDigestsStream.destroy();
            this._createDigestsDbStream(itemInfo);
        }
        // else if the digests db stream is actively fetching, nothing
        // else to do than waiting for the next block to be read
        return undefined;
    }

    /**
     * Helper to create a new digests database read stream starting
     * from the given item, used to provide digest information to
     * _getDigestBlockForItem()
     *
     * @param {object} fromItem - parsed item from which the digests
     * stream should start
     * @return {undefined}
     */
    _createDigestsDbStream(fromItem) {
        this.currentDbDigestsStream = this.digestsDb.createReadStream({
            gte: fromItem.fullKey,
            // '0' is the ASCII character following '/': this restricts
            // the digest's lastKey to be from the current bucket
            lt: `${fromItem.bucketName}0`,
        });
        this.currentDbDigestsStream
            .on('data', ({ key: blockLastKey, value: blockValue }) => {
                // we may encounter the case where the requested key
                // is past the next streamed block: drop this stream
                // and re-create a new stream from the requested key
                // in this case
                if (this.currentItem.fullKey > blockLastKey) {
                    this.currentDbDigestsStream.destroy();
                    return this._createDigestsDbStream(this.currentItem);
                }
                // only pause the stream if the requested key is
                // strictly lower than the block's last key, because
                // if it's equal, at the next invocation of
                // _getDigestBlockForItem() we should just wait for the
                // next digest block without re-creating a new stream
                if (this.currentItem.fullKey < blockLastKey) {
                    this.currentDbDigestsStream.pause();
                }
                const digestBlock = {
                    lastKey: blockLastKey,
                    ...JSON.parse(blockValue),
                };
                this.currentDbDigestBlock = digestBlock;
                this._onCurrentDbDigestBlockEvent(digestBlock);
                return undefined;
            })
            .on('end', () => {
                this.currentDbDigestsStream = null;
                if (this.currentItem.bucketName !== fromItem.bucketName) {
                    this._createDigestsDbStream(this.currentItem);
                } else {
                    this.noMoreDigestsForCurrentBucket = true;
                    this._onCurrentDbDigestBlockEvent(null);
                }
            })
            .on('error', err => {
                this.currentDbDigestBlock = null;
                this.currentDbDigestsStream = null;
                // on error, remove the digestsDb handle to avoid
                // further use of the database
                this.digestsDb = null;
                this._onCurrentDbDigestBlockEvent(null);
                this.emit('error', err);
            });
    }

    _onCurrentDbDigestBlockEvent(digestBlock) {
        const callback = this.currentDbDigestBlockWaitCallback;
        if (callback) {
            this.currentDbDigestBlockWaitCallback = null;
            callback(digestBlock);
        }
    }
}

module.exports = DiffStream;
