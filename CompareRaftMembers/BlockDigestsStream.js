const crypto = require('crypto');
const stream = require('stream');

const DEFAULT_BLOCK_SIZE = 1000;
const DEFAULT_HASH_ALGORITHM = 'md5';

/**
 * BlockDigestsStream is a stream.Transform stream that groups series
 * of consecutive input items into blocks, and outputs each block's
 * attributes as a stream of objects.
 *
 * The stream converts a potentially large list of items into a much
 * smaller list of block hashes. The output can be used in place of
 * the input for quick comparison purpose, by only comparing the block
 * hashes of two listings, where comparing their individual entries
 * could be costly in time and space. When differences are spotted
 * between block entries, one can then list only the differing block
 * sub-range to detect which individual entries differ.
 *
 * Objects written to the stream must contain attributes:
 * - key {string}
 * - value {string}
 *
 * Object read from the stream will then contain the following attributes:
 * - size {number} - number of keys included in the block
 * - lastKey {string} - last key included in the block
 * - digest {string} - block's digest, in hex format
 *
 * All input items are guaranteed to be included in exactly one output
 * block, in the order in which they are streamed.
 *
 * The algorithm uses fixed-sized blocks, each containing a number of
 * keys equal to the defined block size, or smaller if there is an
 * explicit flush or the end of the input stream is reached.
 *
 * Input keys are not required to be in a particular order for the
 * stream to work, although most use cases would need input stream
 * items to be sorted by key in order to do meaningful comparisons.
 *
 * @class BlockDigestsStream
 */
class BlockDigestsStream extends stream.Transform {
    /**
     * @constructor
     * @param {object} params - constructor params
     * @param {number} [params.blockSize=1024] - number of
     * keys in each block, or 0 to disable automatic boundaries
     * @param {string} [params.hashAlgorithm='md5'] - hash algorithm
     * used to compute block boundaries and digests
     */
    constructor(params) {
        super({ objectMode: true });

        const {
            blockSize: userBlockSize,
            hashAlgorithm: userHashAlgorithm,
        } = params || {};
        const blockSize = typeof userBlockSize === 'number'
            ? userBlockSize : DEFAULT_BLOCK_SIZE;
        if (blockSize > 0) {
            this._blockSize = blockSize;
        } else {
            // disable automatic blocking by making the block size very large
            this._blockSize = Number.MAX_SAFE_INTEGER;
        }
        this._hashAlgorithm = userHashAlgorithm || DEFAULT_HASH_ALGORITHM;
        this._initNewBlock();
    }

    /**
     * Force-flush the current block, if it contains at least one key
     *
     * It then outputs a new block, as if the last key received was a
     * boundary, and resets the state of the next block.
     *
     * @return {undefined}
     */
    flush() {
        if (this._currentBlock.lastKey !== null) {
            this._doFlush();
        }
    }

    _doFlush() {
        const digest = this._currentBlock.digest.digest('hex');
        this.push({
            size: this._currentBlock.size,
            digest,
            lastKey: this._currentBlock.lastKey,
        });
        this._initNewBlock();
    }

    _initNewBlock() {
        this._currentBlock = {
            digest: crypto.createHash(this._hashAlgorithm),
            lastKey: null,
            size: 0,
        };
    }

    _transform(chunk, encoding, callback) {
        const keyDigest = crypto.createHash(this._hashAlgorithm).update(chunk.key).digest();
        this._currentBlock.digest.update(keyDigest);
        this._currentBlock.digest.update(chunk.value);
        this._currentBlock.size += 1;
        this._currentBlock.lastKey = chunk.key;
        if (this._currentBlock.size >= this._blockSize) {
            this._doFlush();
        }
        callback();
    }

    _flush(callback) {
        this.flush();
        callback();
    }
}

module.exports = BlockDigestsStream;
