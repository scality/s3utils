const async = require('async');
const assert = require('assert');
const Prando = require('prando');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class Injector {

    constructor(backend) {
        this.backend = backend;
        this.nOps = 0;
        this.nPuts = 0;
        this.nDeletes = 0;
        this.rnd = new Prando(0);
        this.logger = new werelogs.Logger('Injector');
    }

    printStats() {
        // eslint-disable-next-line
        console.error('nOps', this.nOps);
        // eslint-disable-next-line
        console.error('nPuts', this.nPuts);
        // eslint-disable-next-line
        console.error('nDeletes', this.nDeletes);
    }

    genKey(len) {
        let result = '';
        const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        const charactersLen = characters.length;
        for (let i = 0; i < len; i++) {
            result += characters.charAt(
                Math.floor(
                    this.rnd.next() *
                        charactersLen));
        }
        return result;
    }

    genBase16(len) {
        let result = '';
        const characters = 'abcdef0123456789';
        const charactersLen = characters.length;
        for (let i = 0; i < len; i++) {
            result += characters.charAt(
                Math.floor(
                    this.rnd.next() *
                        charactersLen));
        }
        return result;
    }

    _zeroPad(s, n, width) {
        const _n = `${n}`;
        return s + (_n.length >= width ? _n :
            new Array(width - _n.length + 1).join('0') + _n);
    }

    inject(bucketName, numKeys, maxSeq, op, randomSeq, prefix, suffix, keys, values, cb) {
        const injectParams = {};
        injectParams.numKeys = numKeys;
        injectParams.maxSeq = maxSeq;
        injectParams.op = op;
        injectParams.randomSeq = randomSeq;
        injectParams.prefix = prefix;
        injectParams.suffix = suffix;
        return this.injectExt(
            bucketName,
            injectParams,
            null,
            keys,
            values,
            cb);
    }

    injectExt(bucketName, params, inputKeys, outputKeys, outputValues, cb) {
        async.timesLimit(
            params.numKeys,
            10,
            (n, next) => {
                let key;
                if (inputKeys) {
                    const idx = Math.floor(this.rnd.next() * inputKeys.length);
                    key = inputKeys[idx];
                    inputKeys.splice(idx, 1);
                } else {
                    if (params.randomKey) {
                        const len = Math.floor(this.rnd.next() * 255) + 1;
                        key = this.genKey(len);
                    } else {
                        let x;
                        if (params.randomSeq) {
                            x = Math.floor(this.rnd.next() * params.maxSeq);
                        } else {
                            x = n;
                        }
                        key = this._zeroPad(params.prefix, x, 10) +
                            params.suffix;
                    }
                }
                if (outputKeys) {
                    outputKeys.push(key);
                }
                // eslint-disable-next-line
                const value = {
                    versionId: this.genBase16(32),
                    'content-length': Math.floor(this.rnd.next() * 64000),
                    'content-md5': this.genBase16(32),
                };
                if (outputValues) {
                    outputValues.push(value);
                }
                if (params.op === 1) {
                    this.backend.putObjectMD(
                        bucketName,
                        key,
                        value,
                        {},
                        this.logger,
                        next);
                    return undefined;
                } else if (params.op === 0) {
                    this.backend.deleteObjectMD(
                        bucketName,
                        key,
                        {},
                        this.logger,
                        err => {
                            if (err) {
                                if (err.code !== 404) {
                                    return next(err);
                                }
                            }
                            return next();
                        });
                    return undefined;
                }
                return next(new Error('unknow op'));
            },
            err => {
                if (err) {
                    // eslint-disable-next-line
                    console.error('inject error', err);
                    process.exit(1);
                }
                if (cb) {
                    return cb();
                }
                return undefined;
            });
    }
}

module.exports = Injector;

describe('Injector', () => {
    it('zeropad', () => {
        const injector = new Injector();
        assert(injector._zeroPad('foo', 42, 10) === 'foo0000000042');
    });
});
