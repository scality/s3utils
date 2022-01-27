// Ring backend that persists on Sproxyd and offsets on ZK
// const assert = require('assert');
const async = require('async');
const { pipeline } = require('stream');
const MemoryStream = require('memorystream');
const zlib = require('zlib');
const zookeeper = require('node-zookeeper-client');
const Sproxy = require('sproxydclient');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class PersistRingInterface {

    constructor() {
        const zkConnectionString = 'localhost:2181';
        const zkPath = '/persist-ring-interface';
        const spBootstrap = ['localhost:8181'];
        const spPath = '/proxy/DC1/'; // do not forget "/" at the end !!!
        this.reqUid = 'persist-ring-interface-req-uid';
        this.logger = new werelogs.Logger('PersistRingInterface');
        this.zkClient = zookeeper.createClient(zkConnectionString);
        this.zkPath = zkPath;
        this.zkClient.connect();
        this.zkClient.on('error', err => {
            this.logger.error('error connecting', { err });
        });
        this.zkClient.once('connected', () => {
            this.logger.info('connected');
        });
        this.spClient = new Sproxy({
            bootstrap: spBootstrap,
            path: spPath,
        });
    }

    getZKPath(bucketName) {
        return `${this.zkPath}/${bucketName}`;
    }

    // cb(err, offset)
    load(bucketName, persistData, cb) {
        this.logger.info(`loading ${bucketName}`);
        async.waterfall([
            next => {
                this.zkClient.getData(
                    this.getZKPath(bucketName),
                    (err, data) => {
                        if (err) {
                            if (err.name === 'NO_NODE') {
                                this.logger.info(`${bucketName} non-existent`);
                            } else {
                                this.logger.error(`getData ${bucketName} error`, { err });
                            }
                            return next(err);
                        }
                        return next(null, data);
                    });
            },
            (data, next) => {
                const _data = JSON.parse(data.toString());
                this.spClient.get(
                    _data.key,
                    undefined,
                    this.reqUid,
                    (err, stream) => {
                        if (err) {
                            this.logger.error(`sproxyd ${bucketName} error`, { err });
                            return next(err);
                        }
                        return next(null, _data, stream);
                    });
            },
            (_data, stream, next) => {
                const ostream = new MemoryStream();
                pipeline(
                    stream,
                    zlib.createGunzip(),
                    ostream,
                    err => {
                        if (err) {
                            this.logger.error(`pipeline ${bucketName} error`, { err });
                            return next(err);
                        }
                        return next(null, _data, ostream);
                    });
            },
            (_data, stream, next) => {
                persistData.loadState(stream, err => {
                    if (err) {
                        this.logger.error(`load ${bucketName} error`, { err });
                        return next(err);
                    }
                    this.logger.info(`${bucketName} loaded: offset ${_data.offset}`);
                    return next(null, _data);
                });
            }], (err, _data) => {
            if (err) {
                if (err.name === 'NO_NODE') {
                    return cb(null, undefined);
                }
                this.logger.error(`load ${bucketName} error`, { err });
                return cb(err);
            }
            return cb(null, _data.offset);
        });
    }

    // cb(err)
    save(bucketName, persistData, offset, cb) {
        this.logger.info(`saving ${bucketName} offset ${offset}`);
        async.waterfall([
            next => {
                const stream = new MemoryStream();
                persistData.saveState(
                    stream, err => {
                        if (err) {
                            this.logger.error(`save ${bucketName} error`, { err });
                            return next(err);
                        }
                        return next(null, stream);
                    });
            },
            (stream, next) => {
                const ostream = new MemoryStream();
                pipeline(
                    stream,
                    zlib.createGzip(),
                    ostream,
                    err => {
                        if (err) {
                            this.logger.error(`pipeline ${bucketName} error`, { err });
                            return next(err);
                        }
                        return next(null, ostream);
                    });
            },
            (stream, next) => {
                const parameters = {
                    bucketName,
                    namespace: 'persist-ring-interface',
                    owner: 'persist-ring-interface',
                };
                const size = stream._readableState.length;
                this.spClient.put(
                    stream,
                    size,
                    parameters,
                    this.reqUid,
                    (err, key) => {
                        if (err) {
                            this.logger.error(`sproxyd put ${bucketName} error`, { err });
                            return next(err);
                        }
                        const newData = {};
                        newData.offset = offset;
                        newData.key = key;
                        return next(null, newData);
                    });
            },
            (newData, next) => {
                this.zkClient.exists(
                    this.getZKPath(bucketName),
                    (err, stat) => {
                        if (err) {
                            this.logger.error(`exists ${bucketName} error`, { err });
                            return next(err);
                        }
                        let doesExist = false;
                        if (stat) {
                            doesExist = true;
                        }
                        return next(null, newData, doesExist);
                    });
            },
            (newData, doesExist, next) => {
                if (doesExist) {
                    this.zkClient.getData(
                        this.getZKPath(bucketName),
                        (err, _oldData) => {
                            if (err) {
                                this.logger.error(`getData ${bucketName} error`, { err });
                                return next(err);
                            }
                            const oldData = JSON.parse(_oldData);
                            return next(null, newData, oldData);
                        });
                } else {
                    this.zkClient.mkdirp(
                        this.getZKPath(bucketName),
                        null,
                        err => {
                            if (err) {
                                this.logger.error(`mkdirp ${bucketName} error`, { err });
                                return next(err);
                            }
                            return next(null, newData, null);
                        });
                }
            },
            (newData, oldData, next) => {
                const _newData = JSON.stringify(newData);
                this.zkClient.setData(
                    this.getZKPath(bucketName),
                    new Buffer(_newData),
                    err => {
                        if (err) {
                            this.logger.error(`setData ${bucketName} error`, { err });
                            return cb(err);
                        }
                        this.logger.info(`${bucketName} saved: new key ${newData.key} offset ${offset}`);
                        // console.log('oldData', oldData);
                        if (oldData) {
                            this.spClient.delete(
                                oldData.key,
                                this.reqUid,
                                err => {
                                    if (err) {
                                        this.logger.error(
                                            `sproxyd del ${bucketName} old key ${oldData.key} error`,
                                            { err });
                                        return next(err);
                                    }
                                    return next();
                                });
                        } else {
                            return next();
                        }
                        return undefined;
                    });
            }], err => {
            if (err) {
                this.logger.error(`save ${bucketName} error`, { err });
                return cb(err);
            }
            return cb();
        });
    }
}

module.exports = PersistRingInterface;
