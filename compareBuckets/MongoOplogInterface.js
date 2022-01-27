/*
  fake backend that uses MongoDB for oplog (manual debug)

  - start mongodb (>=3.6)
  $ git clone github.com/scality/Zenko-MD
  $ cd Zenko-MD
  $ MDP_DRIVER=mongodb npm start
*/
const MongoClient = require('mongodb').MongoClient;
const bson = require('bson');
const async = require('async');
const Flusher = require('./Flusher');
const { jsutil } = require('arsenal');
const { isMasterKey } = require('arsenal/lib/versioning/Version');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

const databaseName = 'metadata';

class MongoOplogInterface {

    constructor() {
        this.logger = new werelogs.Logger('MongoOplogInterface');
    }

    start(bucketName, persist, persistData, stopAt, cb) {
        let db;
        let collection;
        async.waterfall([
            next => {
                MongoClient.connect(
                    'mongodb://localhost:27017',
                    (err, client) => {
                        if (err) {
                            this.logger.error('error connecting to mongodb', { err, bucketName });
                            return next(err);
                        }
                        db = client.db(databaseName, {
                            ignoreUndefined: true,
                        });
                        collection = db.collection(bucketName);
                        return next();
                    });
            },
            next => {
                let resumeToken = undefined;
                // get stored offset if we have it
                // load persistData if we have it
                persist.load(bucketName, persistData, (err, offset) => {
                    if (err) {
                        return next(err);
                    }
                    if (offset && offset._data) {
                        resumeToken = {};
                        resumeToken._data = new bson.Binary(new Buffer(offset._data, 'base64'));
                    }
                    return next(null, resumeToken, persistData);
                });
            },
            (resumeToken, persistData, next) => {
                if (resumeToken !== undefined) {
                    this.logger.info(
                        `skipping resumeToken acquisition (resumeToken=${resumeToken})`,
                        { bucketName });
                    return next(null, resumeToken, persistData, true);
                }
                this.logger.info('resumeToken acquisition',
                                 { bucketName });
                const changeStream = collection.watch();
                // big hack to extract resumeToken
                changeStream.once('change', () => next(null, changeStream.resumeToken, persistData, false));
                return undefined;
            },
            (resumeToken, persistData, skipListing, next) => {
                if (skipListing) {
                    this.logger.info(`skipping listing resumeToken=${resumeToken}`,
                                     { bucketName });
                    return next(null, resumeToken, persistData);
                }
                this.logger.info(`listing resumeToken=${resumeToken}`,
                                 { bucketName });
                persistData.initState(
                    err => {
                        if (err) {
                            // eslint-disable-next-line
                            console.error(err);
                            process.exit(1);
                        }
                        persist.save(
                            bucketName, persistData, resumeToken, err => {
                                if (err) {
                                    return next(err);
                                }
                                return next(null, resumeToken, persistData);
                            });
                        return undefined;
                    });
                return undefined;
            },
            (resumeToken, persistData, next) => {
                this.logger.info(`reading oplog resumeToken=${resumeToken}`,
                                 { bucketName });
                const nextOnce = jsutil.once(next);
                const flusher = new Flusher(bucketName, persist, persistData, null);
                flusher.events.on('stop', () => nextOnce());
                flusher.startFlusher();
                // console.log('resumeToken', resumeToken);
                const changeStream = collection.watch({ resumeAfter: resumeToken });
                changeStream.on(
                    'change', item => {
                        if (item.ns.db === databaseName) {
                            let opType;
                            const _item = {};
                            _item.key = item.documentKey._id;
                            if (item.operationType === 'insert' ||
                                item.operationType === 'replace') {
                                _item.value = {};
                                _item.value.versionId = item.fullDocument.value.versionId;
                                _item.value.size = item.fullDocument.value['content-length'];
                                _item.value.md5 = item.fullDocument.value['content-md5'];
                            }
                            if (item.operationType === 'insert') {
                                opType = 'put';
                            } else if (item.operationType === 'replace') {
                                opType = 'put';
                            } else if (item.operationType === 'delete') {
                                opType = 'del';
                                if (!isMasterKey(_item.key)) {
                                    // ignore for now
                                    return;
                                }
                            } else {
                                return;
                            }
                            flusher.addEvent(
                                _item,
                                changeStream.resumeToken,
                                opType);
                        }
                    });
            }], err => {
            if (err) {
                return cb(err);
            }
            this.logger.info('returning',
                             { bucketName });
            return cb();
        });
    }
}

module.exports = MongoOplogInterface;
