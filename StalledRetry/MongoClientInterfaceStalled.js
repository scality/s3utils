const async = require('async');

const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;

const USERSBUCKET = '__usersbucket';
const METASTORE = '__metastore';
const INFOSTORE = '__infostore';
const PENSIEVE = 'PENSIEVE';
const MPU_BUCKET_PREFIX = 'mpuShadowBucket';

class MongoClientInterfaceStalled extends MongoClientInterface {
    constructor(params) {
        super(params);
        this.cursorWrapperFactory = params.cursorWrapperFactory;
        this.requestHandlerFactory = params.requestHandlerFactory;
    }

    _getStalledObjectsByBucket(bucketName) {
        const c = this.getCollection(bucketName);
        const reducedFields = {
            '_id': {
                id: '$_id',
                storageClasses: '$value.replicationInfo.storageClass',
                status: '$value.replicationInfo.status',
                key: '$value.key',
                versionId: '$value.versionId',
            },
            'value.last-modified': 1,
        };
        return c.aggregate([
            { $project: reducedFields },
            { $match: {
                '_id.id': { $regex: /\0/ },
                '_id.status': { $eq: 'PENDING' },
            } },
        ]);
    }

    queueStalledObjects(cb) {
        const cmpDate = new Date();
        cmpDate.setHours(cmpDate.getHours() - 1);
        const reqHandler = this.requestHandlerFactory(this.logger);

        let stalledCount = 0;
        this.db.listCollections().toArray((err, collections) => {
            if (err) {
                return cb(err);
            }
            return async.eachLimit(collections, 1, (value, next) => {
                const skipBucket = value.name === METASTORE ||
                    value.name === INFOSTORE ||
                    value.name === USERSBUCKET ||
                    value.name === PENSIEVE ||
                    value.name.startsWith(MPU_BUCKET_PREFIX);
                if (skipBucket) {
                    // skip
                    return next();
                }
                const bucketName = value.name;
                const wrapper = this.cursorWrapperFactory(
                    bucketName,
                    cmpDate,
                    this._getStalledObjectsByBucket(bucketName),
                    this.logger
                );

                return reqHandler.handleRequests(wrapper, (err, results) => {
                    if (err) {
                        this.logger.error(
                            'encounted error while processing requests',
                            { method: 'queueStalledObjects', error: err }
                        );
                        return next(err);
                    }

                    this.logger.debug('completed handling requests', {
                        method: 'queueStalledObjects',
                        info: results,
                    });

                    stalledCount += results.stalled;
                    return next();
                });
            }, err => cb(err, stalledCount));
        });
    }
}

module.exports = {
    MongoClientInterfaceStalled,
};

