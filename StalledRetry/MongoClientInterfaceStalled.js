const async = require('async');

const {
    MongoClientInterface,
} = require('arsenal').storage.metadata.mongoclient;

class MongoClientInterfaceStalled extends MongoClientInterface {
    constructor(params) {
        super(params.mongodb);
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
            {
                $match: {
                    '_id.id': { $regex: /\0/ },
                    '_id.status': { $eq: 'PENDING' },
                },
            },
        ]);
    }

    queueStalledObjects(expiredBy, cb) {
        const cmpDate = new Date();
        cmpDate.setHours(cmpDate.getHours() - expiredBy);
        const reqHandler = this.requestHandlerFactory(this.logger);

        let stalledCount = 0;
        this.db.listCollections().toArray((err, collections) => {
            if (err) {
                return cb(err);
            }
            return async.eachSeries(collections, (value, next) => {
                if (this._isSpecialCollection(value.name)) {
                    // skip
                    return next();
                }
                const bucketName = value.name;
                const wrapper = this.cursorWrapperFactory(
                    bucketName,
                    cmpDate,
                    this._getStalledObjectsByBucket(bucketName),
                    this.logger,
                );

                return reqHandler.handleRequests(wrapper, (err, results) => {
                    if (err) {
                        this.logger.error(
                            'encountered error while processing requests',
                            { method: 'queueStalledObjects', error: err },
                        );
                        return next(err);
                    }

                    this.logger.debug('completed handling requests', {
                        method: 'queueStalledObjects',
                        bucket: value.name,
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

