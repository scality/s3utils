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
        // TODO: refactor the object structure, property '_id' may not be required
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
            {
                $match: {
                    'value.replicationInfo.status': { $eq: 'PENDING' },
                },
            },
            { $project: reducedFields },
        ]).stream();
    }

    async queueStalledObjects(expiredBy, cb) {
        const cmpDate = new Date();
        cmpDate.setHours(cmpDate.getHours() - expiredBy);
        const reqHandler = this.requestHandlerFactory(this.logger);

        let stalledCount = 0;
        try {
            const collections = await this.db.listCollections().toArray();
            let i = 0;
            return async.eachSeries(collections, (value, next) => {
                i++;
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
        } catch (err) {
            return cb(err);
        }
    }
}

module.exports = {
    MongoClientInterfaceStalled,
};

