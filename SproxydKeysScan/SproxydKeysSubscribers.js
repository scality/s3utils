const { MultiMap } = require('./DuplicateKeysWindow');
const { repairObject } = require('../repairDuplicateVersionsSuite');
const getBucketdURL = require('../VerifyBucketSproxydKeys/getBucketdURL');
const { Logger } = require('werelogs');
const log = new Logger('s3utils:DuplicateKeysIngestion');

const subscribers = new MultiMap();

class DuplicateSproxydKeyFoundHandler {
    constructor() {
        this._repairObject = repairObject;
        this._getBucketdURL = getBucketdURL;
        this.bucketdHost = process.env.BUCKETD_HOSTPORT;
    }

    _cmp(a, b) {
        // here largest string is first (which is the older version)
        // older version is chosen to repair
        if (a > b) { return -1; }
        if (b > a) { return 1; }
        return 0;
    }

    handle(params) {
        const [objectUrl, objectUrl2] =
            [{ Bucket: params.bucket, Key: params.objectKey }, { Bucket: params.bucket, Key: params.existingObjectKey }]
                .map(_params => this._getBucketdURL(this.bucketdHost, _params))
                .sort(this._cmp);

        const objInfo = {
            objectUrl,
            objectUrl2,
        };

        return this._repairObject(objInfo, err => {
            if (err) {
                log.error('an error occurred repairing object', {
                    objectUrl: objInfo.objectUrl,
                    error: { message: err.message },
                });
            }
        });
    }
}

subscribers.set('duplicateSproxyKeyFound', new DuplicateSproxydKeyFoundHandler());
module.exports = { subscribers, DuplicateSproxydKeyFoundHandler };
