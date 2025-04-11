const arsenal = require('arsenal');
const bucketclient = require('bucketclient');

const { BucketClientInterface } = arsenal.storage.metadata.bucketclient;
const { usersBucket, splitter: mdKeySplitter } = arsenal.constants;

const rootLogger = require('./log');
const env = require('./env');
const utils = require('./utils');

const PAGE_SIZE = 1000;

const params = {
    bucketdBootstrap: [env.bucketd],
    https: env.bucketdTls ? env.tls.certs : undefined,
};

const metadata = new BucketClientInterface(params, bucketclient, rootLogger);

const listObjects = utils.retryable((bucket, params, log, cb) => metadata.listObject(bucket, params, log, (err, res) => {
    if (err && err.NoSuchBucket) {
        return cb(null, []);
    }
    return cb(err, res);
}));

/**
 * List all s3 buckets implemented as a async generator
 *
 * @param {object} log - werelogs logger instance
 * @returns {object} - async generator
 */
async function* listBuckets(log) {
    const listingParams = { maxKeys: PAGE_SIZE, listingType: 'Basic' };
    let gt;

    while (true) {
        let res;
        try {   
            res = await listObjects(usersBucket, { ...listingParams, gt }, log);
        } catch (error) {
            if (error.NoSuchBucket) {
                log.info('no buckets found');
                return;
            }
            log.error('Error during listing', { error });
            throw error;
        }

        log.debug('got list of buckets from bucketd', { length: res.length });

        yield res.map(data => {
            const { key, value } = data;
            const [account, name] = key.split(mdKeySplitter);
            return {
                account,
                name,
                value: JSON.parse(value),
            };
        });

        if (res.length !== PAGE_SIZE) {
            break;
        }

        gt = res[res.length - 1].key;
    }
}

async function getRaftSessionIds(log) {
    return new Promise((resolve, reject) => {
        metadata.client.getAllRafts(log.getSerializedUids(), (error, res) => {
            if (error) {
                log.error('error getting raft session ids', { error });
                return reject(error);
            }

            const data = JSON.parse(res);

            return resolve(data.map(raft => `${raft.id}`));
        }, log);
    });
}

module.exports = {
    listBuckets,
    getRaftSessionIds,
};
