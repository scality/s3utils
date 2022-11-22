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

const listObjects = utils.retryable(metadata.listObject.bind(metadata));

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
            // eslint-disable-next-line no-await-in-loop
            res = await listObjects(usersBucket, { ...listingParams, gt }, log);
        } catch (error) {
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

module.exports = {
    listBuckets,
};
