const async = require('async');
const util = require('util');
const env = require('./env');

const ebConfig = {
    times: env.retryLimit,
    interval: retryCount => 50 * (2 ** retryCount),
};

/**
 * Wraps a function using async.retryable and util.promisify to return
 * a promise that automatically retries the passed function with exponential backoff.
 *
 * To work around async.retryable in 2.x being able to take a async function as an argument,
 * but requiring callback-style calling of the returned function.
 *
 * @param {function} func - function to retry
 * @returns {Promise} -
 */
function retryable(func) {
    const inner = async.retryable(ebConfig, func);
    return util.promisify(inner);
}

module.exports = {
    retryable,
};
