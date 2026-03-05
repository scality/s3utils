'use strict';

const http = require('http');
const async = require('async');
const { http: httpArsn } = require('httpagent');
const werelogs = require('werelogs');

const log = new werelogs.Logger('s3utils:httpRequest');

const httpAgent = new httpArsn.Agent({
    keepAlive: true,
});

/**
 * Makes an HTTP request and returns the response with the full body
 * buffered in res.body.
 *
 * @param {string} method - HTTP method (e.g. 'GET', 'DELETE')
 * @param {string} url - full URL to request
 * @param {object} [retryParams] - if provided, failed requests are retried
 *   via async.retry with these params (e.g. { times: 100, interval: 5000 });
 *   both network errors and 5xx responses are treated as retryable failures
 * @returns {Promise<http.IncomingMessage>} response object with body in res.body
 */
function httpRequest(method, url, retryParams) {
    async function attempt() {
        const res = await new Promise((resolve, reject) => {
            const urlObj = new URL(url);
            const req = http.request({
                hostname: urlObj.hostname,
                port: urlObj.port,
                path: `${urlObj.pathname}${urlObj.search}`,
                method,
                agent: httpAgent,
            }, r => {
                const chunks = [];
                r.on('data', chunk => chunks.push(chunk));
                r.once('end', () => {
                    // eslint-disable-next-line no-param-reassign
                    r.body = chunks.join('');
                    log.trace('received HTTP response', { method, url, statusCode: r.statusCode });
                    resolve(r);
                });
                r.once('error', err => reject(new Error(
                    'error reading response from HTTP request '
                        + `to ${url}: ${err.message}`
                )));
            });
            req.once('error', err => reject(new Error(
                `error sending HTTP request to ${url}: ${err.message}`
            )));
            log.trace('sending HTTP request', { method, url });
            req.end();
        });
        if (res.statusCode >= 500) {
            throw new Error(`${method} ${url} returned status ${res.statusCode}`);
        }
        return res;
    }

    if (retryParams) {
        return async.retry(retryParams, attempt);
    }
    return attempt();
}

module.exports = httpRequest;
