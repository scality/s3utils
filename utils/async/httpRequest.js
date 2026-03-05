'use strict';

const http = require('http');
const { http: httpArsn } = require('httpagent');
const werelogs = require('werelogs');

const log = new werelogs.Logger('s3utils:httpRequest');

const httpAgent = new httpArsn.Agent({
    keepAlive: true,
});

function httpRequest(method, url) {
    return new Promise((resolve, reject) => {
        const urlObj = new URL(url);
        const req = http.request({
            hostname: urlObj.hostname,
            port: urlObj.port,
            path: `${urlObj.pathname}${urlObj.search}`,
            method,
            agent: httpAgent,
        }, res => {
            const chunks = [];
            res.on('data', chunk => chunks.push(chunk));
            res.once('end', () => {
                // eslint-disable-next-line no-param-reassign
                res.body = chunks.join('');
                log.trace('received HTTP response', { method, url, statusCode: res.statusCode });
                resolve(res);
            });
            res.once('error', err => reject(new Error(
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
}

module.exports = httpRequest;
