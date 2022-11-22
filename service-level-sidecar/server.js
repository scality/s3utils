const crypto = require('crypto');
const http = require('http');
const https = require('https');
const util = require('util');

const express = require('express');

const env = require('./env');
const { getServiceReport } = require('./report');
const log = require('./log');

const app = express();

const getServiceReportCb = util.callbackify(getServiceReport);

/**
 *
 * @param {number} statusCode - HTTP status code
 * @param {*} httpMessage - HTTP status message
 * @param {*} httpDesc - An extra msg to include in the JSON response
 * @returns {Error} - instance of Error
 */
function makeError(statusCode, httpMessage, httpDesc) {
    const err = new Error(httpDesc || httpMessage);
    err.status = statusCode;
    err.httpMessage = httpMessage;
    err.httpDesc = httpDesc;
    return err;
}


// Middleware to add request logger
app.use((req, res, next) => {
    // eslint-disable-next-line no-param-reassign
    req.log = log.newRequestLogger();
    req.log.info('received request');
    next();
});

function finishMiddleware(req, res) {
    const info = {
        httpCode: res.statusCode,
        httpMessage: res.statusMessage,
    };
    req.log.end('finished handling request', info);
}

// Hash our api key once for reuse during the request
// We prepend `Bearer ` to avoid having to strip it from the header value runtime for comparison
const actualKeyHash = crypto.createHash('sha512').copy().update(`Bearer ${env.apiKey}`).digest();

// Any handler mounted under `/api/` requires an Authorization header
app.use('/api', (req, res, next) => {
    const key = req.headers.authorization;

    // api key isn't present
    if (req.headers.authorization === undefined) {
        req.log.error('request has no api key');
        next(makeError(400, 'Bad Request', 'Missing authorization header'));
        return;
    }

    const suppliedKeyHash = crypto
        .createHash('sha512')
        .update(req.headers.authorization)
        .digest();

    // Use timing safe equals to avoid timing attacks
    const valid = crypto.timingSafeEqual(suppliedKeyHash, actualKeyHash);

    if (!valid) {
        req.log.error('invalid api key');
        next(makeError(401, 'Access Denied'));
        return;
    }
    next();
});

// Our report generation handler
app.post('/api/report', (req, res, next) => {
    const timestamp = Date.now() * 1000;
    getServiceReportCb(timestamp, req.log, (err, report) => {
        if (err) {
            req.log.error('error generating metrics report', { error: err });
            next(makeError(500, 'Internal Server Error'));
            return;
        }
        res.status(200).send(report);
        // directly call our finish middleware
        finishMiddleware(req, res);
    });
});

// Register a catch-all 404 handler
// Must be defined after all other non-error middleware
app.use((req, res, next) => {
    next(makeError(404, 'Not Found'));
});

// Catch all error handler
app.use((err, req, res, next) => {
    req.log.error('error during request', { error: err });
    // Default errors to `500 Internal Server Error` in case something unexpected slips through
    const data = {
        code: err.status || 500,
        error: err.httpMessage || 'Internal Server Error',
    };

    if (err.httpDesc) {
        data.msg = err.httpDesc;
    }

    res.status(data.code).send(data);
    // call next so we hit our logging handler
    next();
});

// handler used for finish logging on error
app.use(finishMiddleware);

/**
 * Create and start a new HTTP server
 * @param {function} cb - called with running server instance
 * @returns {undefined}
 */
function startServer(cb) {
    const server = env.tls.enabled ? https.createServer(env.tls.certs, app) : http.createServer(app);
    server.listen(env.port, env.host, () => cb(server));
}

module.exports = {
    startServer,
};
