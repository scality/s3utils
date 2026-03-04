/* eslint-disable no-console */
const http = require('http');
const { http: httpArsn } = require('httpagent');
const async = require('async');

const { Logger } = require('werelogs');

const DEFAULT_LISTING_LIMIT = 1000;

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
    BUCKETS, RAFT_SESSIONS,
} = process.env;

const VERBOSE = process.env.VERBOSE === '1';

const LISTING_LIMIT = (
    process.env.LISTING_LIMIT
        && Number.parseInt(process.env.LISTING_LIMIT, 10))
      || DEFAULT_LISTING_LIMIT;

const USAGE = `
cleanupMpuOrphans.js

This script cleans up orphaned multipart upload (MPU) data from S3 buckets.

Usage:
    node cleanupMpuOrphans.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint
    One of:
        BUCKETS: comma-separated list of buckets to scan
    or:
        RAFT_SESSIONS: comma-separated list of raft sessions to scan

Optional environment variables:
    VERBOSE: set to 1 for more verbose output
    LISTING_LIMIT: number of keys to list per listing request (default ${DEFAULT_LISTING_LIMIT})
`;

if (!BUCKETS && !RAFT_SESSIONS) {
    console.error('ERROR: either BUCKETS or RAFT_SESSIONS environment '
                  + 'variable must be defined');
    console.error(USAGE);
    process.exit(1);
}
if (BUCKETS && RAFT_SESSIONS) {
    console.error('ERROR: only one of BUCKETS or RAFT_SESSIONS environment '
                  + 'variables can be defined');
    console.error(USAGE);
    process.exit(1);
}
if (!BUCKETD_HOSTPORT) {
    console.error('ERROR: BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!SPROXYD_HOSTPORT) {
    console.error('ERROR: SPROXYD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}

const log = new Logger('s3utils:cleanupMpuOrphans');

const httpAgent = new httpArsn.Agent({
    keepAlive: true,
});

let remainingBuckets = (BUCKETS && BUCKETS.split(',')) || [];

function httpRequest(method, url, cb) {
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
            return cb(null, res);
        });
        res.once('error', err => cb(new Error(
            'error reading response from HTTP request '
                + `to ${url}: ${err.message}`
        )));
        return undefined;
    });
    req.once('error', err => cb(new Error(
        `error sending HTTP request to ${url}: ${err.message}`
    )));
    req.end();
}

function raftSessionsToBuckets(cb) {
    if (!RAFT_SESSIONS) {
        return cb();
    }
    const rsList = RAFT_SESSIONS.split(',');
    return async.each(rsList, (rs, done) => {
        const url = `http://${BUCKETD_HOSTPORT}/_/raft_sessions/${rs}/bucket`;
        httpRequest('GET', url, (err, res) => {
            if (err) {
                return cb(err);
            }
            if (res.statusCode !== 200) {
                return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
            }
            const resp = JSON.parse(res.body);
            remainingBuckets = remainingBuckets.concat(resp.filter(
                bucket => !bucket.startsWith('mpuShadowBucket')
                    && bucket !== 'users..bucket'
            ));
            return done();
        });
    }, cb);
}

function processBucket(bucket, cb) {
    if (VERBOSE) {
        log.info('processing bucket', { bucket, listingLimit: LISTING_LIMIT });
    }
    // stub: MPU orphan cleanup logic to be implemented
    return process.nextTick(cb);
}

function main() {
    async.series([
        done => raftSessionsToBuckets(done),
        done => async.eachSeries(remainingBuckets, processBucket, done),
    ], err => {
        if (err) {
            log.error('an error occurred during cleanup', {
                error: { message: err.message },
            });
            process.exit(1);
        } else {
            log.info('completed MPU orphan cleanup');
            process.exit(0);
        }
    });
}

main();

function stop() {
    log.info('stopping execution');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGTERM', stop);
process.on('SIGQUIT', stop);

module.exports = { httpRequest };
