const { waterfall, doWhilst, eachLimit, retry } = require('async');
const http = require('http');
const { Producer } = require('node-rdkafka');
const { scheduleJob } = require('node-schedule');

const { errors } = require('arsenal');
const VID_SEP = require('arsenal').versioning.VersioningConstants
      .VersionId.Separator;
const { Logger } = require('werelogs');

const BackbeatClient = require('./BackbeatClient');

const log = new Logger('s3utils::autoRetryFailedCRR-S3C');
const CLOUDSERVER_ENDPOINT = process.env.CLOUDSERVER_ENDPOINT;
const BACKBEAT_API_ENDPOINT = process.env.BACKBEAT_API_ENDPOINT;
const ACCESS_KEY = process.env.ACCESS_KEY;
const SECRET_KEY = process.env.SECRET_KEY;
const SITE_NAME = process.env.SITE_NAME;
const KAFKA_HOSTS = process.env.KAFKA_HOSTS;
const KAFKA_TOPIC = process.env.KAFKA_TOPIC;
const CRON_RULE = process.env.CRON_RULE || '0 */6 * * *';


if (!CLOUDSERVER_ENDPOINT) {
    throw new Error('CLOUDSERVER_ENDPOINT not defined');
}
if (!BACKBEAT_API_ENDPOINT) {
    throw new Error('BACKBEAT_API_ENDPOINT not defined');
}
if (!ACCESS_KEY) {
    throw new Error('ACCESS_KEY not defined');
}
if (!SECRET_KEY) {
    throw new Error('SECRET_KEY not defined');
}
if (!SITE_NAME) {
    throw new Error('missing SITE_NAME environment variable, must be set to' +
                    ' the value of "site" property in the CRR configuration');
}
if (!KAFKA_HOSTS) {
    throw new Error('KAFKA_HOSTS not defined');
}
if (!KAFKA_TOPIC) {
    throw new Error('KAFKA_TOPIC not defined');
}

const PRODUCER_MESSAGE_MAX_BYTES = 5000020;
const PRODUCER_RETRY_DELAY_MS = 5000;
const PRODUCER_MAX_RETRIES = 60;
const PRODUCER_POLL_INTERVAL_MS = 2000;

const bbOptions = {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    endpoint: CLOUDSERVER_ENDPOINT,
    region: 'us-east-1',
    sslEnabled: false,
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
    httpOptions: {
        timeout: 0,
        agent: new http.Agent({ keepAlive: true }),
    },
};

const bb = new BackbeatClient(bbOptions);

const producer = new Producer({
    'metadata.broker.list': KAFKA_HOSTS,
    'message.max.bytes': PRODUCER_MESSAGE_MAX_BYTES,
}, {
});

let requeueInProgress = false;
let stopRequest = false;

function _requeueObject(bucket, key, versionId, counters, cb) {
    /* eslint-disable no-param-reassign */
    if (stopRequest) {
        return process.nextTick(cb);
    }
    return waterfall([
        // get object blob
        next => bb.getMetadata({
            Bucket: bucket,
            Key: key,
            VersionId: versionId,
        }, (err, mdRes) => {
            if (err) {
                log.error('error getting metadata of object', {
                    bucket,
                    key,
                    error: err.message,
                });
                ++counters.errors;
            }
            next(err, mdRes);
        }),
        (mdRes, next) => {
            const objMD = JSON.parse(mdRes.Body);
            if (!objMD.replicationInfo ||
                objMD.replicationInfo.status !== 'FAILED') {
                log.info('skipping object: not FAILED', {
                    bucket,
                    key,
                    versionId,
                    status: objMD.replicationInfo ?
                        objMD.replicationInfo.status : 'NEW',
                });
                ++counters.skipped;
                return next();
            }
            const objSize = objMD['content-length'];
            // reset to PENDING
            objMD.replicationInfo.status = 'PENDING';
            objMD.replicationInfo.backends[0].status = 'PENDING';
            const mdBlob = JSON.stringify(objMD);
            // create an entry as if coming from the raft log
            const entry = JSON.stringify({
                type: 'put',
                bucket,
                key: `${key}${VID_SEP}${objMD.versionId}`,
                value: mdBlob,
            });
            return retry({
                times: PRODUCER_MAX_RETRIES,
                interval: PRODUCER_RETRY_DELAY_MS,
            }, attemptDone => {
                try {
                    producer.produce(
                        KAFKA_TOPIC,
                        null, // partition
                        new Buffer(entry), // value
                        `${bucket}/${key}`, // key (for keyed partitioning)
                        Date.now(), // timestamp
                        null);
                    log.info('requeued object version for replication', {
                        bucket,
                        key,
                        versionId,
                    });
                    ++counters.requeued;
                    counters.requeuedBytes += objSize;
                    return attemptDone();
                } catch (err) {
                    log.error('error producing entry to kafka, retrying', {
                        bucket,
                        key,
                        error: err.message,
                    });
                    return attemptDone(err);
                }
            }, err => {
                if (err) {
                    log.error('give up producing entry to kafka after retries',
                              { bucket, key, error: err.message });
                    ++counters.errors;
                }
                next(err);
            });
        },
    ], err => {
        if (err) {
            log.error('error in _requeueObject waterfall',
                      { error: err.message });
        }
        return cb();
    });
    /* eslint-enable no-param-reassign */
}

function _requeueAll() {
    log.info('starting requeuing task');
    const counters = {
        requeued: 0,
        requeuedBytes: 0,
        skipped: 0,
        errors: 0,
    };
    let marker = undefined;
    requeueInProgress = true;
    doWhilst(
        batchDone => {
            log.info('requeuing task progress', counters);
            let url = `${BACKBEAT_API_ENDPOINT}/_/crr/failed?sitename=${SITE_NAME}`;
            if (marker !== undefined) {
                url += `&marker=${marker}`;
            }
            const failedReq = http.request(url, res => {
                const bodyChunks = [];
                res.on('data', chunk => bodyChunks.push(chunk));
                res.on('error', err => {
                    log.error('error receiving response body for the list of failed CRR', {
                        error: err.message,
                    });
                    return batchDone(err);
                });
                res.on('end', () => {
                    const body = Buffer.concat(bodyChunks).toString();
                    if (res.statusCode !== 200) {
                        log.error('request to retrieve list of failed CRR returned HTTP error status', {
                            errorCode: res.statusCode,
                            error: body,
                        });
                        return batchDone(errors.InternalError);
                    }
                    let result;
                    try {
                        result = JSON.parse(body);
                    } catch (err) {
                        log.error('invalid response: not JSON');
                        return batchDone(errors.InternalError);
                    }
                    marker = result.NextMarker;
                    eachLimit(
                        result.Versions, 10,
                        (version, objDone) => _requeueObject(
                            version.Bucket, version.Key, version.VersionId, counters, objDone),
                        () => batchDone(null, result.IsTruncated));
                });
            });
            failedReq.on('error', err => {
                log.error('error sending request to retrieve list of failed CRR', {
                    error: err.message,
                });
                return batchDone(err);
            });
            failedReq.end();
        },
        isTruncated => isTruncated && !stopRequest,
        () => {
            requeueInProgress = false;
            if (stopRequest) {
                log.info('aborted requeuing task', counters);
                producer.disconnect();
            } else {
                log.info('completed requeuing task', counters);
            }
        });
}

let cronJob = null;

producer.connect();
producer.on('ready', () => {
    producer.setPollInterval(PRODUCER_POLL_INTERVAL_MS);
    log.info('process is ready', { cronRule: CRON_RULE });
    cronJob = scheduleJob(CRON_RULE, _requeueAll);
});
producer.on('event.error', error => {
    // This is a bit hacky: the "broker transport failure"
    // error occurs when the kafka broker reaps the idle
    // connections every few minutes, and librdkafka handles
    // reconnection automatically anyway, so we ignore those
    // harmless errors (moreover with the current
    // implementation there's no way to access the original
    // error code, so we match the message instead).
    if (!['broker transport failure',
          'all broker connections are down']
        .includes(error.message)) {
        log.error('error with producer', {
            error: error.message,
        });
    }
});

function stop(signal) {
    log.info('received signal, exiting', { signal });
    if (cronJob) {
        cronJob.cancel();
    }
    if (requeueInProgress) {
        stopRequest = true;
    } else {
        producer.disconnect();
    }
}

process.on('SIGTERM', () => stop('SIGTERM'));
process.on('SIGINT', () => stop('SIGINT'));
