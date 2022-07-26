/* eslint-disable no-console */

const async = require('async');
const fs = require('fs');
const path = require('path');
const stream = require('stream');

const { Logger } = require('werelogs');
const Level = require('level');

const DBListStream = require('./DBListStream');
const DiffStream = require('./DiffStream');
const DiffStreamOplogFilter = require('./DiffStreamOplogFilter');

const DEFAULT_PARALLEL_SCANS = 4;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;

const {
    BUCKETD_HOSTPORT,
    DATABASES,
    DIFF_OUTPUT_FILE,
    LISTING_DIGESTS_INPUT_DIR,
} = process.env;

const LOG_PROGRESS_INTERVAL = (
    process.env.LOG_PROGRESS_INTERVAL
        && Number.parseInt(process.env.LOG_PROGRESS_INTERVAL, 10))
      || DEFAULT_LOG_PROGRESS_INTERVAL;

const PARALLEL_SCANS = (
    process.env.PARALLEL_SCANS
        && Number.parseInt(process.env.PARALLEL_SCANS, 10))
      || DEFAULT_PARALLEL_SCANS;

const EXCLUDE_FROM_CSEQS = process.env.EXCLUDE_FROM_CSEQS
      && JSON.parse(process.env.EXCLUDE_FROM_CSEQS);

const USAGE = `
followerDiff.js

This tool compares Metadata leveldb databases on the repd follower on
which it is run against the leader's view, and outputs the differences
to the file path given as the DIFF_OUTPUT_FILE environment
variable.

In this file, it outputs each key that differs as line-separated JSON
entries, where each entry can be one of:

- [{ key, value }, null]: this key is present on this follower but not
  on the leader

- [null, { key, value }]: this key is not present on this follower but
  is present on the leader

- [{ key, value: "{value1}" }, { key, value: "{value2}" }]: this key
  has a different value between this follower and the leader: "value1"
  is the value seen on the follower and "value2" the value seen on the
  leader.

It is possible and recommended to speed-up the comparison by providing
a pre-computed digests database via the LISTING_DIGESTS_INPUT_DIR
environment variable, so that ranges of keys that match the digests
database do not have to be checked by querying the leader. The
pre-computed digests database can be generated via a run of
"verifyBucketSproxydKeys" script, providing it the
LISTING_DIGESTS_OUTPUT_DIR environment variable.

Usage:
    node followerDiff.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    DATABASES: space-separated list of databases to scan
    DIFF_OUTPUT_FILE: file path where diff output will be stored

Optional environment variables:
    LISTING_DIGESTS_INPUT_DIR: read listing digests from the specified LevelDB database
    PARALLEL_SCANS: number of databases to scan in parallel (default ${DEFAULT_PARALLEL_SCANS})
    EXCLUDE_FROM_CSEQS: mapping of raft sessions to filter on, where
        keys are raft session IDs and values are the cseq value for that
        raft session. Filtering will be based on all oplog records more
        recent than the given "cseq" for the raft session. Input diff
        entries not belonging to one of the declared raft sessions are
        discarded from the output. The value must be in the following JSON
        format:
            {"rsId":cseq[,"rsId":cseq...]}
        Example:
            {"1":1234,"4":4567,"6":6789}
        This configuration would cause diff entries which bucket/key
        appear in either of the following to be discarded from the output:
        - oplog of raft session 1 after cseq=1234
        - or oplog of raft session 4 after cseq=4567
        - or oplog of raft session 6 after cseq=6789
        - or any other raft session's oplog at any cseq
`;

if (!BUCKETD_HOSTPORT) {
    console.error('ERROR: BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!DATABASES) {
    console.error('ERROR: DATABASES not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!DIFF_OUTPUT_FILE) {
    console.error('ERROR: DIFF_OUTPUT_FILE not defined');
    console.error(USAGE);
    process.exit(1);
}

const [BUCKETD_HOST, BUCKETD_PORT] = BUCKETD_HOSTPORT.split(':');
if (!BUCKETD_PORT) {
    console.error('ERROR: BUCKETD_HOSTPORT must be of form "ip:port"');
    console.error(USAGE);
    process.exit(1);
}
const DATABASES_LIST = DATABASES.split(' ').filter(dbPath => {
    const dbName = path.basename(dbPath);
    return !['sdb', 'stdb', 'dbAttributes'].includes(dbName);
});
const DIFF_OUTPUT_STREAM = fs.createWriteStream(DIFF_OUTPUT_FILE, { flags: 'wx' });

const log = new Logger('s3utils:CompareRaftMembers:followerDiff');

const status = {
    keysScanned: 0,
    onlyOnLeader: 0,
    onlyOnFollower: 0,
    differingValue: 0,
};

function logProgress(message) {
    log.info(message, status);
}

setInterval(
    () => logProgress('progress update'),
    LOG_PROGRESS_INTERVAL * 1000,
);

// Output a byte stream of newline-separated JSON entries from input
// streamed objects + update metrics
class JSONLStream extends stream.Transform {
    constructor() {
        super({ objectMode: true });
    }

    _transform(diffEntry, encoding, callback) {
        if (diffEntry[0] === null) {
            status.onlyOnLeader += 1;
        } else if (diffEntry[1] === null) {
            status.onlyOnFollower += 1;
        } else {
            status.differingValue += 1;
        }
        this.push(`${JSON.stringify(diffEntry)}\n`);
        callback();
    }

    _flush(callback) {
        this.push(null);
        callback();
    }
}

let diffStreamsSink;
if (EXCLUDE_FROM_CSEQS) {
    diffStreamsSink = new DiffStreamOplogFilter({
        bucketdHost: BUCKETD_HOST,
        bucketdPort: BUCKETD_PORT,
        excludeFromCseqs: EXCLUDE_FROM_CSEQS,
    });
    diffStreamsSink
        .pipe(new JSONLStream())
        .pipe(DIFF_OUTPUT_STREAM);
} else {
    diffStreamsSink = new JSONLStream();
    diffStreamsSink
        .pipe(DIFF_OUTPUT_STREAM);
}

let digestsDb;
if (LISTING_DIGESTS_INPUT_DIR) {
    digestsDb = new Level(LISTING_DIGESTS_INPUT_DIR, { createIfMissing: false });
}

function scanDb(dbPath, cb) {
    log.info('starting scan of database', { dbPath });
    const db = new Level(dbPath);
    const dbRawStream = db.createReadStream();
    const dbListStream = new DBListStream({ dbName: path.basename(dbPath) });
    const diffStream = new DiffStream({
        bucketdHost: BUCKETD_HOST,
        bucketdPort: BUCKETD_PORT,
        digestsDb,
    });

    dbRawStream
        .pipe(dbListStream)
        .pipe(diffStream);
    dbListStream
        .on('data', () => {
            status.keysScanned += 1;
        });
    diffStream
        .on('data', data => {
            if (!diffStreamsSink.write(data)) {
                diffStream.pause();
                diffStreamsSink.once('drain', () => {
                    diffStream.resume();
                });
            }
        })
        .on('end', () => {
            log.info('completed scan of database', { dbPath });
            cb();
        })
        .on('error', err => {
            log.error('error from diff stream', { dbPath, error: err.message });
            cb(err);
        });
}

function main() {
    log.info('starting scan');
    async.series([
        done => async.eachLimit(DATABASES_LIST, PARALLEL_SCANS, scanDb, done),
        done => {
            if (digestsDb) {
                digestsDb.close(done);
            } else {
                done();
            }
        },
        done => {
            diffStreamsSink.end();
            diffStreamsSink.on('finish', done);
        },
    ], err => {
        if (err) {
            logProgress('error during scan');
            process.exit(1);
        }
        logProgress('completed scan');
        DIFF_OUTPUT_STREAM.end();
        DIFF_OUTPUT_STREAM.on('finish', () => {
            process.exit(0);
        });
    });
}

main();

function stop() {
    log.info('stopping execution');
    logProgress('last status');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
