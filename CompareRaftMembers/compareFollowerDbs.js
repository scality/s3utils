/* eslint-disable no-console */

const async = require('async');
const fs = require('fs');
const path = require('path');
const stream = require('stream');

const { Logger } = require('werelogs');
const Level = require('level');

const DBListStream = require('./DBListStream');
const DiffStreamOplogFilter = require('./DiffStreamOplogFilter');
const KeyValueStreamDiff = require('./KeyValueStreamDiff');

const DEFAULT_PARALLEL_SCANS = 4;
const DEFAULT_LOG_PROGRESS_INTERVAL = 10;

const USAGE = `
compareFollowerDbs.js

This tool compares offline Metadata leveldb databases of two different
Metadata nodes, and outputs the differences to the file path given as
the DIFF_OUTPUT_FILE environment variable.

In this file, it outputs each key that differs as line-separated JSON
entries, where each entry can be one of:

- [{ key, value }, null]: this key is present on follower #1 but not
  on follower #2

- [null, { key, value }]: this key is not present on follower #1 but
  is present on follower #2

- [{ key, value: "{value1}" }, { key, value: "{value2}" }]: this key
  has a different value between follower #1 and follower #2: "value1"
  is the value seen on follower #1 and "value2" the value seen on
  follower #2.

Usage:
    node compareFollowerDbs.js

Mandatory environment variables:
    DATABASES1: space-separated list of databases of follower #1 to
    compare against follower #2
    DATABASES2: space-separated list of databases of follower #2 to
    compare against follower #1
    DIFF_OUTPUT_FILE: file path where diff output will be stored

Optional environment variables:
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
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint, needed when EXCLUDE_FROM_CSEQS is set
`;

for (const envVar of [
    'DATABASES1',
    'DATABASES2',
    'DIFF_OUTPUT_FILE',
]) {
    if (!process.env[envVar]) {
        console.error(`ERROR: ${envVar} not defined`);
        console.error(USAGE);
        process.exit(1);
    }
}

const {
    DATABASES1,
    DATABASES2,
    DIFF_OUTPUT_FILE,
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

const DATABASE_LISTS = [
    DATABASES1,
    DATABASES2,
].map(
    databases => databases
        .split(' ')
        .filter(dbPath => {
            const dbName = path.basename(dbPath);
            return !['sdb', 'stdb', 'dbAttributes'].includes(dbName);
        })
        .sort(),
);

if (DATABASE_LISTS[0].length !== DATABASE_LISTS[1].length) {
    console.error('ERROR: DATABASES1 and DATABASES2 must contain the same number of database paths');
    process.exit(1);
}

const DATABASE_COMPARISON_PAIRS = [];
for (let i = 0; i < DATABASE_LISTS[0].length; ++i) {
    DATABASE_COMPARISON_PAIRS.push([
        DATABASE_LISTS[0][i],
        DATABASE_LISTS[1][i],
    ]);
}

const DIFF_OUTPUT_STREAM = fs.createWriteStream(DIFF_OUTPUT_FILE, { flags: 'wx' });

const log = new Logger('s3utils:CompareRaftMembers:compareFollowerDbs');

const status = {
    keysScanned: 0,
    onlyOnFollower1: 0,
    onlyOnFollower2: 0,
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
            status.onlyOnFollower2 += 1;
        } else if (diffEntry[1] === null) {
            status.onlyOnFollower1 += 1;
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
    const { BUCKETD_HOSTPORT } = process.env;
    if (!BUCKETD_HOSTPORT) {
        console.error('ERROR: BUCKETD_HOSTPORT env var is required for EXCLUDE_FROM_CSEQS');
        console.error(USAGE);
        process.exit(1);
    }
    const [BUCKETD_HOST, BUCKETD_PORT] = BUCKETD_HOSTPORT.split(':');
    if (!BUCKETD_PORT) {
        console.error('ERROR: BUCKETD_HOSTPORT must be of form "ip:port"');
        console.error(USAGE);
        process.exit(1);
    }
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

function compareDbs(dbPair, cb) {
    log.info(
        'starting comparison of pair of databases',
        { db1: dbPair[0], db2: dbPair[1] },
    );
    const dbListStreams = dbPair.map(dbPath => {
        const db = new Level(dbPath);
        const dbRawStream = db.createReadStream();
        const dbListStream = new DBListStream({ dbName: path.basename(dbPath) });
        dbRawStream.pipe(dbListStream);
        return dbListStream;
    });
    dbListStreams[0]
        .on('data', () => {
            status.keysScanned += 1;
        });

    const diffStream = new KeyValueStreamDiff(dbListStreams[0], dbListStreams[1]);
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
            log.info('completed scan of database pair', { db1: dbPair[0], db2: dbPair[1] });
            cb();
        })
        .on('error', err => {
            log.error(
                'error from diff stream',
                { db1: dbPair[0], db2: dbPair[1], error: err.message },
            );
            cb(err);
        });
}

function main() {
    log.info('starting scan');
    async.series([
        done => async.eachLimit(DATABASE_COMPARISON_PAIRS, PARALLEL_SCANS, compareDbs, done),
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
