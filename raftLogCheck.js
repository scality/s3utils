const http = require('http');
const https = require('https');
const async = require('async');
const werelogs = require('werelogs');

const logLevel = Number.parseInt(process.env.DEBUG, 10) === 1
    ? 'debug' : 'info';
const loggerConfig = {
    level: logLevel,
    dump: 'error',
};
werelogs.configure(loggerConfig);
const log = new werelogs.Logger('s3utils::raftLogCheck');

let pageSize = process.env.PAGE_SIZE;
if (pageSize === undefined) {
    pageSize = 1000;
}

let useHttps = process.env.USE_HTTPS;
if (useHttps === undefined) {
    useHttps = false;
}

let protocol = 'http';
if (useHttps) {
    protocol = 'https';
}

const myArgs = process.argv.slice(2);

if (myArgs.length !== 3) {
    log.error(
        'usage: node raftLogCheck.js bucketdHost bucketdPort raftSession',
    );
    process.exit(1);
}

const bucketdHost = myArgs[0];
const bucketdPort = myArgs[1];
const raftSession = myArgs[2];

function getUrl(url, cb) {
    log.debug('url', { url });
    const transport = useHttps ? https : http;
    transport.get(url, res => {
        const body = [];
        res.on('data', chunk => body.push(chunk));
        res.on('end', () => {
            try {
                return cb(null, JSON.parse(body.join('')));
            } catch (err) {
                return cb(err);
            }
        });
    }).on('error', err => (cb(err)));
}

function compareWithLeaderLog(leaderLog, connected, seq, cb) {
    const rlog = log.newRequestLogger();
    rlog.debug('leaderLog:', { leaderLog });
    async.each(connected, (member, cb) => {
        getUrl(
            `${protocol}://${member.host}:${member.adminPort}`
            + `/_/raft/log?begin=${seq}&limit=${pageSize}`,
            (err, memberLog) => {
                if (err) {
                    return cb(err);
                }
                rlog.info('getting member page', { member, seq });
                rlog.debug('memberLog:', { memberLog });
                let i;
                let j;
                for (i = 0;
                    i < pageSize;
                    i++) {
                    if (memberLog.log === undefined
                        || memberLog.log[i] === undefined
                        || memberLog.log[i].entries
                        === undefined) {
                        rlog.warn(
                            'undefined entries at',
                            { seq: seq + i, entry: j },
                        );
                        continue;
                    }
                    if (leaderLog.log !== undefined
                        && leaderLog.log[i] !== undefined
                        && leaderLog.log[i].entries !== undefined) {
                        for (j = 0;
                            j < leaderLog.log[i].entries.length;
                            j++) {
                            let expectKey = true;
                            let expectValue = true;
                            if (leaderLog.log[i].entries[j].key === undefined) {
                                if (leaderLog.log[i].method === 0 // CREATE
                                    || leaderLog.log[i].method === 1 // DELETE
                                    || leaderLog.log[i].method === 7) { // PUT_ATTR
                                    expectKey = false;
                                } else {
                                    rlog.warn('undefined leader key at', {
                                        seq: seq + i,
                                        off: j,
                                        db: leaderLog.log[i].db,
                                        entry: leaderLog.log[i].entries[j],
                                    });
                                    continue;
                                }
                            }
                            if (leaderLog.log[i].entries[j].value
                                === undefined) {
                                if (leaderLog.log[i].entries[j].type
                                    === 'del') {
                                    expectValue = false;
                                } else {
                                    rlog.warn('undefined leader value at', {
                                        seq: seq + i,
                                        off: j,
                                        entry: leaderLog.log[i].entries[j],
                                    });
                                    continue;
                                }
                            }
                            if (memberLog.log[i].entries[j].key
                                === undefined) {
                                if (expectKey) {
                                    rlog.warn('undefined member key at', {
                                        seq: seq + i,
                                        off: j,
                                    });
                                }
                            }
                            if (memberLog.log[i].entries[j].value
                                === undefined) {
                                if (expectValue) {
                                    rlog.warn('undefined member value at', {
                                        seq: seq + i,
                                        off: j,
                                        entry: memberLog.log[i].entries[j],
                                    });
                                }
                            }
                            if (memberLog.log[i].entries[j].key
                                !== leaderLog.log[i].entries[j].key) {
                                rlog.warn('key mismatch at', {
                                    seq: seq + i,
                                    off: j,
                                    key: memberLog.log[i].entries[j].key,
                                });
                            }
                            if (memberLog.log[i].entries[j].value
                                !== leaderLog.log[i].entries[j].value) {
                                rlog.warn('value mismatch at', {
                                    seq: seq + i,
                                    off: j,
                                    value: memberLog.log[i].entries[j].value,
                                });
                            }
                        }
                    }
                }
                return cb();
            },
        );
    }, err => {
        if (err) {
            rlog.error('compareWithLeaderLog error', { error: err.message });
            return cb(err);
        }
        rlog.end();
        return cb();
    });
}

async.waterfall([
    next => {
        getUrl(
            `${protocol}://${bucketdHost}:${bucketdPort}/`
            + `_/raft_sessions/${raftSession}/info`,
            (err, info) => {
                if (err) {
                    log.error('fetching info', { error: err.message });
                    return next(err);
                }
                log.debug('info:', { info });
                return next(
                    null,
                    info.leader.host,
                    info.leader.adminPort,
                    info.connected,
                );
            },
        );
    },
    (leaderHost, leaderPort, connected, next) => {
        getUrl(
            `${protocol}://${leaderHost}:${leaderPort}/_/raft/state`,
            (err, state) => {
                if (err) {
                    log.error('fetching state', { error: err.message });
                    return next(err);
                }
                log.debug('state:', { state });
                return next(
                    null,
                    leaderHost,
                    leaderPort,
                    connected,
                    state.backups * 10000 + 1,
                    state.committed,
                );
            },
        );
    },
    (leaderHost, leaderPort, connected, _bseq, _vseq, next) => {
        let seq = _bseq;
        async.until(() => {
            log.debug('test', { seq, _vseq });
            return seq >= _vseq;
        }, cb => {
            log.info('page', { seq, pageSize });
            getUrl(
                `${protocol}://${leaderHost}:${leaderPort}/`
                + `_/raft/log?begin=${seq}&limit=${pageSize}`,
                (err, leaderLog) => {
                    if (err) {
                        log.error('getting leader log', { error: err.message });
                        return cb(err);
                    }
                    compareWithLeaderLog(
                        leaderLog,
                        connected,
                        seq,
                        err => {
                            if (err) {
                                log.error(
                                    'page error',
                                    { error: err.message },
                                );
                                return cb(err);
                            }
                            log.info('page finished');
                            seq += pageSize;
                            return cb();
                        },
                    );
                    return undefined;
                },
            );
            return undefined;
        }, err => {
            if (err) {
                log.error('paging error', { error: err.message });
                return next(err);
            }
            return next();
        });
    }], err => {
    if (err) {
        log.error('error', { error: err.message });
        process.exit(1);
    }
    log.info('DONE');
    process.exit(0);
});
