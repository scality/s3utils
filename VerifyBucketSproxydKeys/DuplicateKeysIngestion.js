const { whilst, waterfall } = require('async');
const { httpRequest } = require('../VerifyBucketSproxydKeys');

const {
    BUCKETD_HOSTPORT,
    RAFT_SESSIONS
} = process.env;

//in order to send keys to DuplicateKeysWindow, we need to read the raft journals
//furthermore, there should be only one of this service across all servers. Ballot should be useful here
class RaftJournalReader {
    constructor(begin, limit, session_id) {
        this.begin = begin;
        this.limit = limit;
        this.url = this._getJournalUrl(session_id)
    }

    _getJournalUrl(session_id) {
        return `http://${BUCKETD_HOSTPORT}/_/raft_sessions/${session_id}`;
    }

    getEntries(begin, limit, cb) {
        const requestUrl = `${this.url}/log?begin=${begin}&limit=${limit}&targetLeader=False`;
        
        return httpRequest('GET', requestUrl, (err, res) => {
                if (err) {
                    return cb(err);
                }
                if (res.statusCode !== 200) {
                    return cb(new Error(`GET ${url} returned status ${res.statusCode}`));
                }
                const entries = JSON.parse(res.body);
                return cb(null, entries);
        });
    }

    processEntries(entries, cb) {
    //TODO
    }

    updateStatus(status, next) {
    //TODO
    }

    run() {
        whilst(
            () => true,
            waterfall([
                (next) => this.getEntries(this.offset, this.limit,
                    (err, res) => {
                        if (err) {
                            //TODO
                        } else {
                            next(null, entries)
                    }
                }),
                (entries, next)  => this.processEntries(entries,
                    (err, res) => {
                        if (err) {
                            //TODO
                        } else {
                            next(null, status)
                        }
                })
            ], err => {
                if (err) {
                    log.error('error in RaftJournalEntries:run',
                              { error: err.message });
                }
                return cb();
            })
        );
    }
}


function stop() {
    log.info('stopping raft journal ingesition');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);