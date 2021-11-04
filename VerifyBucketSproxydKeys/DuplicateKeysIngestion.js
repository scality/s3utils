const { whilst, waterfall } = require('async');

//in order to send keys to DuplicateKeysWindow, we need to read the raft journals
//furthermore, there should be only one of this service across all servers. Ballot should be useful here

class RaftJournalEntries {
    constructor(limit, offset) {
        this.limit = limit;
        this.offset = offset;
    }

    getEntries(offset, limit, next) {
    //TODO
    }

    processEntries(entries, next) {
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