const async = require('async');
const { jsutil } = require('arsenal');

class StalledRequestHandler {
    constructor(client, params) {
        this.client = client;
        this.dryRun = params.dryRun;
        this.batchSize = params.batchSize;
        this.concurrentRequests = params.concurrentRequests;
        this.log = params.log;

        this.queue = null;
        this.killed = false;

        this.queueSetup();
    }

    queueSetup() {
        this.queue = async.queue(({ batch, getNext }, done) => {
            this.log.info(`retrying stalled objects, count# ${batch.length}`);
            if (this.dryRun) {
                this.log.info('dry run, skipping retry request');
                return getNext(done);
            }

            if (typeof this.client.retryFailedObjects !== 'function') {
                return done(new Error('invalid stalled handler client'));
            }

            return this.client.retryFailedObjects({
                Body: JSON.stringify(batch),
            }, err => {
                if (err) {
                    return done(err);
                }
                return getNext(done);
            });
        }, this.concurrentRequests);
    }

    isInProgress() {
        this.log.debug('queue progress', {
            running: this.queue.running(),
            inQueue: this.queue.length(),
        });
        return (this.queue.running() > 0 || this.queue.length() > 0);
    }

    kill() {
        this.queue.kill();
        this.killed = true;
    }

    _waitForCompletion(cb) {
        async.whilst(() =>
            (!this.killed && this.isInProgress()),
        done => setTimeout(done, 1000), cb);
    }

    handleRequests(source, cb) {
        // todo: check using instanceof?
        if (!source || typeof source.getBatch !== 'function') {
            return cb(new Error('Invalid cursor source'));
        }

        if (this.killed) {
            return cb(new Error('Terminated handler'));
        }

        const onceCB = jsutil.once(cb);
        const nextBatch = cb => source.getBatch(
            this.batchSize,
            (err, batch) => {
                if (err) {
                    this.log.error('failed to retrieve batch', {
                        method: '_handleNextBatch',
                        error: err,
                    });
                    return cb(err);
                }

                if (batch !== null) {
                    this.queue.push({ batch, getNext: nextBatch });
                }

                return cb(null);
            });

        this.queue.error = (err, batch) => {
            this.log.error('error occurred while processing request', {
                method: 'StalledRequestHandler::handleRequests',
                error: err,
                batch,
            });
            this.kill();
            return onceCB(err);
        };

        return async.times(
            this.concurrentRequests,
            (_, cb) => nextBatch(cb),
            err => {
                if (err) {
                    this.log.error('failed to populate queue', {
                        method: '::handleRequests',
                        error: err,
                    });
                    return onceCB(err);
                }

                // wait for queue to be done and source to be consumed
                return this._waitForCompletion(err => {
                    if (err) {
                        this.killed = true;
                        this.queue.error = null;
                        return onceCB(err);
                    }

                    const result = {
                        bucket: source.bucketName,
                        stalled: source.getInfo().stalled,
                    };

                    this.queue.error = null;
                    return onceCB(null, result);
                });
            }
        );
    }
}

module.exports = {
    StalledRequestHandler,
};

