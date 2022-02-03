const async = require('async');
const { jsutil } = require('arsenal');

class StalledRequestHandler {
    constructor(client, params) {
        this.client = client;
        this.dryRun = params.dryRun;
        this.batchSize = params.batchSize;
        this.concurrentRequests = params.concurrentRequests;
        this.log = params.log;

        this._queue = null;
        this.killed = false;
        this.queueError = null;

        this.queueSetup();
    }

    queueSetup() {
        this._queue = async.queue(({ bucket, batch, getNext }, done) => {
            if (this.queueError !== null) {
                return done();
            }

            this.log.info(`retrying stalled objects, count# ${batch.length}`);
            if (this.dryRun) {
                this.log.info('dry run, skipping retry request');
                return getNext(done);
            }

            if (typeof this.client.retryFailedObjects !== 'function') {
                return done(new Error('invalid stalled handler client'));
            }

            return this.client.retryFailedObjects({
                Body: JSON.stringify(batch.map(entry => entry.toObject())),
            }, err => {
                if (err) {
                    return done(err);
                }
                return getNext(done);
            });
        }, this.concurrentRequests);

        this._queue.error = (err, { bucket, batch }) => {
            this.log.error('error occurred while processing request', {
                error: err,
                lastBatch: batch,
                bucket,
            });
            this.queueError = err;
            this.kill();
        };
    }

    isInProgress() {
        this.log.debug('queue progress', {
            running: this._queue.running(),
            inQueue: this._queue.length(),
        });
        return (this._queue.running() > 0 || this._queue.length() > 0);
    }

    kill() {
        this._queue.kill();
        this.killed = true;
    }

    _waitForCompletion(cb) {
        async.whilst(
            () => (!this.killed && this.isInProgress()),
            done => setTimeout(done, 1000),
            cb,
        );
    }

    handleRequests(source, cb) {
        // todo: check using instanceof?
        if (!source || typeof source.getBatch !== 'function') {
            return cb(new Error('Invalid cursor source'));
        }

        if (this.killed) {
            return cb(new Error('terminated handler'));
        }

        const onceCB = jsutil.once(cb);
        const nextBatch = cb => source.getBatch(
            this.batchSize,
            (err, batch) => {
                if (err) {
                    this.log.error('failed to retrieve batch', {
                        method: 'nextBatch',
                        error: err,
                    });
                    return cb(err);
                }

                if (batch !== null) {
                    this._queue.push({
                        bucket: source.bucketName,
                        batch,
                        getNext: nextBatch,
                    });
                }

                return cb(null);
            },
        );

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
                return this._waitForCompletion(() => {
                    if (this.queueError) {
                        this.killed = true;
                        return onceCB(this.queueError);
                    }

                    const result = {
                        bucket: source.bucketName,
                        stalled: source.getInfo().stalled,
                    };

                    return onceCB(null, result);
                });
            },
        );
    }
}

module.exports = {
    StalledRequestHandler,
};

