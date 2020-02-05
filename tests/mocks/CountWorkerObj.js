class CountWorkerObj {
    constructor(id) {
        this.id = id;
        this.callbacks = [];
    }

    init(callback) {
        this.callbacks.push(callback);
    }

    setup(callback) {
        this.callbacks.push(callback);
    }

    teardown(callback) {
        this.callbacks.push(callback);
    }

    count(_bucketInfos, callback) {
        this.callbacks.push(callback);
    }

    kill() {}
}

module.exports = CountWorkerObj;
