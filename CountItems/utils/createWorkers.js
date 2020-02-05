const cluster = require('cluster');
const CountWorkerObj = require('../CountWorkerObj');

function createWorkers(numWorkers) {
    if (!cluster.isMaster) return {};
    const workers = {};
    for (let i = 0; i < numWorkers; ++i) {
        const worker = cluster.fork();
        workers[worker.process.pid] =
            new CountWorkerObj(worker.process.pid, worker);
    }
    return workers;
}

module.exports = createWorkers;
