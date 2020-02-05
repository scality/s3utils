const cluster = require('cluster');

if (cluster.isMaster) {
    require('./CountItems/masterProcess');
} else if (cluster.isWorker) {
    require('./CountItems/workerProcess');
}

