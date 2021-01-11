const cluster = require('cluster');

const { parseEnvInt } = require('./utils');

const HEAP_PROFILER_INTERVAL =
    parseEnvInt(process.env.HEAP_PROFILER_INTERVAL, 10 * 60 * 100);
const HEAP_PROFILER_PATH = process.env.HEAP_PROFILER_PATH;

if (cluster.isMaster) {
    require('./utils/heapProfiler')(
        `${HEAP_PROFILER_PATH}/master`,
        HEAP_PROFILER_INTERVAL
    );
    require('./CountItems/masterProcess');
} else if (cluster.isWorker) {
    require('./utils/heapProfiler')(
        `${HEAP_PROFILER_PATH}/worker/${cluster.worker.id}`,
        HEAP_PROFILER_INTERVAL
    );
    require('./CountItems/workerProcess');
}

