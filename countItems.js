const cluster = require('cluster');

const { parseEnvInt } = require('./utils');

const HEAP_PROFILER_INTERVAL_MS = parseEnvInt(process.env.HEAP_PROFILER_INTERVAL_MS, 10 * 60 * 1000);
const { HEAP_PROFILER_PATH } = process.env;

if (cluster.isMaster) {
    require('./utils/heapProfiler')(
        `${HEAP_PROFILER_PATH}/master`,
        HEAP_PROFILER_INTERVAL_MS,
    );
    require('./CountItems/masterProcess');
} else if (cluster.isWorker) {
    require('./utils/heapProfiler')(
        `${HEAP_PROFILER_PATH}/worker/${cluster.worker.id}`,
        HEAP_PROFILER_INTERVAL_MS,
    );
    require('./CountItems/workerProcess');
}

