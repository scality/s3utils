const fs = require('fs');
const path = require('path');
const v8 = require('v8');
const werelogs = require('werelogs');

const logger = new werelogs.Logger('Utils::HeapProfiler');

function generateFilename(dir) {
    return path.join(dir, `${Date.now()}.heapsnapshot`);
}

function writeSnapshot(filename) {
    try {
        v8.writeHeapSnapshot(filename);
        logger.debug(`heapdump written to ${filename}:`);
    } catch (err) {
        logger.error(`unable to write heapdump to ${filename}:`, {
            error: err,
        });
    }
}

module.exports = (dir, interval) => {
    if (process.env.ENABLE_HEAP_PROFILER
        && process.env.ENABLE_HEAP_PROFILER !== '0') {
        if (!fs.existsSync(dir)) {
            try {
                fs.mkdirSync(dir, { recursive: true });
            } catch (err) {
                logger.error('unable to create directory for heap profiler', {
                    error: err,
                });
                logger.error('skipping heap profiling');
                return;
            }
        }

        writeSnapshot(generateFilename(dir));
        setInterval(() => writeSnapshot(generateFilename(dir)), interval);
        process.on('exit', () => writeSnapshot(generateFilename(dir)));
    }
};
