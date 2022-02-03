const fs = require('fs');
const path = require('path');
const werelogs = require('werelogs');
const heapdump = require('heapdump');

const logger = new werelogs.Logger('Utils::HeapProfiler');

function generateFilename(dir) {
    return path.join(dir, `${Date.now()}.heapsnapshot`);
}

function writeSnapshot(filename) {
    heapdump.writeSnapshot(filename, (err, filename) => {
        if (err) {
            logger.error(`unable to write heapdump to ${filename}:`, {
                error: err,
            });
        } else {
            logger.debug(`heapdump written to ${filename}:`);
        }
    });
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
