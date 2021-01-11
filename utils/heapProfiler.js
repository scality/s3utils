const fs = require('fs');
const path = require('path');
const werelogs = require('werelogs');
const heapProfile = require('heap-profile');

const logger = new werelogs.Logger('Utils::HeapProfiler');

module.exports = (dir, interval) => {
    if (!!process.env.ENABLE_HEAP_PROFILER) {
        if (!fs.existsSync(dir)) {
            try {
                fs.mkdirSync(dir);
            } catch (err) {
                logger.error('unable to create directory for heap profiler');
                logger.error('skipping heap profiling');
                return;
            }
        }

        heapProfile.start();

        setInterval(() => {
            const filename = path.join(
                dir,
                `heap-profiler-${Date.now()}.heapsnapshot`
            );
            heapProfile.write(filename, (err, filename) => {
                if (err) {
                    logger.error(`unable to write heapdump to ${filename}:`, {
                        error: err,
                    });
                }
            });
        }, interval);
    }
};
