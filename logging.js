const DEBUG = 1;
const INFO = 2;
const WARN = 3;
const ERROR = 4;
const FATAL = 5;

const LEVEL_TO_STR = {
    1: 'debug',
    2: 'info',
    3: 'warn',
    4: 'error',
    5: 'fatal',
};

/* eslint-disable no-console */

/**
 * @class
 * @classdesc Simple logging wrapper for scripts, logging JSON lines
 * to stderr, with API similar to werelogs
 */
class Logger {
    constructor(loggerName) {
        this._loggerName = loggerName;
        this._logLevel = INFO;
    }

    /**
     * Set log level
     *
     * @param {Number} level - 1=DEBUG 2=INFO 3=WARN 4=ERROR 5=FATAL
     * @return {undefined}
     */
    setLogLevel(level) {
        this._logLevel = level;
    }

    _log(level, message, extraArgs) {
        if (level >= this._logLevel) {
            console.error(JSON.stringify(Object.assign({}, {
                name: this._loggerName,
                time: Date.now(),
                level: LEVEL_TO_STR[level],
                message,
            }, extraArgs || {})));
        }
        return this;
    }

    debug(message, extraArgs) {
        return this._log(DEBUG, message, extraArgs);
    }
    info(message, extraArgs) {
        return this._log(INFO, message, extraArgs);
    }
    warn(message, extraArgs) {
        return this._log(WARN, message, extraArgs);
    }
    error(message, extraArgs) {
        return this._log(ERROR, message, extraArgs);
    }
    fatal(message, extraArgs) {
        return this._log(FATAL, message, extraArgs);
    }
}

module.exports = {
    Logger,
};

/* eslint-enable no-console */
