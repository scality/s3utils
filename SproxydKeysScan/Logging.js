const werelogs = require('werelogs');
const { env } = require('./env');

/**
 * @class
 * @classdesc - aggregates events in a map and logs them at an interval.
 * call .run() after instantiation to being loop.
 */
class AggregateLogger {
    /**
     * @constructor
     * @param {number} logInterval - log summary statistics once every intervalSize seconds
     */
    constructor(logInterval) {
        this.log = new werelogs.Logger('s3utils:SproxydKeysScan:AggregateLogger');
        if (AggregateLogger._instance) {
            const message = 'AggregateLogger is a singleton and has been instatiated. This instantiation is ignored';
            AggregateLogger._instance.log.debug(message);
            return AggregateLogger._instance;
        }
        this.interval = logInterval;
        this.beginTime = new Date();
        this.statistics = new Map();
        AggregateLogger._instance = this;
        return AggregateLogger._instance;
    }

    /**
     * @param {string} event - increases frequency of event string by 1 in this.statistics.
     * @returns {undefined}
     */
    update(event) {
        this.statistics.set(event, (this.statistics.get(event) || 0) + 1);
    }

    /**
     * logs the current event frequencies in this.statistics and resets the counts for the next interval.
     * @returns {undefined}
     */
    logInterval() {
        const currentTime = new Date();
        const summary = {};
        for (const [k, v] of this.statistics) {
            summary[k] = v;
        }
        const data = {
            beginTime: this.beginTime.toISOString(),
            endTime: currentTime.toISOString(),
            summary,
        };
        this.log.info('Summary update:', data);

        // setup for next interval
        this.beginTime = currentTime;
        this.statistics.clear();
    }

    run() {
        setInterval(this.logInterval.bind(this), this.interval * 1000);
    }
}
/**
 * @class
 * @classdesc - creates a proxy to a werelogs.Logger class instance.
 */
class ProxyLoggerCreator {
    /**
    * @constructor
    * @param {class} logger - an instance of werelogs.Logger
    * @returns {class} - a proxy to the logger
    */
    constructor(logger) {
        this.logger = logger;
        this.aggregateLogger = new AggregateLogger(env.LOG_INTERVAL);
        const context = this;
        const logLevels = new Set(['trace', 'debug', 'info', 'warn', 'error', 'fatal']);
        const handlers = {
            get(target, prop) {
                if (typeof target[prop] === 'function') {
                    return new Proxy(target[prop], {
                        apply: (target, thisArg, argumentsList) => {
                            if (logLevels.has(prop)) {
                                context.logLevelHandler.apply(context, argumentsList);
                            }
                            return Reflect.apply(target, thisArg, argumentsList);
                        },
                    });
                }
                return Reflect.get(target, prop);
            },
        };

        return new Proxy(this.logger, handlers);
    }

    /**
     * intercepts calls to the instance and updates AggregateLogger.
     * @param {string} message - log level message
     * @param {Object} data - log level data. if eventMessage is a property
     * of this object, then the value of eventMessage is updated in AggregateLogger
     * @return {undefined}
     */
    logLevelHandler(message, data) {
        if (data && data.eventMessage) {
            this.aggregateLogger.update(data.eventMessage);
        }
    }
}

module.exports = { ProxyLoggerCreator, AggregateLogger };
