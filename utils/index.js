
/**
 * parse environment variable as a integer number
 * @param {string} env - environment variable value
 * @param {int} defValue - default value if environment cannot be found or is
 *      not int parseable
 * @returns {int} - return integer value
 */
function parseEnvInt(env, defValue) {
    return (env && !Number.isNaN(env))
        ? Number.parseInt(env, 10)
        : defValue;
}

module.exports = {
    parseEnvInt,
};
