/**
 * Parse an "older than" string to an absolute cutoff date object.
 *
 * @param {string} olderThan - string to parse: it can be specified in
 * the following forms:
 * - As an ISO date like "2022-07-14T12:30:00Z"
 * - As a relative number of days in the past compared to now, like
 *   "1 day" or "30 days"
 * @return {Date} - the absolute cutoff date
 */
function parseOlderThan(olderThan) {
    const numberOfDaysMatch = /^([0-9]+) days?$/.exec(olderThan);
    if (numberOfDaysMatch) {
        const numberOfDays = Number.parseInt(numberOfDaysMatch[1], 10);
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - numberOfDays);
        return cutoff;
    }
    const numberOfSecsMatch = /^([0-9]+) seconds?$/.exec(olderThan);
    if (numberOfSecsMatch) {
        const numberOfMs = Number.parseInt(numberOfSecsMatch[1], 10) * 1000;
        return new Date(Date.now() - numberOfMs);
    }
    return new Date(olderThan);
}

module.exports = parseOlderThan;
