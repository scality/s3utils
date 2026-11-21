const { Logger } = require('werelogs');

const log = new Logger('s3utils:validateLocations');

/**
 * Validates that locations array is valid for objects with non-zero content-length
 * @param {string} objectUrl - The S3 object URL for logging
 * @param {Array} locations - The locations array from metadata
 * @param {Object} status - Status object to update counters
 * @param {Object} findDuplicateSproxydKeys - Object with skipVersion method
 * @returns {boolean} - true if valid, false if invalid
 */
function validateLocations(objectUrl, locations, status, findDuplicateSproxydKeys) {
    // Handle broken metadata where locations is invalid or empty for non-zero content-length objects
    if (!Array.isArray(locations) || locations.length === 0) {
        log.error('object with non-zero content-length has invalid or empty location data', {
            objectUrl,
            locations,
            locationType: typeof locations,
        });
        // eslint-disable-next-line no-param-reassign
        status.objectsScanned += 1;
        // eslint-disable-next-line no-param-reassign
        status.objectsWithBrokenMetadata += 1;
        findDuplicateSproxydKeys.skipVersion();
        return false;
    }
    return true;
}

module.exports = validateLocations;
