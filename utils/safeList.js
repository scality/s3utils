const xml2js = require('xml2js');

const recoverableErrors = new Set([
    'TimestampParserError',
]);

const NO_VALUE = Symbol('noValue');

/**
 * @typedef {Object} ExtractResult
 * @property {any} value - extracted value
 * @property {Error|undefined} error - error if any
 */

/**
 * @typedef {function} Extractor
 * @param {string} key - key to extract
 * @param {object} data - data to extract from
 * @returns {ExtractResult} - extracted value
 */

/**
 * extractKeys - extract keys from data using the provided extractors
 *
 * @param {object} data - data to extract keys from
 * @param {object<string, Extractor>} keys - Mapping of keys to extractors
 * @returns {ExtractResult} - extracted values
 */
function extractKeys(data, keys) {
    const extracted = {};
    for (const [key, extractor] of Object.entries(keys)) {
        const { value, error } = extractor(key, data);
        if (error) {
            return { error };
        }
        if (value !== NO_VALUE) {
            extracted[key] = value;
        }
    }
    return { value: extracted };
}

/**
 * safeExtract - extract a value from data
 * @param {string} key - key to extract
 * @param {any} data - data to extract from
 * @returns {ExtractResult} - extracted value
 */
function safeExtract(key, data) {
    if (Array.isArray(data[key]) && data[key].length > 0) {
        return { value: data[key][0] };
    }
    return { value: NO_VALUE };
}

/**
 * safeExtractInt - extract a value from data and parse it as an int
 * If the value cannot be parsed as an int, the raw string value will be returned.
 *
 * @param {string} key - key to extract
 * @param {any} data - data to extract from
 * @returns {ExtractResult} - extracted value
 */
function safeExtractInt(key, data) {
    if (Array.isArray(data[key]) && data[key].length > 0) {
        const parsed = parseInt(data[key][0], 10);
        if (!Number.isNaN(parsed)) {
            return { value: parsed };
        }
        return { value: data[key][0] };
    }

    return { value: NO_VALUE };
}

/**
 * safeExtractDate - extract a value from data and parse it as a ISO8601 date
 * If the value cannot be parsed as a date, the raw string value will be returned.
 *
 * @param {string} key - key to extract
 * @param {any} data - data to extract from
 * @returns {ExtractResult} - extracted value
 */
function safeExtractDate(key, data) {
    if (Array.isArray(data[key]) && data[key].length > 0) {
        const parsed = new Date(data[key][0]);
        if (Number.isNaN(parsed.getTime())) {
            return { value: data[key][0] };
        }
        return { value: parsed };
    }
    return { value: NO_VALUE };
}

/**
 * safeExtractBool - extract a value from data and parse it as a boolean
 * Any value other than the string 'true' will be parsed as false.
 *
 * @param {string} key - key to extract
 * @param {any} data - data to extract from
 * @returns {ExtractResult} - extracted value
 */
function safeExtractBool(key, data) {
    if (Array.isArray(data[key]) && data[key].length > 0) {
        return { value: data[key][0] === 'true' };
    }
    return { value: NO_VALUE };
}

/**
 * safeExtractArray - extract an array of values from the array found at key
 *
 * @param {string} key - key containing the array to extract
 * @param {any} data - data to extract from
 * @param {object<string, Extractor>} arrKeys - Mapping of keys to extractors
 * @returns {ExtractResult} - extracted value
 */
function safeExtractArray(key, data, arrKeys) {
    const arr = [];
    if (Array.isArray(data[key])) {
        for (const item of data[key]) {
            const { value, error } = extractKeys(item, arrKeys);
            if (error) {
                return { error };
            }
            arr.push(value);
        }
    }
    return { value: arr };
}

const ownerKeys = {
    ID: safeExtract,
    DisplayName: safeExtract,
};

/**
 * safeExtractOwner - extract an owner object from data
 * @param {string} key - key to extract
 * @param {any} data - data to extract from
 * @returns {ExtractResult} - extracted value
 */
function extractOwner(key, data) {
    if (!Array.isArray(data[key]) || data[key].length === 0) {
        return { value: NO_VALUE };
    }

    const { value, error } = extractKeys(data[key][0], ownerKeys);
    if (error) {
        return { error };
    }
    return { value };
}

const listingKeys = {
    Name: safeExtract,
    Prefix: safeExtract,
    KeyMarker: safeExtract,
    VersionIdMarker: safeExtract,
    NextKeyMarker: safeExtract,
    NextVersionIdMarker: safeExtract,
    Delimiter: safeExtract,
    MaxKeys: safeExtractInt,
    IsTruncated: safeExtractBool,
    EncodingType: safeExtract,
    RequestCharged: safeExtract,
};

const versionKeys = {
    Key: safeExtract,
    VersionId: safeExtract,
    IsLatest: safeExtractBool,
    LastModified: safeExtractDate,
    ETag: safeExtract,
    Size: safeExtractInt,
    StorageClass: safeExtract,
    Owner: extractOwner,
};

const deleteMarkerKeys = {
    Key: safeExtract,
    VersionId: safeExtract,
    IsLatest: safeExtractBool,
    LastModified: safeExtractDate,
    ETag: safeExtract,
    Owner: extractOwner,
    StorageClass: safeExtract,
};

const commonPrefixKeys = {
    Prefix: safeExtract,
};


/**
 * recoverListing - parse an xml listing response in a safe manner
 * @param {object} resp - response from complete event of listObjectVersions
 * @param {object|undefined} resp.httpResponse - http response from S3 (if any)
 * @param {Error|undefined} resp.error - error from S3 (if any)
 * @param {function} cb - callback
 * @returns {undefined}
 */
function recoverListing(resp, cb) {
    const parser = new xml2js.Parser();
    parser.parseString(resp.httpResponse.body, (err, data) => {
        if (err) {
            // if there is an error parsing the xml, return the original error
            return cb(resp.error);
        }

        if (!data.ListVersionsResult) {
            // if no listing data is present, return the original error
            return cb(resp.error);
        }

        const { value, error } = extractKeys(data.ListVersionsResult, listingKeys);
        if (error) {
            return cb(error);
        }

        const { value: versions, error: verErr } = safeExtractArray(
            'Version',
            data.ListVersionsResult,
            versionKeys,
        );
        if (verErr) {
            return cb(verErr);
        }

        const { value: deleteMarkers, error: delErr } = safeExtractArray(
            'DeleteMarker',
            data.ListVersionsResult,
            deleteMarkerKeys,
        );
        if (delErr) {
            return cb(delErr);
        }

        const { value: commonPrefixes, error: comErr } = safeExtractArray(
            'CommonPrefix',
            data.ListVersionsResult,
            commonPrefixKeys,
        );
        if (comErr) {
            return cb(comErr);
        }

        value.Versions = versions;
        value.DeleteMarkers = deleteMarkers;
        value.CommonPrefixes = commonPrefixes;
        return cb(null, value);
    });
}

/**
 *  safeListObjectVersions - listObjectVersions with error recovery
 *
 * This function is a wrapper around listObjectVersions that will attempt to
 * recover from certain format errors by parsing the XML response and returning the
 * data in the same format as listObjectVersions.
 *
 * Values that fail to parse will be returned as the raw string value present in the xml.
 *
 * @param {AWS.S3} s3 - S3 client instance
 * @param {object} params - listObjectVersions params
 * @param {function} cb - callback
 * @returns {undefined}
 */
function safeListObjectVersions(s3, params, cb) {
    const req = s3.listObjectVersions(params);
    req.on('complete', resp => {
        if (resp.error) {
            if (recoverableErrors.has(resp.error.code)) {
                return recoverListing(resp, cb);
            }
            return cb(resp.error);
        }
        return cb(null, resp.data);
    });
    req.send();
}

module.exports = {
    safeListObjectVersions,
    recoverListing,
};
