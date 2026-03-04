

const werelogs = require('werelogs');

const httpRequest = require('../httpRequest');

const log = new werelogs.Logger('s3utils:listVersions');


/**
 * Fetch the complete metadata of an object version from bucketd.
 *
 * Correctly handles non-versioned objects and null versions:
 *
 * - Non-versioned (versionId === 'null'): fetch without a versionId query
 *   param; reject the result if it now has a versionId field (the object was
 *   overwritten by a versioned one since the listing was taken).
 *
 * - Versioned: try the primary ?versionId=<id> URL first. If that fails and
 *   the listing entry carries isNull, try the master-key URL and
 *   ?versionId=null as fallbacks, accepting the result only when its versionId
 *   matches the expected one.
 *
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} bucket - bucket name
 * @param {string} key - object key
 * @param {string} versionId - version ID from the listing entry
 * @param {object} listingParsedMd - parsed metadata from the listing entry
 * @param {object} [retryParams] - retry parameters forwarded to httpRequest
 * @returns {Promise<object|null>} full metadata object, or null if not found/skipped
 */
async function fetchFullObjectMetadata(bucketdHostport, bucket, key, versionId, listingParsedMd, retryParams) {
    const baseUrl = `http://${bucketdHostport}/default/bucket/${bucket}/${
        encodeURIComponent(key)}`;

    function parseResponse(url, res) {
        if (res.statusCode === 404) {
            return null;
        }
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        try {
            return JSON.parse(res.body);
        } catch (e) {
            throw new Error(`failed to parse metadata from ${url}: ${e.message}`);
        }
    }

    if (versionId === 'null') {
        // Non-versioned object: fetch without versionId param
        const res = await httpRequest('GET', baseUrl, retryParams);
        const fullMd = parseResponse(baseUrl, res);
        if (fullMd === null) {
            return null; // 404: object is gone
        }
        if ('versionId' in fullMd) {
            // Object has since been overwritten by a versioned one; skip
            return null;
        }
        return fullMd;
    }

    // Versioned object: try the primary URL first
    const primaryUrl = `${baseUrl}?versionId=${encodeURIComponent(versionId)}`;
    const res = await httpRequest('GET', primaryUrl, retryParams);
    if (res.statusCode === 200) {
        return parseResponse(primaryUrl, res);
    }
    if (res.statusCode !== 404) {
        throw new Error(`GET ${primaryUrl} returned status ${res.statusCode}`);
    }
    // Primary returned 404; if the listing entry is a null version,
    // try alternative URLs
    if (!('isNull' in listingParsedMd)) {
        return null;
    }
    for (const altUrl of [baseUrl, `${baseUrl}?versionId=null`]) {
        const altRes = await httpRequest('GET', altUrl, retryParams);
        const altMd = parseResponse(altUrl, altRes);
        if (altMd !== null && altMd.versionId === versionId) {
            return altMd;
        }
    }
    return null;
}

/**
 * Async generator that iterates over all versions in a bucket using
 * DelimiterVersions listing. Yields { key, versionId, value } for each
 * version, where value is the fully resolved metadata. When the listing
 * result has a pruned location array (large MPUs), the full metadata is
 * fetched individually.
 *
 * Page fetches and individual metadata fetches are retried on transient
 * errors (network failures and 5xx responses) using RETRY_PARAMS.
 *
 * @param {string} bucketdHostport - host:port of the bucketd endpoint
 * @param {string} bucket - name of the bucket to list
 * @param {object} [options] - listing options
 * @param {number} [options.pageSize=1000] - number of entries requested per
 *   listing page (passed as maxKeys to bucketd)
 * @param {number} [options.maxItems] - maximum total number of entries to
 *   yield; if omitted, all entries are yielded
 * @param {string} [options.prefix] - only yield entries whose key starts with
 *   this prefix; an empty string (the default) applies no prefix filter
 * @param {string} [options.keyMarker] - resume listing from this key marker
 *   (exclusive); defaults to the beginning of the bucket
 * @param {string} [options.versionIdMarker] - resume listing from this
 *   version ID marker, used together with keyMarker
 * @param {object} [options.retry] - if provided, passed as retryParams to
 *   httpRequest to retry on network errors and 5xx responses
 *   (e.g. { times: 100, interval: 5000 }); by default requests are not retried
 * @returns {AsyncGenerator<{key: string, versionId: string, value: object}>}
 *   async generator yielding one entry per version
 */
async function* listVersions(bucketdHostport, bucket, {
    pageSize = 1000,
    maxItems,
    prefix = '',
    keyMarker: startKeyMarker = '',
    versionIdMarker: startVersionIdMarker = '',
    retry,
} = {}) {
    let keyMarker = startKeyMarker;
    let versionIdMarker = startVersionIdMarker;
    let isTruncated = true;
    let remaining = maxItems ?? Infinity;

    while (isTruncated && remaining > 0) {
        const maxKeys = Math.min(pageSize, remaining);
        const url = `http://${bucketdHostport}/default/bucket/${bucket}`
            + `?listingType=DelimiterVersions&maxKeys=${maxKeys}${
                prefix ? `&prefix=${encodeURIComponent(prefix)}` : ''
            }&keyMarker=${encodeURIComponent(keyMarker)}`
            + `&versionIdMarker=${encodeURIComponent(versionIdMarker)}`;

        const res = await httpRequest('GET', url, retry);
        if (res.statusCode !== 200) {
            throw new Error(`GET ${url} returned status ${res.statusCode}`);
        }
        const {
            Versions,
            IsTruncated,
            NextKeyMarker,
            NextVersionIdMarker,
        } = JSON.parse(res.body);

        for (const entry of (Versions || [])) {
            const { key, versionId } = entry;
            let parsedMd;
            try {
                parsedMd = JSON.parse(entry.value);
            } catch (e) {
                log.warn('failed to parse object metadata', {
                    bucket,
                    key,
                    error: e.message,
                });
                continue;
            }
            // Only fetch full metadata when the listing result has a pruned
            // location array: typically the field is absent for large MPUs
            const needMdFetch = (
                'content-length' in parsedMd
                && parsedMd['content-length'] !== 0
                && (parsedMd.location === undefined || parsedMd.location === null)
            );
            if (needMdFetch) {
                parsedMd = await fetchFullObjectMetadata(bucketdHostport, bucket, key, versionId, parsedMd, retry);
                if (parsedMd === null) {
                    log.debug('full object metadata not found or skipped', {
                        bucket, key, versionId,
                    });
                    continue;
                }
            }
            yield { key, versionId, value: parsedMd };
            --remaining;
        }

        isTruncated = IsTruncated;
        if (isTruncated) {
            keyMarker = NextKeyMarker || '';
            versionIdMarker = NextVersionIdMarker || '';
        }
    }
}

module.exports = listVersions;
