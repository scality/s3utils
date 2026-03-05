'use strict';

const async = require('async');
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
 * Returns the full metadata object, or null when not found/skipped.
 */
async function fetchFullObjectMetadata(bucketdHostport, bucket, key, versionId, listingParsedMd) {
    const baseUrl = `http://${bucketdHostport}/default/bucket/${bucket}/`
        + encodeURIComponent(key);

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
        const res = await httpRequest('GET', baseUrl);
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
    const res = await httpRequest('GET', primaryUrl);
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
        // eslint-disable-next-line no-await-in-loop
        const altRes = await httpRequest('GET', altUrl);
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
 * Page fetches and individual metadata fetches are each retried up to
 * 100 times on transient errors.
 */
async function* listVersions(bucketdHostport, bucket, listingLimit) {
    let keyMarker = '';
    let versionIdMarker = '';
    let isTruncated = true;

    while (isTruncated) {
        const url = `http://${bucketdHostport}/default/bucket/${bucket}`
            + `?listingType=DelimiterVersions&maxKeys=${listingLimit}`
            + `&keyMarker=${encodeURIComponent(keyMarker)}`
            + `&versionIdMarker=${encodeURIComponent(versionIdMarker)}`;

        // eslint-disable-next-line no-await-in-loop
        const { Versions, IsTruncated, NextKeyMarker, NextVersionIdMarker } = await async.retry(
            { times: 100, interval: 5000 },
            async () => {
                const res = await httpRequest('GET', url);
                if (res.statusCode !== 200) {
                    throw new Error(`GET ${url} returned status ${res.statusCode}`);
                }
                return JSON.parse(res.body);
            }
        );

        for (const entry of (Versions || [])) {
            const { key, versionId } = entry;
            let parsedMd;
            try {
                parsedMd = JSON.parse(entry.value);
            } catch (e) {
                log.warn('failed to parse object metadata', {
                    bucket, key,
                    error: { message: e.message },
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
            if (!needMdFetch) {
                yield { key, versionId, value: parsedMd };
                continue;
            }
            // eslint-disable-next-line no-await-in-loop
            const fullMd = await async.retry(
                { times: 100, interval: 5000 },
                () => fetchFullObjectMetadata(bucketdHostport, bucket, key, versionId, parsedMd)
            );
            if (fullMd === null) {
                log.warn('full object metadata not found or skipped', {
                    bucket, key, versionId,
                });
                continue;
            }
            yield { key, versionId, value: fullMd };
        }

        isTruncated = IsTruncated;
        if (isTruncated) {
            keyMarker = NextKeyMarker || '';
            versionIdMarker = NextVersionIdMarker || '';
        }
    }
}

module.exports = listVersions;
