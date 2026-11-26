/**
 * @class
 * @classdesc support data structure to check duplicate sproxyd keys:
 * when a sproxyd key that we found and want to insert already
 * exists in this mapping, it's a duplicate.
 */
class FindDuplicateSproxydKeys {
    /**
     * @constructor
     * @param {number} windowSize - number of consecutive calls to
     * this.insertKeys() from which to keep keys
     */
    constructor(windowSize) {
        this.windowSize = windowSize;
        //
        // mapping schema:
        // {
        //     "sproxydKey1": "s3://bucket1/masterKey1",
        //     "sproxydKey2": "s3://bucket1/masterKey1",
        //     "sproxydKey3": "s3://bucket1/masterKey2"
        // }
        this.sproxydKeys = {};

        //
        // The array declared below is used to maintain this window by
        // keeping LISTING_LIMIT + 1 items, each containing an array
        // of sproxyd keys for a single object version.
        //
        // schema:
        // {
        //     123: ["sproxydKey1", "sproxydKey2"], // versionId1
        //     124: ["sproxydKey3"]                 // versionId2
        //     125: ... // max LISTING_LIMIT entries
        // }
        //
        this.versionsWindow = {};
        this.versionsWindowSeq = 0;
    }

    /**
     * Skip one listed version, while still updating the window
     *
     * @return {null} - always returns null
     */
    skipVersion() {
        return this.insertVersion(null, null);
    }

    /**
     * Insert an object version with its sproxyd keys
     *
     * @param {string} objectId object identifier
     * @param {array} keys - array of sproxyd keys to insert

     * @return {null|object}
     *  - if no duplicate key has been found, returns null
     *  - if a duplicate key has been found, returns an info object:
     *    { objectId {string} object URL of which the duplicate key belongs to
     *      key {string} duplicate sproxyd key }
     */
    insertVersion(objectId, keys) {
        let dupInfo = null;
        if (keys) {
            this.versionsWindow[this.versionsWindowSeq] = keys;
            keys.forEach(key => {
                const objectId2 = this.sproxydKeys[key];
                if (objectId2) {
                    // duplicate found
                    dupInfo = { objectId: objectId2, key };
                } else {
                    this.sproxydKeys[key] = objectId;
                }
            });
        }
        this._shiftVersionsWindow();
        return dupInfo;
    }

    _shiftVersionsWindow() {
        this.versionsWindowSeq += 1;
        // cleanup oldest sproxyd keys and window entry (if it exists)
        const oldestKeys = this.versionsWindow[
            this.versionsWindowSeq - this.windowSize];
        if (oldestKeys) {
            oldestKeys.forEach(key => {
                delete this.sproxydKeys[key];
            });
            delete this.versionsWindow[
                this.versionsWindowSeq - this.windowSize];
        }
    }
}

module.exports = FindDuplicateSproxydKeys;
