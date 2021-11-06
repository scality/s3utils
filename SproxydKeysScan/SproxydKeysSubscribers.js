const { MultiMap } = require('./DuplicateKeysWindow')
const { repairObject } = require('../VerifyBucketSproxydKeys');
const getObjectURL = require('./VerifyBucketSproxydKeys/getObjectURL');

const subscribers = new MultiMap();

subscribers.set('duplicateSproxyKeyFound', async (params) => {
    const [objectUrl, existingObjectUrl] = [params.objectId, params.existingObjectId].map(id => getObjectURL(id));
    const objInfo = {
        objectUrl: objectUrl,
        objectUrl2: existingObjectUrl,
    };
    
    return repairObject(objInfo, (err, res) => {
        if (err) {
            //what behavior is needed when repairObject fails? Possibly retry up to N times. Send repairObject to a jobs queue and requeue on failure?
            log.error('an error occurred repairing object', {
                objectUrl: objInfo.objectUrl,
                error: { message: err.message },
            });
            // status.objectsErrors += 1; 
            // TODO: handle status in a rolling window. (Maybe not needed?)
        } else {
            // once objects are repaired, what are the new keys? They should be inserted into BoundedMap and continue being tracked
            // we can delete old key 
            for (const [sproxydKey, newKey] in Object.entries(res.copiedKeys)) {
                params.context.sproxydKeys.delete(sproxydKey)
                params.context.sproxydKeys.setAndUpdate(newKey, res.objectUrl); //should be id instead of Url
            };
        };
    });
})

export { subscribers };