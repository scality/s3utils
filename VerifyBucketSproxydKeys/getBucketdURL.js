function getBucketdURL(hostPort, params) {
    let baseURL = `http://${hostPort}/default/bucket/${params.Bucket}`;
    if (params.Key !== undefined) {
        baseURL += `/${encodeURIComponent(params.Key)}`;
    }
    const queryParams = [];
    if (params.MaxKeys) {
        queryParams.push(`maxKeys=${params.MaxKeys}`);
    }
    if (params.KeyMarker !== undefined) {
        queryParams.push(`marker=${encodeURIComponent(params.KeyMarker)}`);
    }
    return `${baseURL}${queryParams.length > 0 ? '?' : ''}${queryParams.join('&')}`;
}

module.exports = getBucketdURL;
