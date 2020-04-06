#!/bin/sh

# Tries to pull the specified tag of a specified image
# If it can not it builds, tags, and pushes one.

DOCKER_REPO='registry.scality.com/zenko'
CONTEXT="$1"

if [ -z "$DOCKER_IMAGE" ]; then
    echo 'DOCKER_IMAGE environment variable has not been set. quitting'
    exit 1
fi

if [ -z "$DOCKER_TAG" ]; then
    echo "DOCKER_TAG not found, defaulting to 'latest'"
    DOCKER_TAG='latest'
fi

echo "Trying to pull $DOCKER_REPO/$DOCKER_IMAGE:$DOCKER_TAG"

docker pull $DOCKER_REPO/$DOCKER_IMAGE:$DOCKER_TAG

if [ "$?" -ne 0 ]; then
    echo "Image not found, building..."
    set -e
    docker build -t $DOCKER_REPO/$DOCKER_IMAGE:$DOCKER_TAG $CONTEXT
    docker push $DOCKER_REPO/$DOCKER_IMAGE:$DOCKER_TAG
else
    echo "Pulled image."
fi