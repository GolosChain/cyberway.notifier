#!/bin/bash
set -euo pipefail

IMAGETAG=${BUILDKITE_BRANCH:-master}
BRANCHNAME=${BUILDKITE_BRANCH:-master}

docker images

docker login -u=$DHUBU -p=$DHUBP
docker push cyberway/cyberway-notifier:${IMAGETAG}

if [[ "${IMAGETAG}" == "master" ]]; then
    docker tag cyberway/cyberway-notifier:${IMAGETAG} cyberway/cyberway-notifier:stable
    docker push cyberway/cyberway-notifier:stable
fi

if [[ "${IMAGETAG}" == "develop" ]]; then
    docker tag cyberway/cyberway-notifier:${IMAGETAG} cyberway/cyberway-notifier:latest
    docker push cyberway/cyberway-notifier:latest
fi
