#!/bin/bash

set -euo pipefail

wget https://raw.githubusercontent.com/nats-io/prometheus-nats-exporter/master/docker/linux/amd64/Dockerfile -O Dockerfile-exporter
docker build -t cyberway/nats-exporter:latest -f Dockerfile-exporter .

docker images

docker login -u=$DHUBU -p=$DHUBP
docker push cyberway/nats-exporter:latest

rm Dockerfile-exporter
