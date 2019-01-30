#!/bin/bash

docker volume create cyberway-queue || true

EXPORTER_PASS=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
EXPORTER_USER=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)

if [[ ! -e .env ]]; then
  echo "EXPORTER_USER=$EXPORTER_USER" >> .env
  echo "EXPORTER_PASS=$EXPORTER_PASS" >> .env
fi 

docker-compose up -d
