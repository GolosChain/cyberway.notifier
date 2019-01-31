#!/bin/bash

FROM=${1:-1}
TO=${2:-10}

for((i=$FROM;i<=$TO;i+=1)); do
    echo "Message $i" >> /var/lib/docker/volumes/cyberway-queue/_data/msg.txt
done
