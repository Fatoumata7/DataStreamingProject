#!/bin/bash
echo "Starting Kafka broker..."
docker rm -f kafka-broker
docker network create teaching
docker run -it --rm --name kafka-broker --network teaching --user root -p 9000:9000 -p 9092:9092 ghcr.io/osekoo/kafka:3.5
