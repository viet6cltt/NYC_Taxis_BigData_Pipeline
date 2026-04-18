#!/bin/bash

# Build the Docker image for the historical to bronze batch job
docker build -t nyc-taxi-batch:v1 -f batch/historical_to_bronze/Dockerfile ..

# Build the Docker image for the replay producer
docker build -t nyc-taxi-replay:v1 -f streaming/replay_producer/Dockerfile ..

