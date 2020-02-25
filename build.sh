#!/bin/bash

rm -rf build/*
rm -rf consumer/build/*
rm -rf producer/build/*
rm -rf streams/build/*

./gradlew build

docker build -t com.opi.kafka/consumer:latest consumer/
docker build -t com.opi.kafka/streams:latest streams/
docker build -t com.opi.kafka/producer:latest producer/