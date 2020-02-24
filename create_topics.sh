#!/bin/bash

# create the person topics
../kafka/scripts/create_topic.sh account 10
../kafka/scripts/create_topic.sh account-transform 10
../kafka/scripts/create_topic.sh person 10
../kafka/scripts/create_topic.sh address 10
../kafka/scripts/create_topic.sh phone 10
../kafka/scripts/create_topic.sh phone-encrypted 10

# create the thing topics
../kafka/scripts/create_topic.sh thing-0 10
../kafka/scripts/create_topic.sh thing-0-encrypted 10
../kafka/scripts/create_topic.sh thing-1 10
../kafka/scripts/create_topic.sh thing-1-encrypted 10
../kafka/scripts/create_topic.sh thing-2 10
../kafka/scripts/create_topic.sh thing-2-encrypted 10
../kafka/scripts/create_topic.sh thing-3 10
../kafka/scripts/create_topic.sh thing-3-encrypted 10
../kafka/scripts/create_topic.sh thing-4 10
../kafka/scripts/create_topic.sh thing-4-encrypted 10