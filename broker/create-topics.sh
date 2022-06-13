#! /bin/bash

add_topic() {
    kafka-topics.sh --bootstrap-server localhost:9092   \
                    --create                            \
                    --topic wiki                        \
                    --partitions 3                      \
                    --replication-factor 1
    echo CREATED_TOPIC::test-topic
}

add_topic&