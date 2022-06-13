#! /bin/bash

kafka-console-consumer.sh   --bootstrap-server localhost:9092 \
                            --topic tweets                    \
                            --from-beginning                  \
                            --timeout-ms 5000