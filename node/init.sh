#! /bin/env bash

init() {
    local -i res=1
    while [ $res -ne 0 ]; do 
        sleep 10
        cqlsh -f /etc/cassandra/ddl.cql
        res=${?}
    done
}

init &
/usr/local/bin/docker-entrypoint.sh "${@}"