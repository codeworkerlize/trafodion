#!/bin/bash

if [[ -z $ESGYNDB_VERSION ]]; then
    ESGYNDB_VERSION=2.7
fi

if [[ -z $ESGYNDB_DOCKER_VERSION ]]; then
    ESGYNDB_DOCKER_VERSION=esgyndb:$ESGYNDB_VERSION
fi

if [[ ! -z $1 ]]; then
    docker run -d -P --privileged=true -h localhost -e HOST_IP=$1 $ESGYNDB_DOCKER_VERSION
else
    docker run -d -P --privileged=true -h localhost $ESGYNDB_DOCKER_VERSION
fi
