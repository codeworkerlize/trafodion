#!/bin/bash

if [[ -z $ESGYNDB_VERSION ]]; then
    ESGYNDB_VERSION=2.7
fi

valid=0
for VALID_VERSION in 2.7 2.8 2.9; do
    if [[ $ESGYNDB_VERSION == $VALID_VERSION ]]; then
        valid=1
    fi
done

if [[ $valid -eq 0 ]]; then
    echo "Invalid \$ESGYNDB_VERSION provided"
    exit 1
fi

if [[ -z $ESGYNDB_DOCKER_VERSION ]]; then
    ESGYNDB_DOCKER_VERSION=esgyndb:$ESGYNDB_VERSION
fi

ESGYN_DOWNLOAD_DAILY=http://downloads.esgyn.local/esgyn/AdvEnt$ESGYNDB_VERSION/publish/daily/

if [[ -z $ESGYNDB_DOWNLOAD_LINK ]]; then
    # get the latest daily build
    RELEASE=`curl $ESGYN_DOWNLOAD_DAILY |grep 'rh7-release'|tail -n 1 |sed -re 's#.*href="(.*)/".*#\1#'`
    ESGYNDB_DOWNLOAD_LINK=${ESGYN_DOWNLOAD_DAILY}${RELEASE}/esgynDB_server-${ESGYNDB_VERSION}.0-RH7-x86_64.tar.gz
fi

docker build --build-arg ESGYNDB_DOWNLOAD_LINK=$ESGYNDB_DOWNLOAD_LINK --build-arg ESGYNDB_VERSION=${ESGYNDB_VERSION}.0 --rm=true -t $ESGYNDB_DOCKER_VERSION .
