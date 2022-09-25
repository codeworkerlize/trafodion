#!/bin/bash

build_rlwrap(){
    RLWRAP=rlwrap-0.45.2
    TARGET="$TRAF_HOME/export/lib$SQ_MBTYPE/rlwrap"
    test -f $TARGET && return 0
    cd `dirname $0` && tar zxvf $RLWRAP.tar.gz
    (cd $RLWRAP && ./configure && make -j`nproc` && cp -f ./src/rlwrap $TARGET)
    rm -rf $RLWRAP
}
build_rlwrap

