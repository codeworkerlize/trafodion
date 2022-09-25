#!/bin/bash

hadoopver=$CDH_DIRNAME
if [[ $hadoopver =~ cdh5.4 ]]
then
  CDHVER=5.4
elif [[ $hadoopver =~ cdh5.5|cdh5.6 ]]
then
  CDHVER=5.5
else
  CDHVER=5.7
fi

if [[ -n $CDHVER ]]
then
  ESGYN_TRX=$(ls $PARCELS_ROOT/$PARCEL_DIRNAME/lib/*.jar)
  ESGYN_TRX+=:$(ls $PARCELS_ROOT/$PARCEL_DIRNAME/lib/$CDHVER/*.jar)

  if [[ -n "${HBASE_CLASSPATH}" ]]; then
    export HBASE_CLASSPATH="${HBASE_CLASSPATH}:$ESGYN_TRX"
  else
    export HBASE_CLASSPATH="$ESGYN_TRX"
  fi
fi

