#!/bin/bash

export ESGYN_PARCEL="$PARCELS_ROOT/$PARCEL_DIRNAME"
export ESGYN_PARCEL_VER="_TRAF_VERSION_"
#To force a certain minimum patch version of CSD is being used, set ESGYN_CSD_MINIMUM_VER
#  (major and minor number must match regardless of this setting)
#export ESGYN_CSD_MINIMUM_VER="x.y.z"

# Default home, in case parcel is activated, but config files not yet updated
export TRAF_HOME="$PARCELS_ROOT/$PARCEL_DIRNAME/traf_home"

# Set HBase Classpath extension
hadoopver=$CDH_DIRNAME
if [[ $hadoopver =~ cdh5.4 ]]
then
  CDHVER=5_4
elif [[ $hadoopver =~ cdh5.5|cdh5.6 ]]
then
  CDHVER=5_5
else
  CDHVER=5_7
fi

if [[ -n $CDHVER ]]
then
  ESGYN_TRX=$(ls $TRAF_HOME/export/lib/trafodion-utility-*.jar)
  ESGYN_TRX+=:$(ls $TRAF_HOME/export/lib/hbase-trx-cdh${CDHVER}-*.jar)

  if [[ -n "${HBASE_CLASSPATH}" ]]; then
    export HBASE_CLASSPATH="${HBASE_CLASSPATH}:$ESGYN_TRX"
  else
    export HBASE_CLASSPATH="$ESGYN_TRX"
  fi
fi

