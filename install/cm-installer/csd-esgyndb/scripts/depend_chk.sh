#!/bin/bash

MT=$1
KA="$2"

pkgs="apr apr-util sqlite expect perl-DBD-SQLite protobuf xerces-c"
pkgs+=" perl-Params-Validate perl-Time-HiRes gzip lzo lzop unzip gcc-c++"
pkgs+=" unixODBC unixODBC-devel openldap-clients snappy lsof gnuplot"

if [[ $MT == 1 ]]
then
  pkgs+=" libcgroup"
fi

if [[ "$KA" == "true" ]]
then
  pkgs+=" keepalived"
fi

if [[ $(uname -r) =~ el7 ]]
then
  pkgs+=" net-tools"
  if [[ $MT == 1 ]]
  then
    pkgs+=" libcgroup-tools"
  fi
  if grep -q ^ec2 /sys/hypervisor/uuid 2>/dev/null
  then
    pkgs+=" awscli"
  fi
fi

errcnt=0

for package in $pkgs
do
  found=$(rpm -qa $package 2>/dev/null | wc -l)
  if [[ $found == 0 ]]
  then
    echo "Error: Package dependency not installed: $package"
    (( errcnt+=1 ))
  fi
done

echo "Dependency check: $errcnt packages missing"

##### Java version
jv=$($JAVA_HOME/bin/java -version 2>&1 | sed -n '/ version / s/^.*"\(.*\)"/\1/p')

if [[ ! $jv =~ ^1.8 ]]
then
  echo "Error: Java 1.8 required."
  echo "    $JAVA_HOME is version $jv"
  (( errcnt+=1 ))
else
  echo "Java version check okay"
fi

exit $errcnt
