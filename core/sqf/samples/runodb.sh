#!/bin/bash
export ODBCHOME=$PWD
export ODBCSYSINI=${ODBCHOME}
export ODBCINI=${ODBCHOME}/odbc.ini
export AppUnicodeType=utf16

$TRAF_HOME/export/bin64/odb64luo -d TRAFDSN -u db__root -p ss -i

