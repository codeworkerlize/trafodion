#!/bin/bash

# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@


# This is the .bashrc for the Trafodion environment
#
#-------------------------------------------
# Execute the system's default .bashrc first
#-------------------------------------------
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

#-------------------------------------------
# Execute the trafodion_config script
#-------------------------------------------

if [ -f /etc/trafodion/trafodion_config ]; then
   source /etc/trafodion/trafodion_config
fi

#-------------------------------------------
# Execute the sqenv.sh script if it exists.
#-------------------------------------------
PATH=".:$PATH"

if [ -f $TRAF_HOME/sqenv.sh ]; then
        pushd . >/dev/null 2>&1
        cd $TRAF_HOME
        source ./sqenv.sh
        popd >/dev/null 2>&1
        export MANPATH=$MANPATH:$MPI_ROOT/share/man
fi

#-------------------------------------------
# additional settings for Trafodion environment
#-------------------------------------------
ETC_SECURITY_MSG="***ERROR: To fix this please configure /etc/security/limits.conf properly on $HOSTNAME."

# set core file size
ulimit -c unlimited

# set max open files
ulimit -n 32768
if [ $? -ne 0 ]; then
    echo "***ERROR: Unable to set max open files. Current value $(ulimit -n)"
    echo $ETC_SECURITY_MSG
fi

# Do not modify this file, it may be over-written without notice.
# For local customization to trafodion shell environment, add a file:
#    ~trafodion/.bashrc.local
# Be sure commands in local file do not write anything to stdout
if [[ -r ~trafodion/.bashrc.local ]]
then
  source ~trafodion/.bashrc.local
fi

export PDSH_RCMD_TYPE=ssh

if command -v rlwrap &>/dev/null;then
    alias sqlci='rlwrap sqlci'
    alias trafci='rlwrap trafci'
fi
