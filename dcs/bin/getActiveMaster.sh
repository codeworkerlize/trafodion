#!/usr/bin/env bash
#/**
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
# */

# Get the activeMaster hostname
if [[ -f "${TRAF_HOME}/sql/scripts/swzkcli" ]]; then
   zkcli="swzkcli"
else
   zkcli="hbase zkcli"
fi

DCS_MASTER_ZNODE=/trafodion/dcs/master
if [[ -n "$TRAF_ROOT_ZNODE" ]];then
   DCS_MASTER_ZNODE=$TRAF_ROOT_ZNODE/$TRAF_INSTANCE_ID/dcs/master
fi

master=$($zkcli ls $DCS_MASTER_ZNODE 2>/dev/null | grep ^\\[)
if [[ -n "$master" && "$master" != "[]" ]]; then
   echo "$master" | cut -d '[' -f 2 | cut -d ':' -f 1
fi
