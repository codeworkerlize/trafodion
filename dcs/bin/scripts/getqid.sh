#!/bin/bash
#/**
#* @@@ START COPYRIGHT @@@
#
#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing,
#software distributed under the License is distributed on an
#"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#KIND, either express or implied.  See the License for the
#specific language governing permissions and limitations
#under the License.
#
#* @@@ END COPYRIGHT @@@
# */
#

if [ ! -n "$2" ] ;then
    echo "ERROR: PROCESS_STATS and CPUNode is MUST"
    exit 1;
fi

result=$(sqlci -q "select cast(substr(variable_info, position('recentQid:' in variable_info) + 11) as varchar(175) CHARACTER SET UTF8) QUERY_ID  from table (statistics(NULL, 'PROCESS_STATS=$1,CPU=$2'))")
echo $result
if [[ $result =~ "ERROR" ]];then
    exit 1;
else
    exit 0;
fi
