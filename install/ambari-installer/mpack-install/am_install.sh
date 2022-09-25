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

instloc="$1"
mpack="$2"
vers="$3"
pkgcnt="$4" #RPM package count


# tar up the mpack
tball="${instloc}/${mpack}-${vers}.tar.gz"

cd "${instloc}"
tar czf "$tball" ${mpack}

# install ambari mpack
if (( pkgcnt > 1 ))
then
  cmd="upgrade-mpack"
else
  cmd="install-mpack"
fi
ambari-server $cmd --verbose --mpack="$tball"
ret=$?

exit $ret
