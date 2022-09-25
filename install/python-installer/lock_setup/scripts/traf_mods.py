#!/usr/bin/env python

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

### this script should be run on first node with trafodion user ###

import re
import os
import sys
import base64
import json
from gen_lock_config import generateCDHConfig
from common import err, warn, cmd_output_as_user, mod_file, append_file, write_file

def run(dbcfgs):
    # mod ms.env.custom
    traf_var = cmd_output_as_user(dbcfgs['traf_user'], 'echo $TRAF_CONF')
    if len(traf_var) == 0:
        err("Qianbase server is not initialized")
    ms_file = traf_var + "/ms.env.custom"
    if not os.path.exists(ms_file):
        warn(ms_file + " is not exists")
        cmd_output_as_user(dbcfgs['traf_user'], "touch " + ms_file)
    lock_switch = 0
    if dbcfgs["use_lock"].upper() == 'Y':
        lock_switch = 1
    else:
        lock_switch = 0
    new_config_line = "ENABLE_ROW_LEVEL_LOCK=%d\n" % lock_switch
    #don't use append_file,will be lost "\n" in head of string
    lines = ""
    with open(ms_file, 'r') as f:
        for line in f:
            if re.search(r"^ENABLE_ROW_LEVEL_LOCK\s*=.*", line) == None:
                lines += line
        if lines[-1] != "\n":
            #new line
            lines += "\n"
        lines += new_config_line
    write_file(ms_file, lines)

# main
if __name__ == '__main__':
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
