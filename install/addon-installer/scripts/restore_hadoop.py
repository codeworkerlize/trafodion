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

import sys
import json
import base64
import logging
from hadoop_mods import CDHMod
from constants import DEF_MODCFG_FILE
from common import ParseXML, ParseJson, err, set_stream_logger

logger = logging.getLogger()


def restore(xmlstr1, xmlstr2):
    return xmlstr2


def run(dbcfgs):
    def_mod_cfgs = ParseJson(DEF_MODCFG_FILE).load()
    if def_mod_cfgs['FLAG'] == 1:
        hadoop_mod = CDHMod(dbcfgs['mgr_user'], dbcfgs['mgr_pwd'], dbcfgs['mgr_url'], dbcfgs['cluster_name'])
        hadoop_mod.mod(restore, def_mod_cfgs)

        def_mod_cfgs['FLAG'] = 0
        ParseJson(DEF_MODCFG_FILE).save(def_mod_cfgs)


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
