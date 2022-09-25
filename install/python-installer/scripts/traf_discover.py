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

### this script should be run on all nodes with sudo user ###

import json
import sys
import base64
from Discover import Discover

PREFIX = 'get_'

def run(dbcfgs):
    discover = Discover(dbcfgs)
    all_methods = [m for m in dir(discover) if m.startswith(PREFIX)]
    # do minumum discover in installation mode
    install_methods = ['default_java', 'linux_distro', 'firewall_status', 'traf_status', 'license_status', 'aws_ec2',
                       'mount_disks', 'ssh_pam', 'kernel_ver', 'hbase_ver', 'home_dir', 'hadoop_auth', 'hadoop_group_mapping']
    install_methods = [PREFIX + m for m in install_methods]
    allinstall_methods = ['linux_distro', 'firewall_status', 'traf_status', 'license_status', 'aws_ec2',
                          'mount_disks', 'ssh_pam', 'kernel_ver', 'home_dir', 'hadoop_auth', 'hadoop_group_mapping']
    all_install_methods = [PREFIX + m for m in allinstall_methods]
    preinstall_methods = ['hostname', 'fqdn', 'linux_distro', 'firewall_status', 'selinux_status', 'ntp_status',
                          'systime', 'timezone', 'transparent_hugepages']
    preinstall_methods = [PREFIX + m for m in preinstall_methods]
    if discover_mode == 'install':
        methods = install_methods
    elif discover_mode == 'preinstall':
        methods = preinstall_methods
    elif discover_mode == 'allinstall':
        methods = all_install_methods
    else:
        methods = all_methods

    result = {}
    for method in methods:
        key, value = getattr(discover, method)() # call method
        result[key] = value

    print json.dumps(result)

# main
if __name__ == '__main__':
    try:
        dbcfgs_json = sys.argv[1]
        discover_mode = sys.argv[2]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        print 'No db config found'
        exit(1)
    run(dbcfgs)
