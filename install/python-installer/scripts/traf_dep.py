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

""" this script should be run on all nodes with sudo user """

import os
import sys
import json
import base64
import logging
import platform
from constants import *
from collections import defaultdict
from common import run_cmd3, cmd_output, err, CentralConfig, License, set_stream_logger

logger = logging.getLogger()
EPEL_RELEASE = 'epel-release'

class Package(object):
    def __init__(self, node_count=1, offline=False, addnode_mode=False, ldap_enabled=False):
        self.node_count = node_count
        self.offline = offline
        self.addnode_mode = addnode_mode
        self.ldap_enabled = ldap_enabled
        self.req_pkg_list = self._list_required_packages()
        self.all_pkg_list = self._list_all_packages()
        self.offline_package_cmd = self._get_offline_package_cmd()
        self.package_cmd = self._get_package_cmd()

    @staticmethod
    def _is_mt_enabled():
        try:
            lic = License(EDB_LIC_FILE)
            return lic.is_multi_tenancy_enabled()
        except StandardError as e:
            err(str(e))

    def _get_package_cmd(self):
        return ''

    def _get_offline_package_cmd(self):
        return ''

    def _list_all_packages(self):
        return []

    def _list_required_packages(self):
        return []

    def install(self):
        install_pkg_list = []

        # pdsh should not exist on single node
        if self.node_count == 1 and not self.addnode_mode:
            try:
                self.req_pkg_list.remove('pdsh')
            except ValueError:
                pass

        for pkg in self.req_pkg_list:
            if pkg + '-' in self.all_pkg_list:
                logger.info('Package %s had already been installed' % pkg)
            else:
                install_pkg_list.append(pkg)

        # no need to install epel if pdsh and protobuf is installed
        if self.distro != 'ubuntu':
            PDSH_PKG_PREFIX = 'pdsh-rcmd-ssh-'
            PROTOBUF_PKG_PREFIX = 'protobuf-'
            if PDSH_PKG_PREFIX in self.all_pkg_list and PROTOBUF_PKG_PREFIX in self.all_pkg_list:
                try:
                    logger.info('Removed epel-release from installing package list')
                    install_pkg_list.remove(EPEL_RELEASE)
                except ValueError:
                    pass

        for pkg in install_pkg_list:
            logger.info('Installing %s ...' % pkg)
            if self.offline:
                rc, err_info = run_cmd3(self.offline_package_cmd + pkg)
            else:
                rc, err_info = run_cmd3(self.package_cmd + pkg)

            if rc:
                err('Failed to install Trafodion dependencies: \'%s\'.\n%s'
                    'All remaining required packages are: [%s] Please install them manually on all nodes and re-run the installer.'
                    % (pkg, err_info, ','.join(install_pkg_list)))

class Centos6Package(Package):
    def __init__(self, node_count, offline, addnode_mode, ldap_enabled):
        super(Centos6Package, self).__init__(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        self.distro = 'centos6'

    def _get_package_cmd(self):
        return 'yum install -y '

    def _get_offline_package_cmd(self):
        return 'yum install -y --disablerepo=\* --enablerepo=%s ' % REPO_NAME

    def _list_all_packages(self):
        return cmd_output('rpm -qa')

    def _list_required_centos_packages(self):
        package_list = CentralConfig().get('packages_centos')
        if not self.offline and not os.path.exists(EPEL_FILE):
            package_list = [EPEL_RELEASE] + package_list

        if self.ldap_enabled:
            package_list += ['openldap-clients']
        return package_list

    def _list_required_packages(self):
        package_list = self._list_required_centos_packages()
        if self._is_mt_enabled():
            package_list += ['libcgroup']
        return package_list

class Centos7Package(Centos6Package):
    def __init__(self, node_count, offline, addnode_mode, ldap_enabled):
        super(Centos6Package, self).__init__(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        self.distro = 'centos7'

    def _list_required_packages(self):
        package_list = self._list_required_centos_packages()
        package_list += ['net-tools']
        if self._is_mt_enabled():
            package_list += ['libcgroup-tools']
        return package_list

class Kylin10Package(Centos6Package):
    def __init__(self, node_count, offline, addnode_mode, ldap_enabled):
        super(Kylin10Package, self).__init__(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        self.distro = 'kylin10'

    def _list_required_packages(self):
        package_list = CentralConfig().get('packages_centos')
        package_list += ['net-tools', 'java-1.8.0-openjdk-devel', 'redhat-lsb-core', 'redhat-lsb']
        package_list.remove('pdsh')
        package_list.remove('protobuf')
        package_list.remove('perl-Params-Validate')
        if self.ldap_enabled:
            package_list += ['openldap-clients']
        if self._is_mt_enabled():
            package_list += ['libcgroup-tools']
        return package_list

class UbuntuPackage(Package):
    def __init__(self, node_count, addnode_mode, ldap_enabled):
        # not support offline mode for Ubuntu now
        super(UbuntuPackage, self).__init__(node_count, offline=False, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        self.distro = 'ubuntu'

    def _get_package_cmd(self):
        return 'apt-get install -y '

    def _list_all_packages(self):
        return cmd_output("dpkg -l | awk '{print $2\"-\"$3}' | sed s/:amd64//g")

    def _list_required_packages(self):
        package_list = CentralConfig().get('packages_ubuntu')
        if self.ldap_enabled:
            package_list += ['ldap-utils']
        if self._is_mt_enabled():
            package_list += ['cgroup-tools']
        return package_list

class SusePackage(Package):
    def _get_package_cmd(self):
        return 'zypper install -y '

    # Cannot install SuSE dependencies automatically
    def install(self):
        logger.info('SuSE OS is detected, skip installing dependencies, you MUST install them manually')

def run(dbcfgs):
    """ install Trafodion dependencies """

    node_count = len(dbcfgs['node_list'].split(','))
    # key traf_shadow means we are running in adding node mode
    addnode_mode = True if 'traf_shadow' in dbcfgs else False
    ldap_enabled = True if dbcfgs['ldap_security'].upper() == 'Y' else False
    offline = True if dbcfgs['offline_mode'] == 'Y' else False
    repo_url = True if dbcfgs['repo_url'] else False

    if offline:
        logger.info('Offline mode is enabled, setting up local repo config [%s]' % REPO_FILE)
        baseurl = dbcfgs['repo_url'] if repo_url else 'http://%s:%s' % (dbcfgs['repo_ip'], dbcfgs['repo_http_port'])
        repo_content = LOCAL_REPO_PTR % baseurl
        with open(REPO_FILE, 'w') as f:
            f.write(repo_content)
    else:
        logger.info('Offline mode is disabled')

    package = Package()
    os_name = platform.dist()[0]
    os_major_ver = platform.dist()[1].split('.')[0]
    if os_name.upper() == OS_SUSE:
        package = SusePackage()
    elif os_name.upper() == OS_CENTOS:
        if CENTOS7_MAJOR_VER in os_major_ver:
            package = Centos7Package(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        elif CENTOS6_MAJOR_VER in os_major_ver:
            package = Centos6Package(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
    elif os_name.upper() == OS_REDHAT:
        if REDHAT7_MAJOR_VER in os_major_ver:
            package = Centos7Package(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        elif REDHAT6_MAJOR_VER in os_major_ver:
            package = Centos6Package(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
    elif os_name.upper() == OS_UBUNTU:
        package = UbuntuPackage(node_count, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
    elif os_name.upper() == OS_KYLIN:
        if KYLIN10_MAJOR_VER in os_major_ver:
            package = Kylin10Package(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)
        else:
            package = Centos7Package(node_count, offline=offline, addnode_mode=addnode_mode, ldap_enabled=ldap_enabled)

    package.install()

    if offline:
        os.remove(REPO_FILE)

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
