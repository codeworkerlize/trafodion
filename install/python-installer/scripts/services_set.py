#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import sys
import json
import base64
import socket
import platform
import logging
from collections import defaultdict
from Service import Service, RedHat6NTP, RedHat7Chrony, KylinChrony
from constants import *
from common import err, set_stream_logger, run_cmd, cmd_output, mod_file, run_cmd3, append_file

logger = logging.getLogger()

def run(dbcfgs):
    os_name = platform.dist()[0]
    os_major_ver = platform.dist()[1].split('.')[0]
    time_zone = cmd_output('date +"%Z"')
    hosts_content = dbcfgs["hosts"]
    enable_ntp = dbcfgs["enable_ntp"]
    hostname = socket.getfqdn()

    service = Service(dbcfgs, os_major_ver)
    service.set_umask()

    # modify /etc/hosts
    service.gen_hosts(hosts_content)

    # modify /etc/hostname
    if hostname not in hosts_content:
        ip_hostname = dict([[h.split()[i] for i in range(2)] for h in hosts_content.split(',')])
        service.modify_hostname(ip_hostname)

    # modify sysctl configs
    service.sysctl_configs()

    # disable selinux
    service.disable_selinux()

    # stop iptables
    service.iptables_stop()

    #disable hugpages
    service.disable_hugpages()

    # start ntpd
    if enable_ntp.upper() == 'Y':
        if os_name.upper() == OS_CENTOS:
            if CENTOS6_MAJOR_VER in os_major_ver:
                ntpd = RedHat6NTP(dbcfgs, time_zone, os_name)
            elif CENTOS7_MAJOR_VER in os_major_ver:
                ntpd = RedHat7Chrony(dbcfgs, time_zone, os_name)
        elif os_name.upper() == OS_REDHAT:
            os_name = 'rhel'
            if REDHAT6_MAJOR_VER in os_major_ver:
                ntpd = RedHat6NTP(dbcfgs, time_zone, os_name)
            elif REDHAT7_MAJOR_VER in os_major_ver:
                ntpd = RedHat7Chrony(dbcfgs, time_zone, os_name)
        elif os_name.upper() == OS_KYLIN:
            ntpd = KylinChrony(dbcfgs, time_zone, os_name)

        ntpd.setup()



if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
