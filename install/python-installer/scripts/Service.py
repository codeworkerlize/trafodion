#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import json
import math
import socket
import logging
import time
from constants import *
from common import err, run_cmd, cmd_output, mod_file, run_cmd3, append_file, run_cmd2, get_host_ip, get_sudo_prefix

logger = logging.getLogger()


class Service(object):
    def __init__(self, dbcfgs, os_major_ver):
        self.node_list = dbcfgs["node_list"]
        self.os_major_ver = os_major_ver
        self.host = get_host_ip(socket)

    def gen_hosts(self, hosts_content):
        logger.info('Modify the /etc/hosts file')
        with open(HOST_FILE, 'r') as f:
            lines = f.readlines()
        exist_host = [h for h in set(lines).difference(set(hosts_content.split(','))) if not h.startswith(('#', '127.0.0.1', '::1'))]
        with open(HOST_FILE, 'w') as f:
            f.write('#Created by Pre-Setup\n')
            f.write('127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4\n')
            f.write('::1         localhost localhost.localdomain localhost6 localhost6.localdomain6\n\n')
            f.writelines(hosts_content.split(','))
            f.writelines(exist_host)

    def modify_hostname(self, ip_hostname):
        logger.info('Modify hostname')
        ip = self.host
        if self.os_major_ver in REDHAT6_MAJOR_VER:
            mod_file('/etc/sysconfig/network', {'HOSTNAME=*': 'HOSTNAME=%s' % ip_hostname[ip]})
        else:
            with open('/etc/hostname', 'w') as f:
                f.write('%s' % ip_hostname[ip])
        run_cmd('hostname %s' % ip_hostname[ip])

    def sysctl_configs(self):
        logger.info('Setting up sysctl configs')
        with open(SYSCTL_CONF_FILE, 'r') as f:
            lines = f.read()
        if 'vm.swappiness=1' not in lines:
            append_file(SYSCTL_CONF_FILE, 'vm.swappiness=1')
        cmd_output('/sbin/sysctl -e -p /etc/sysctl.conf 2>&1 > /dev/null')

        limit = '''* soft nofile 65536
* hard nofile 65536
* soft nproc 131072
* hard nproc 131072
'''
        append_file('/etc/security/limits.conf', limit)

    def iptables_stop(self):
        logger.info('Stopping iptables service')
        if self.os_major_ver in REDHAT6_MAJOR_VER:
            run_cmd('service iptables stop')
        else:
            run_cmd('systemctl stop firewalld')
        iptables_stat = cmd_output('iptables -nL|grep -vE "(Chain|target)"')
        if iptables_stat:
            run_cmd('iptables -F')

    @staticmethod
    def disable_selinux():
        logger.info('Disable selinux')
        # disable selinux temporarily
        cmd_output('setenforce 0')
        # disable selinux permanently
        mod_file(SELINUX_FILE, {'SELINUX=\w+': 'SELINUX=disabled'})

    @staticmethod
    def disable_hugpages():
        disabl_hugepages_cmd = '''
if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi
if test -f /sys/kernel/mm/transparent_hugepage/defrag; then
    echo never > /sys/kernel/mm/transparent_hugepage/defrag
fi
'''
        logger.info('Disable transparent hugepages')
        output = cmd_output('cat /etc/rc.d/rc.local | grep -Pzo "if test -f /sys/kernel/mm/transparent_hugepage/enabled; then\n\s*echo never > /sys/kernel/mm/transparent_hugepage/enabled\nfi\nif test -f /sys/kernel/mm/transparent_hugepage/defrag; then\n\s*echo never > /sys/kernel/mm/transparent_hugepage/defrag\nfi\n"')
        if not output:
            run_cmd('echo \'%s\' >> /etc/rc.d/rc.local' % disabl_hugepages_cmd)
        run_cmd('chmod +x /etc/rc.d/rc.local')

        run_cmd('echo never > /sys/kernel/mm/transparent_hugepage/enabled')
        run_cmd('echo never > /sys/kernel/mm/transparent_hugepage/defrag')

    @staticmethod
    def set_umask():
        logger.info('Setting up umask')
        umask_cmd = """
if [[ -n `grep -E "^umask 0077" /etc/bashrc` ]]; then
    %s sed -i "s/umask 0077$/umask 0022/g" /etc/bashrc;
elif [[ -z `grep -E "^umask 0022" /etc/bashrc` ]]; then
    %s sed -i '$aumask 0022' /etc/bashrc;
fi;"""
        run_cmd(umask_cmd % (get_sudo_prefix(), get_sudo_prefix()))


class Ntp(object):
    def __init__(self, dbcfgs, time_zone, os_name):
        self.time_zone = time_zone
        self.os_name = os_name
        self.nodes = dbcfgs["node_list"].split(',')
        self.ntp_server = dbcfgs["ntp_server"]
        self.gateway = dbcfgs["ntp_gateway"]
        self.mask = dbcfgs["ntp_mask"]
        self.host = get_host_ip(socket)

    @staticmethod
    def _check_package(pkg_name):
        if not cmd_output('rpm -qa | grep %s-' % pkg_name):
            logger.info('Installing %s' % pkg_name)
            rc, stderr = run_cmd3('yum install -y %s' % pkg_name)
            if rc:
                err('Failed to install %s: %s' % (pkg_name, stderr))
        else:
            logger.info('Package %s had already been installed' % pkg_name)

    def _set_timezone(self):
        if self.time_zone != 'CST':
            run_cmd('timedatectl set-timezone "Asia/Shanghai"')

    def _mod_ntp_conf(self):
        if self.nodes[0] == self.host:
            logger.info('Configuring the ntp service')
            config = 'restrict %s mask %s nomodify' % (self.gateway, self.mask)
            append_file(NTP_CONF_FILE, config, 'restrict 127.0.0.1')
            mod_file(NTP_CONF_FILE,
                     {'\nserver 0.%s.*' % self.os_name: '\n#server 0.%s.pool.ntp.org iburst' % self.os_name,
                      '\nserver 1.%s.*' % self.os_name: '\n#server 1.%s.pool.ntp.org iburst' % self.os_name,
                      '\nserver 2.%s.*' % self.os_name: '\n#server 2.%s.pool.ntp.org iburst' % self.os_name,
                      '\nserver 3.%s.*' % self.os_name: '\n#server 3.%s.pool.ntp.org iburst' % self.os_name})
            append_file(NTP_CONF_FILE, 'server %s iburst' % self.ntp_server, '#server 3.%s.pool.ntp.org iburst' % self.os_name)

            if self.ntp_server != "127.127.1.0":
                logger.info('Synchronizing time with ntp server')
                run_cmd('ntpdate -u %s' % self.ntp_server)  # execute ntpdate twice to ensure time precision
                run_cmd('ntpdate -u %s' % self.ntp_server)
        else:
            logger.info('Configuring the ntp service')
            config = '''server %s iburst''' % (self.nodes[0])
            mod_file(NTP_CONF_FILE,
                     {'\nserver 0.%s.*' % self.os_name: '\n#server 0.%s.pool.ntp.org iburst' % self.os_name,
                      '\nserver 1.%s.*' % self.os_name: '\n#server 1.%s.pool.ntp.org iburst' % self.os_name,
                      '\nserver 2.%s.*' % self.os_name: '\n#server 2.%s.pool.ntp.org iburst' % self.os_name,
                      '\nserver 3.%s.*' % self.os_name: '\n#server 3.%s.pool.ntp.org iburst' % self.os_name})
            append_file(NTP_CONF_FILE, config, '#server 3.%s.pool.ntp.org iburst' % self.os_name)

            logger.info('Synchronizing time with local server')
            while run_cmd2('ntpdate -u %s' % self.nodes[0]):
                time.sleep(5)
                run_cmd2('ntpdate -u %s' % self.nodes[0])


class RedHat6NTP(Ntp):
    def __init__(self, dbcfgs, time_zone, os_name):
        super(RedHat6NTP, self).__init__(dbcfgs, time_zone, os_name)

    def _set_timezone(self):
        if self.time_zone != 'CST':
            run_cmd('ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime')

    def setup(self):
        self._check_package('ntp')
        self._set_timezone()

        if 'running' in cmd_output('service ntpd status'):
            run_cmd('service ntpd stop')

        self._mod_ntp_conf()
        run_cmd('hwclock -w')  # Synchronize hardware time

        logger.info('Starting ntp service')
        run_cmd('chkconfig ntpd on')
        run_cmd('service ntpd start')


class RedHat7NTP(Ntp):
    def __init__(self, dbcfgs, time_zone, os_name):
        super(RedHat7NTP, self).__init__(dbcfgs, time_zone, os_name)

    def setup(self):
        self._check_package('ntp')
        self._set_timezone()

        if 'running' in cmd_output('systemctl status ntpd'):
            run_cmd('systemctl stop ntpd')

        self._mod_ntp_conf()
        run_cmd('hwclock -w')

        logger.info('Starting ntp service')
        run_cmd('systemctl enable ntpd')
        run_cmd('systemctl start ntpd')


class RedHat7Chrony(Ntp):
    def __init__(self, dbcfgs, time_zone, os_name):
        super(RedHat7Chrony, self).__init__(dbcfgs, time_zone, os_name)

    @property
    def _calc_mask_length(self):
        mask = self.mask.split('.')
        length = 0
        for i in range(4):
            mask[i] = int(mask[i])
            if (255 & mask[i]) == 255:
                length += 8
            else:
                length += (8 - math.log(256-mask[i], 2))
        return length

    def _mod_ntp_conf(self):
        logger.info('Configuring the chrony service')
        local_server = self.nodes[0]
        if self.nodes[0] == self.host:
            allow_config = 'allow %s/%d' % (self.gateway, self._calc_mask_length)
            append_file(CHRONY_CONF_FILE, allow_config, '#allow 192.168.0.0/16')
            append_file(CHRONY_CONF_FILE, 'allow 127.0.0.0/8', '#allow 192.168.0.0/16')
            mod_file(CHRONY_CONF_FILE, {'#local stratum 10': 'local stratum 10'})

        if self.ntp_server == '127.127.1.0':
            server_info = 'server %s iburst' % local_server
        else:
            server_info = 'server %s iburst prefer trust\nserver %s iburst' % (self.ntp_server, local_server)

        mod_file(CHRONY_CONF_FILE,
                 {'\nserver 0.%s.*' % self.os_name: '\n#server 0.%s.pool.ntp.org iburst' % self.os_name,
                  '\nserver 1.%s.*' % self.os_name: '\n#server 1.%s.pool.ntp.org iburst' % self.os_name,
                  '\nserver 2.%s.*' % self.os_name: '\n#server 2.%s.pool.ntp.org iburst' % self.os_name,
                  '\nserver 3.%s.*' % self.os_name: '\n#server 3.%s.pool.ntp.org iburst' % self.os_name})
        for s in server_info.split('\n'):
            append_file(CHRONY_CONF_FILE, s, '#server 3.%s.pool.ntp.org iburst' % self.os_name)

        logger.info('Synchronizing time with ntp server')
        run_cmd('chronyc -a makestep')

    def setup(self):
        self._check_package('chrony')
        self._set_timezone()

        logger.info('Starting chrony service')
        run_cmd('systemctl enable chronyd')
        run_cmd('systemctl start chronyd')

        self._mod_ntp_conf()
        run_cmd('systemctl restart chronyd')
        run_cmd('hwclock -w')


class KylinChrony(RedHat7Chrony):
    def __init__(self, dbcfgs, time_zone, os_name):
        super(KylinChrony, self).__init__(dbcfgs, time_zone, os_name)

    def _mod_ntp_conf(self):
        logger.info('Configuring the chrony service')
        local_server = self.nodes[0]
        if self.nodes[0] == self.host:
            allow_config = 'allow %s/%d' % (self.gateway, self._calc_mask_length)
            append_file(CHRONY_CONF_FILE, allow_config, '#allow 192.168.0.0/16')
            append_file(CHRONY_CONF_FILE, 'allow 127.0.0.0/8', '#allow 192.168.0.0/16')
            mod_file(CHRONY_CONF_FILE, {'#local stratum 10': 'local stratum 10'})

        if self.ntp_server == '127.127.1.0':
            server_info = 'server %s iburst' % local_server
        else:
            server_info = 'server %s iburst prefer trust\nserver %s iburst' % (self.ntp_server, local_server)

        mod_file(CHRONY_CONF_FILE, {'pool pool.ntp.org iburst': '#pool pool.ntp.org iburst'})
        for s in server_info.split('\n'):
            append_file(CHRONY_CONF_FILE, s, '#pool pool.ntp.org iburst')

        logger.info('Synchronizing time with ntp server')
        run_cmd('chronyc -a makestep')


if __name__ == '__main__':
    exit(0)

