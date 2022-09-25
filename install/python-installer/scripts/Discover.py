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

import re
import os
import json
import sys
import time
import subprocess
import platform
from glob import glob
from constants import *
from common import run_cmd2, cmd_output, err, get_sudo_prefix, CentralConfig, ParseXML, License, ParseInI, run_cmd, run_cmd3


PREFIX = 'get_'
CCFG = CentralConfig()


def deco(func):
    def wrapper(self):
        if PREFIX in func.__name__:
            name = func.__name__.replace(PREFIX, '')
            value = func(self)
            return name, {'value': value, 'doc': func.__doc__}
        else:
            return

    return wrapper


def deco_consistent(func):
    def wrapper(self):
        if PREFIX in func.__name__:
            name = func.__name__.replace(PREFIX, '')
            value = func(self)
            return name, {'value': value, 'doc': func.__doc__, 'consistent': 'true'}
        else:
            return

    return wrapper


def deco_warn_err(func):
    """ append warning or error status based on the threshold settings in configs """

    def wrapper(self):
        if PREFIX in func.__name__:
            name = func.__name__.replace(PREFIX, '')
            value = func(self)

            items = CCFG.get(name)
            cmpr_operator = items['cmpr']
            warn = error = 0
            if items.has_key(WARN):
                warn = int(items[WARN])

            if items.has_key(ERR):
                error = int(items[ERR])

            if cmpr_operator == 'lt':
                if int(value) < warn:
                    if int(value) < error:
                        status = ERR
                    else:
                        status = WARN
                else:
                    status = OK
            elif cmpr_operator == 'gt':
                if int(value) > warn:
                    status = WARN
                else:
                    status = OK
            elif cmpr_operator == 'et':
                if int(value) != warn:
                    status = ERR
                else:
                    status = OK
            value = str(value)
            warn = str(warn)
            if items.has_key('unit'):
                value += items['unit']
                warn += items['unit']
            dic = {'value': value, 'status': status, 'doc': func.__doc__}
            if status != OK:
                if str(dic['doc']) is "umask_root" or str(dic['doc']) is "umask_other":
                    dic['expected'] = warn.rjust(3, '0')
                else:
                    dic['expected'] = warn
            return name, dic

        else:
            return

    return wrapper


def deco_val_valid(func):
    """ append status if values found in configs """

    def wrapper(self):
        if PREFIX in func.__name__:
            name = func.__name__.replace(PREFIX, '')
            value = func(self)
            avl_values = CCFG.get(name)  # list
            if value in avl_values:
                status = OK
            else:
                status = ERR
            return name, {'value': value, 'status': status, 'doc': func.__doc__}
        else:
            return

    return wrapper


def deco_val_chk(func):
    """ append status based on func return value/status """

    def wrapper(self):

        if PREFIX in func.__name__:
            name = func.__name__.replace(PREFIX, '')
            value, status = func(self)
            if value == []:
                value = NA
                status = OK
            return name, {'value': value, 'status': status, 'doc': func.__doc__}
        else:
            return

    return wrapper


class Discover(object):
    """ discover functions, to add a new discover function,
        simply add a new def with name get_xx and decorated
        by 'deco', then return result in string format:

        @deco
        def get_xx(self):
            # do something
            return result
    """
    umaskid = None

    def __init__(self, dbcfgs={}):
        self.CPUINFO = cmd_output('cat /proc/cpuinfo')
        self.MEMINFO = cmd_output('cat /proc/meminfo')
        self.SYSCTLINFO = cmd_output('/sbin/sysctl -a')
        if OS_UBUNTU in platform.dist()[0].upper():
            self.PKGINFO = cmd_output("dpkg -l | awk '{print $2\"-\"$3}' | sed s/:amd64//g").split('\n')
        else:
            self.PKGINFO = cmd_output('rpm -qa').split('\n')
        self.DFINFO = cmd_output('df -P').split('\n')
        self.dbcfgs = dbcfgs

    def _parse_string(self, info, string):
        try:
            info = info.split('\n')
            string_line = [line for line in info if string in line][0]
        except IndexError:
            err('Cannot get %s info' % string)
        return string_line

    def _get_cpu_info(self, string):
        return self._parse_string(self.CPUINFO, string).split(':')[1].strip()

    def _get_mem_info(self, string):
        return self._parse_string(self.MEMINFO, string).split(':')[1].split()[0]

    def _get_sysctl_info(self, string):
        return self._parse_string(self.SYSCTLINFO, string).split('=')[1].strip()

    @deco_val_chk
    def get_cpu_arch(self):
        """CPU architecture"""
        arch = platform.processor()
        if not arch:
            return UNKNOWN, ERR
        else:
            return arch, OK


    """The cpufreq directory exists in both the physical machine and virtual machine of arm, 
       but the cpufreq directory does not exist in the virtual machine of X86_64. 
       The reason is unknown, so it needs to be executed after judgment."""
    @deco_val_chk
    def get_cpu_freq(self):
        """CPU freq"""
        if os.path.exists(SCALING_GOVERNOR):
            cpufreq = cmd_output(
                'ls /sys/devices/system/cpu/|grep cpu[0-9]|xargs -I {} grep -lv performance /sys/devices/system/cpu/{}/cpufreq/scaling_governor')
            if cpufreq:
                return 'Powersave', WARN
            else:
                return 'Performance', OK
        else:
            return 'N/A', WARN

    @deco
    def get_cpu_MHz(self):
        """CPU MHz"""
        cpumhz = cmd_output("cat /proc/cpuinfo | grep -i mhz | awk -F: '{print$2}'").split('\n')
        min_cpumhz = min([float(mhz) for mhz in cpumhz])
        max_cpumhz = max([float(mhz) for mhz in cpumhz])
        if min_cpumhz == max_cpumhz:
            cpumhz_info = min_cpumhz
        else :
            cpumhz_info = [min_cpumhz, max_cpumhz]
        return cpumhz_info

    @deco
    def get_net_ty(self):
        """Network Card Type"""
        cardtype = cmd_output("lspci | grep Ethernet | awk -F: '{print $3}'")
        if cardtype is None:
            cardtype = 'N/A'
        return cardtype

    @deco
    def get_DRAM_clock(self):
        """DRAM Clock"""
        locklist = cmd_output("dmidecode|grep -A16 \"Memory Device\"|grep 'Speed'| awk -F: '{print$2}'")
        locklist_num = re.findall('\d+', locklist)
        if len(locklist_num)>0:
            DRAM_Clock = max([float(lock) for lock in locklist_num])
        else:
            DRAM_Clock = 'N/A'
        return DRAM_Clock

    disks_IOPS = None
    fio_flag = False
    # The random read and write test relies on the fio tool,
    # We need to detect whether fio exists
    # If failed to install fio, skip "IOPS_check,and return N/A ;
    # Continue to execute the script and skip "IOPS_check"
    def check_fio(self):
        fio_status = cmd_output('yum list installed | grep fio')
        if not fio_status:
            rc, stderr = run_cmd3('yum install -y fio')
            if not rc:
                self.fio_flag = True
        else:
            self.fio_flag = True

    @deco
    def get_Systemdisk_IOPS(self):
        """Systemdisk_IOPS"""
        dbcfg = ParseInI(DBCFG_FILE_DEF, 'dbconfigs').load()
        disk_path = dbcfg['Systemdisk_path']
        cmd_ckfio =  'fio -direct=1 -iodepth=64 -rw=randrw -ioengine=libaio -bs=1k -size=1G -numjobs=' \
                     '1 -runtime=10 -group_reporting -filename=%s -name=Rand_ReadWrite_IOPS_Test | ' \
                     'grep -E \'(read|write): IOPS\' |awk -F, \'{print$1}\'' % disk_path
        if os.path.exists(disk_path):
            if not self.disks_IOPS is None:
                if self.fio_flag:
                    self.disks_IOPS = cmd_output(cmd_ckfio)
                else:
                    self.disks_IOPS = "write:N/A\nread:N/A"
            else:
                self.check_fio()
                if self.fio_flag:
                    self.disks_IOPS = cmd_output(cmd_ckfio)
                else:
                    self.disks_IOPS = "write:N/A\nread:N/A"
        else :
            self.disks_IOPS = " Systemdisk_path is not exit"
        return self.disks_IOPS

    @deco
    def get_Datadisk_IOPS(self):
        """Datadisk_IOPS"""
        dbcfg = ParseInI(DBCFG_FILE_DEF, 'dbconfigs').load()
        disk_path = dbcfg['Datadisk_path']
        cmd_ckfio =  'fio -direct=1 -iodepth=64 -rw=randrw -ioengine=libaio -bs=1k -size=1G -numjobs=' \
                     '1 -runtime=10 -group_reporting -filename=%s -name=Rand_ReadWrite_IOPS_Test | ' \
                     'grep -E \'(read|write): IOPS\' |awk -F, \'{print$1}\'' % disk_path
        if os.path.exists(disk_path):
            if not self.disks_IOPS is None:
                if self.fio_flag:
                    self.disks_IOPS = cmd_output(cmd_ckfio)
                else:
                    self.disks_IOPS = "write:N/A\nread:N/A"
            else:
                self.check_fio()
                if self.fio_flag:
                    self.disks_IOPS = cmd_output(cmd_ckfio)
                else:
                    self.disks_IOPS = "write:N/A\nread:N/A"
        else :
            self.disks_IOPS = " Datadisk_path is not exit"
        return self.disks_IOPS


    @deco_val_chk
    def get_sudo(self):
        """Sudo access"""
        rc = run_cmd2('%s echo -n "check sudo access" > /dev/null 2>&1' % get_sudo_prefix())
        if rc:
            return 'Not set', ERR
        else:
            return 'Set', OK

    @deco_val_chk
    def get_ssh_pam(self):
        """SSH PAM settings"""
        if cmd_output('grep "^UsePAM yes" %s' % SSH_CFG_FILE):
            return 'Set', OK
        else:
            return 'Not set', ERR

    @deco_val_chk
    def get_loopback(self):
        """Localhost setting in /etc/hosts"""
        etc_hosts = cmd_output('cat /etc/hosts')
        loopback = r'127.0.0.1\s+localhost\s+localhost.localdomain\s+localhost4\s+localhost4.localdomain4'
        if re.findall(loopback, etc_hosts):
            return 'Set', OK
        else:
            return 'Not set', ERR

    @deco_val_chk
    def get_network_mgr_status(self):
        """NetworkManager service status"""
        if not run_cmd2('service NetworkManager status'):
            return 'Running', WARN
        else:
            return 'Not running', OK

    @deco_val_chk
    def get_ntp_status(self):
        """Ntp service status"""
        if not run_cmd2('service ntpd status'):
            return 'Running', OK
        elif not run_cmd2('service chronyd status'):
            return 'Running', OK
        else:
            return 'Not running', ERR

    @deco_val_chk
    def get_home_nfs(self):
        """NFS on /home"""
        nfs_home = cmd_output('mount|grep -c "on /home type nfs"')
        if int(nfs_home):
            return 'Mounted', ERR
        else:
            return 'Not mounted', OK

    @deco_val_chk
    def get_firewall_status(self):
        """Firewall status"""
        iptables_stat = cmd_output('iptables -nL|grep -vE "(Chain|target)"')
        if iptables_stat:
            return 'Running', WARN
        else:
            return 'Stopped', OK

    @deco_val_chk
    def get_traf_status(self):
        """Leftover Trafodion process"""
        mon_process = cmd_output('ps -ef|grep -v grep|grep -cE "sqcheck|monitor COLD"')
        if int(mon_process) > 0:
            return 'Running', WARN
        else:
            return 'Stopped', OK

    @deco_val_chk
    def get_fqdn(self):
        """FQDN"""
        fqdn = cmd_output('hostname -f')
        if not '.' in fqdn:
            return fqdn, ERR
        else:
            return fqdn, OK

    def firewall_start(self):
        os_major_ver = platform.dist()[1].split('.')[0]
        cmd_os6 = 'service iptables start'
        cmd_os7 = 'systemctl start firewalld'
        if os_major_ver in REDHAT6_MAJOR_VER:
            p=subprocess.Popen(cmd_os6,shell=True)
            return_code = p.wait
            return return_code
        else:
            p=subprocess.Popen(cmd_os7,shell=True)
            return_code = p.wait
            return return_code
        if return_code:
            iptables_stat = cmd_output('iptables -nL|grep -vE "(Chain|target)"')
            if not iptables_stat:
                print 'Failed to turn on the firewall by python script "iptables_start()", please turn on the firewall manually'
                exit(1)


    def firewall_stop(self):
        os_major_ver = platform.dist()[1].split('.')[0]
        cmd_os6 = 'service iptables stop'
        cmd_os7 = 'systemctl stop firewalld'
        if os_major_ver in REDHAT6_MAJOR_VER:
            p = subprocess.Popen(cmd_os6, shell=True)
            return_code = p.wait
            return return_code
        else:
            p = subprocess.Popen(cmd_os7, shell=True)
            return_code = p.wait
            return return_code
        if return_code:
            iptables_stat = cmd_output('iptables -nL|grep -vE "(Chain|target)"')
            if iptables_stat:
                run_cmd('iptables -F')


    portlist_occupied = []
    portlist_closed = []
    portlist_opening = []
    portlist = []
    checkport_flag = 0
    firewall_active = 0

    @deco_val_chk
    def get_ports_check(self):
        """Port check function"""
        #Checking the Firewall Status
        iptables_stat = cmd_output('iptables -nL|grep -vE "(Chain|target)"')
        if not iptables_stat:
            self.firewall_active= 1
            self.firewall_start()

        ports_name_arr = ParseInI(CHECK_PORTS, 'ports').load()
        for port_name in ports_name_arr:
            port = ports_name_arr['%s' % port_name]
            if '-' in port:
                for port_temp in range(int(port.split('-')[0]), int(port.split('-')[1]) + 1):
                    self.portlist.append(int(port_temp))
            else:
                self.portlist.append(int(port))

        for port in self.portlist:
            self.checkport_flag = 'true'
            port_status_opening = cmd_output('sudo firewall-cmd --query-port=%d/tcp|grep yes' % port)
            port_status_occupied = cmd_output('netstat -lnpt |grep %d' % port)
            if not port_status_opening:
                self.portlist_closed.append(port)
            else:
                if port_status_occupied:
                    self.portlist_occupied.append(port)
                else:
                    self.portlist_opening.append(port)

        if self.firewall_active == 1:
            self.firewall_active = 0
            self.firewall_stop()
        return self.portlist_closed, self.portlist_occupied
    @deco_val_chk
    def get_ports_occupied(self):
        """Ports_status(occupied)"""
        if self.checkport_flag == 0:
            self.checkport_flag = 1
            self.get_ports_check()

        return self.portlist_occupied, WARN

    @deco_val_chk
    def get_ports_closed(self):
        """Ports_status(closed)"""
        if self.checkport_flag == 0:
            self.checkport_flag = 1
            self.get_ports_check()

        return self.portlist_closed, ERR

    @deco_warn_err
    def get_umask_root(self):
        """umask_root"""
        if self.umaskid is None and os.path.exists("/etc/profile"):
            self.umaskid = cmd_output("cat /etc/profile|grep umask")
            self.umaskid = re.findall("\d+", self.umaskid)
        return self.umaskid[1] if self.umaskid is not None else NA

    @deco_warn_err
    def get_umask_other(self):
        """umask_other"""
        if self.umaskid is None and os.path.exists("/etc/profile"):
            self.umaskid = cmd_output("cat /etc/profile|grep umask")
            self.umaskid = re.findall("\d+", self.umaskid)
        return self.umaskid[0] if self.umaskid is not None else NA

    @deco_val_chk
    def get_default_java(self):
        """Default java version"""
        jdk_path = glob('/usr/java/*') + \
                   glob('/usr/jdk64/*') + \
                   glob('/usr/lib/jvm/java-*-openjdk')

        jdk_list = {}  # {jdk_version: jdk_path}
        for path in jdk_path:
            jdk_ver = cmd_output('%s/bin/javac -version' % path)

            try:
                main_ver, sub_ver = re.search(r'(\d\.\d\.\d)_(\d+)', jdk_ver).groups()
                # don't support JDK version less than 1.7.0_65
                if main_ver == '1.7.0' and int(sub_ver) < 65:
                    continue
                jdk_list[main_ver] = path
            except AttributeError:
                continue

        # auto detect JDK1.8
        if jdk_list.has_key('1.8.0'):
            return jdk_list['1.8.0'], OK
        else:
            return NA, ERR

    @deco_val_chk
    def get_license_status(self):
        """license status"""
        try:
            lic = License(EDB_LIC_FILE)
            if lic.nolic:
                return 'NA', ERR
            else:
                if lic.is_multi_tenancy_enabled():
                    return 'MTOK', OK
                else:
                    return 'OK', OK
        except StandardError:
            return 'NA', ERR

    @deco_val_valid
    def get_hdfs_ver(self):
        """HDFS version"""
        if self.dbcfgs.has_key('hadoop_home'):  # apache distro
            hadoop_home = self.dbcfgs['hadoop_home']
        else:
            hadoop_home = DEF_HADOOP_HOME
        hdfs_ver = cmd_output('%s/bin/hdfs version | head -n1' % hadoop_home)

        try:
            hdfs_ver = re.search(r'Hadoop (\d\.\d)', hdfs_ver).groups()[0]
        except AttributeError:
            return NA
        return hdfs_ver

    @deco_val_valid
    def get_hive_ver(self):
        """Hive version"""
        if self.dbcfgs.has_key('hive_home'):  # apache distro
            hive_home = self.dbcfgs['hive_home']
        else:
            hive_home = DEF_HIVE_HOME
        hive_ver = cmd_output('%s/bin/hive --version | grep Hive' % hive_home)

        try:
            hive_ver = re.search(r'Hive (\d\.\d)', hive_ver).groups()[0]
        except AttributeError:
            return NA
        return hive_ver

    @deco_val_valid
    def get_hbase_ver(self):
        """HBase version"""
        if self.dbcfgs.has_key('hbase_home'):  # apache distro
            hbase_home = self.dbcfgs['hbase_home']
        else:
            hbase_home = DEF_HBASE_HOME
        hbase_ver = cmd_output('%s/bin/hbase version | head -n1' % hbase_home)

        try:
            hbase_ver = re.search(r'HBase (\d\.\d)', hbase_ver).groups()[0]
        except AttributeError:
            return NA
        return hbase_ver

    @deco_val_valid
    def get_linux_distro(self):
        """Linux distro"""
        os_dist, os_ver = platform.dist()[:2]
        return '%s%s' % (os_dist, os_ver.split('.')[0])

    def _get_disk_free(self, path):
        disk_free = [l.split()[-3] for l in self.DFINFO if l.split()[-1] == path]
        if disk_free:
            return float(disk_free[0])
        else:
            return 0

    def _get_disks(self):
        SYS_BLOCK = '/sys/block'
        devices = cmd_output('ls %s' % SYS_BLOCK).split('\n')
        disk_devices = [device for device in devices if os.path.exists('%s/%s/device' % (SYS_BLOCK, device))]
        return disk_devices

    def _get_mount_disks(self):
        disk_devices = self._get_disks()
        disk_fs = []
        all_fs = [l.split()[0] for l in self.DFINFO]
        for fs in all_fs:
            for device in disk_devices:
                if device in fs:
                    disk_fs.append(fs)

        disk_df = [df for df in self.DFINFO if
                   df.split()[0] in disk_fs and df.split()[-1] != '/home' and df.split()[-1] != '/' and df.split()[
                       -1] != '/opt' and '/boot' not in df.split()[-1]]
        return disk_df

    @deco
    def get_mount_disks(self):
        """Mount Disks"""
        disk_df = self._get_mount_disks()
        mount_disks = [disk.split()[-1] for disk in disk_df]
        return ','.join(mount_disks)

    @deco_warn_err
    def get_user_disk_free(self):
        """Free data File System spaces"""
        disk_df = self._get_mount_disks()
        user_disk_free = sum([self._get_disk_free(l.split()[-1]) for l in disk_df])
        return "%0.0f" % round(float(user_disk_free) / (1024 * 1024), 2)

    @deco_warn_err
    def get_install_disk_free(self):
        """Free System spaces"""
        install_disk_free = self._get_disk_free('/') + self._get_disk_free('/home') + self._get_disk_free('/opt')
        return "%0.0f" % round(float(install_disk_free) / (1024 * 1024), 2)

    @deco_warn_err
    def get_disk_nums(self):
        """Disk numbers"""
        return len(self._get_disks())

    @deco_warn_err
    def get_net_bw(self):
        """Network Card bandwidth"""
        # output are several lines with 1 or 10 or other numbers
        net_bw = cmd_output('lspci -vv |grep -i ethernet |grep -o "[0-9]\+\s*Gb"|sed s"/s*Gb//g"').split('\n')
        net_bw.append('0')  # add a default int when not detected
        bandwidth = max([int(bw) for bw in net_bw if bw.isdigit()])

        # try another way to get the bandwidth
        if bandwidth == 0:
            interfaces = [n.strip() for n in cmd_output("ip a |grep 'state UP' |awk -F: '{print $2}'").split('\n')]

            net_bw = ['0']
            for interface in interfaces:
                bw = cmd_output('ethtool %s |grep -i speed |grep -oE [0-9]\+' % interface)
                net_bw.append(bw)
            bandwidth = max([float(bw) / 1000 for bw in net_bw if bw.isdigit()])

        return bandwidth

    @deco_warn_err
    def get_mem_total(self):
        """Total memory size"""
        mem = self._get_mem_info('MemTotal')
        memsize = mem.split()[0]

        return "%0.0f" % round(float(memsize) / (1024 * 1024), 2)

    @deco_warn_err
    def get_mem_free(self):
        """Current free memory size"""
        free = self._get_mem_info('MemFree')
        buffers = self._get_mem_info('Buffers')
        cached = self._get_mem_info('Cached')
        memfree = float(free) + float(buffers) + float(cached)

        return "%0.0f" % round(float(memfree) / (1024 * 1024), 2)

    @deco_warn_err
    def get_swap_pct(self):
        """Swap/Mem percentage"""
        swap = self._get_mem_info('SwapTotal')
        mem = self._get_mem_info('MemTotal')
        swap_size = swap.split()[0]
        mem_size = mem.split()[0]

        return str(int(swap_size) * 100 / int(mem_size))

    @deco_warn_err
    def get_tcp_time(self):
        """net.ipv4.tcp_keepalive_time"""
        return self._get_sysctl_info('net.ipv4.tcp_keepalive_time')

    @deco_warn_err
    def get_tcp_intvl(self):
        """net.ipv4.tcp_keepalive_intvl"""
        return self._get_sysctl_info('net.ipv4.tcp_keepalive_intvl')

    @deco_warn_err
    def get_tcp_probes(self):
        """net.ipv4.tcp_keepalive_probes"""
        return self._get_sysctl_info('net.ipv4.tcp_keepalive_probes')

    @deco_warn_err
    def get_msgmnb(self):
        """kernel.msgmnb"""
        return self._get_sysctl_info('kernel.msgmnb')

    @deco_warn_err
    def get_msgmax(self):
        """kernel.msgmax"""
        return self._get_sysctl_info('kernel.msgmax')

    @deco_warn_err
    def get_msgmni(self):
        """kernel.msgmni"""
        return self._get_sysctl_info('kernel.msgmni')

    @deco_warn_err
    def get_file_max(self):
        """fs.file-max"""
        return self._get_sysctl_info('fs.file-max')

    @deco_warn_err
    def get_overcommit_memory(self):
        """vm.overcommit_memory"""
        return self._get_sysctl_info('vm.overcommit_memory')

    @deco_warn_err
    def get_cpu_cores(self):
        """CPU cores"""
        return self.CPUINFO.count('processor')

    @deco_consistent
    def get_ext_interface(self):
        """External network interface"""
        return cmd_output('/sbin/ip route |grep default|awk \'{print $5}\'')

    @deco_consistent
    def get_kernel_ver(self):
        """Linux Kernel"""
        return cmd_output('uname -r')

    @deco_consistent
    def get_systime(self):
        """Current system time"""
        return time.strftime('%Y-%m-%d %H:%M:%S')

    @deco_consistent
    def get_timezone(self):
        """System timezone"""
        return time.tzname[0]

    @deco
    def get_dependencies(self):
        """EsgynDB RPM dependencies"""
        dic = {}
        package_list = CCFG.get('packages_centos')
        if OS_UBUNTU in platform.dist()[0].upper():
            package_list = CCFG.get('packages_ubuntu')

        for pkg in self.PKGINFO:
            try:
                pkg_name = re.search(r'(.*?)-\d+\.\d+\.*', pkg).groups()[0]
            except AttributeError:
                pkg_name = pkg
            for reqpkg in package_list:
                if reqpkg in pkg_name:
                    dic[reqpkg] = pkg.replace('%s-' % reqpkg, '')

        napkgs = list(set(package_list).difference(set(dic.keys())))

        for pkg in napkgs:
            dic[pkg] = NA
        return dic

    @deco
    def get_hadoop_distro(self):
        """Hadoop distro"""
        for pkg in self.PKGINFO:
            if 'cloudera-manager-agent' in pkg:
                return 'CM' + re.search(r'cloudera-manager-agent-(\d+\.\d+\.\d+).*', pkg).groups()[0]
            elif 'hdp-select' in pkg:
                return 'HDP' + re.search(r'hdp-select-(\d+\.\d+\.\d+).*', pkg).groups()[0]
        return UNKNOWN

    def _get_core_site_info(self, name):
        if self.dbcfgs.has_key('hadoop_home'):  # apache distro
            core_site_xml = '%s/etc/hadoop/core-site.xml' % self.dbcfgs['hadoop_home']
        else:
            core_site_xml = DEF_CORE_SITE_XML

        if os.path.exists(core_site_xml):
            p = ParseXML(core_site_xml)
            return p.get_property(name)
        else:
            return NA

    @deco
    def get_hadoop_auth(self):
        """Hadoop authentication"""
        return self._get_core_site_info('hadoop.security.authentication')

    @deco
    def get_hadoop_group_mapping(self):
        """Hadoop security group mapping"""
        mapping = self._get_core_site_info('hadoop.security.group.mapping')
        if 'ShellBasedUnixGroupsMapping' in mapping:
            return 'SHELL'
        elif 'LdapGroupsMapping' in mapping:
            return 'LDAP'
        else:
            return 'NONE'

    #    @deco
    #    def get_cpu_model(self):
    #        """CPU model"""
    #        return self._get_cpu_info('model name')

    @deco
    def get_hostname(self):
        """Short hostname"""
        return cmd_output('hostname -s')

    @deco
    def get_python_ver(self):
        """Python version"""
        return platform.python_version()

    @deco
    def get_home_dir(self):
        """Trafodion home directory"""
        if self.dbcfgs.has_key('traf_user'):
            traf_user = self.dbcfgs['traf_user']
            return cmd_output("getent passwd %s | awk -F: '{print $6}' | sed 's/\/%s//g'" % (traf_user, traf_user))
        else:
            return ''

    @deco
    def get_net_interfaces(self):
        """Network interfaces"""
        net_devs = cmd_output('cat /proc/net/dev').split('\n')[3:]
        interfaces = [i.split()[0].replace(':', '') for i in net_devs]
        return ','.join(interfaces)

    @deco
    def get_core_pattern(self):
        """System Core pattern setting"""
        return cmd_output('cat /proc/sys/kernel/core_pattern')

    @deco
    def get_aws_ec2(self):
        """AWS EC2 environment"""
        if cmd_output('cat /sys/hypervisor/uuid')[:3] == 'ec2':
            return 'EC2'
        else:
            return 'OTHER'

    @deco_val_chk
    def get_selinux_status(self):
        """Selinux status"""
        selinux = cmd_output('sestatus')
        if ('permissive' in selinux) or ('disabled' in selinux):
            return 'Stopped', OK
        else:
            return 'Running', WARN

    @deco_val_chk
    def get_transparent_hugepages(self):
        """Transparent huge pages status"""
        enabled_cmd = cmd_output('cat /sys/kernel/mm/transparent_hugepage/enabled')
        defrag_cmd = cmd_output('cat /sys/kernel/mm/transparent_hugepage/defrag')
        if ('[never]' in enabled_cmd) and ('[never]' in defrag_cmd):
            return 'Disabled', OK
        else:
            return 'Enabled', WARN


def testrun():
    discover = Discover()
    methods = ['get_cpu_arch', 'get_cpu_cores', 'get_cpu_freq','get_net_ty','get_DRAM_clock', 'get_cpu_MHz', 'get_Systemdisk_IOPS','get_Datadisk_IOPS' , 'get_disk_nums', 'get_fqdn', 'get_umask_root', 'get_umask_other',
               'get_kernel_ver', 'get_linux_distro',
               'get_mem_total', 'get_net_bw', 'get_swap_pct', 'get_net_interfaces', 'get_ports_occupied',
               'get_ports_closed']
    result = {}
    for method in methods:
        key, value = getattr(discover, method)()  # call method
        result[key] = value

    print json.dumps(result)


# main
if __name__ == '__main__':
    testrun()
