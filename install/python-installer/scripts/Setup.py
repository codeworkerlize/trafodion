#!/usr/bin/env python
import os
import re
import socket
import platform
import logging
from collections import defaultdict
from constants import *
from common import err, cmd_output, run_cmd, set_stream_logger, write_file, get_sudo_prefix, \
                   ParseInI, ParseXML, append_file, gen_template_file, License, mod_file

logger = logging.getLogger()

class Setup(object):
    def __init__(self, cfgs):
        self.cfgs = defaultdict(str, cfgs)
        self.ports = ParseInI(DEF_PORT_FILE, 'ports').load()
        self.ssh_port = cfgs['ssh_port']
        self.distro = self.cfgs['distro']
        self.traf_user = self.cfgs['traf_user'] # e.g. trafodion
        self.traf_home = self.cfgs['traf_abspath'] # e.g. /opt/trafodion/edb-xxx
        self.traf_var = self.cfgs['traf_var']
        self.traf_log = self.cfgs['traf_log']
        self.core_pattern = '%s/cores/core-%%e-%%p-%%t' % self.cfgs['core_loc']
        self.hdfs_user = self.cfgs['hdfs_user'] if self.cfgs['hdfs_user'] else 'hdfs'
        self.hdfs_group = cmd_output('id -ng %s' % self.hdfs_user)
        self.hbase_user = self.cfgs['hbase_user'] if self.cfgs['hbase_user'] else 'hbase'
        self.hbase_group = cmd_output('id -ng %s' % self.hbase_user)
        self.hbase_lib_path = self.cfgs['hbase_lib_path']
        self.zookeeper_user = self.cfgs['zookeeper_user'] if self.cfgs['zookeeper_user'] else 'zookeeper'
        self.hadoop_group = 'hadoop'
        self.java_home = self.cfgs['java_home']
        self.traf_user_dir = '%s/%s' % (self.cfgs['home_dir'], self.traf_user) # e.g. /opt/trafodion
        self.traf_package_file = '/tmp/' + self.cfgs['traf_package'].split('/')[-1]
        self.user_exist = cmd_output('getent passwd %s' % self.traf_user)
        self.platform = platform.dist()[0].upper()
        if self.user_exist:
            self.traf_group = cmd_output('id -ng %s' % self.traf_user)
        else:
            self.traf_group = self.traf_user
        self.traf_groups = cmd_output('id -Gn %s' % self.traf_user)

    def pre_check(self):
        if not os.path.exists(self.hbase_lib_path):
            err('Cannot find HBase lib folder: %s' % self.hbase_lib_path)
        if not os.path.exists(self.java_home):
            err('Cannot find Java location: %s' % self.java_home)

        jdk_ver = cmd_output('%s/bin/javac -version' % self.java_home)
        try:
            jdk_ver, sub_ver = re.search(r'javac (\d\.\d).\d_(\d+)', jdk_ver).groups()
            if jdk_ver == '1.7':
                err('JDK1.7 is not supported')
        except AttributeError:
            err('No JDK found')

    def setup_keepalive(self):
        ### keepalive config ###
        ka_template_file = '%s/conf/keepalived.conf' % self.traf_home
        if os.path.exists(ka_template_file):
            logger.info('Setting up trafodion keepalived configs ...')
            ka_config_file = '/etc/keepalived/%s.conf' % self.traf_user
            ka_template = cmd_output('cat %s' % ka_template_file)
            run_cmd('mkdir -p /etc/keepalived')
            if self.cfgs.has_key('dcs_floating_ip'):
                dcs_fip = self.cfgs['dcs_floating_ip']
            else:
                dcs_fip = '0.0.0.0'
            if self.cfgs.has_key('dcs_interface'):
                dcs_interface = self.cfgs['dcs_interface']
            else:
                dcs_interface = 'eth0'

            STATE="BACKUP"
            ka_priority = 50

            chost = socket.getfqdn()
            arr = self.cfgs['dcs_master_nodes'].split(',')
            for index in range(len(arr)):
              if chost == self.cfgs['master_node']:
                 STATE="MASTER"
                 ka_priority = 100
                 break
              if chost == arr[index]:
                 STATE="BACKUP"
                 ka_priority = 100 - index
                 break

            ka_config = {'NET_INTERFACE': dcs_interface,
                         'DCS_PORT': self.ports['dcs_master_port'],
                         'ROLE': '%s' % STATE,
                         'PRIORITY': '%d' % ka_priority, 
                         'FLOATING_IP': dcs_fip}
            gen_template_file(ka_template, ka_config_file, ka_config)
            run_cmd('chmod 644 %s' % ka_config_file)

    def setup_user(self):
        ### create trafodion user and group ###
        if self.user_exist:
            # trafodion user exists, set actual trafodion group
            logger.info('Trafodion user already exists, skip creating trafodion user')
        else:
            logger.info('Creating trafodion user and group ...')
            # default trafodion group
            if not cmd_output('getent group %s' % self.traf_group):
                run_cmd('/usr/sbin/groupadd %s' % self.traf_group)
            if os.path.exists(self.traf_user_dir):
                run_cmd('chmod 700 %s' % self.traf_user_dir)

            useradd_cmd_ptr = '/usr/sbin/useradd --shell /bin/bash -r -m %s -g %s --home %s --password ' % (self.traf_user, self.traf_group, self.traf_user_dir)
            if self.cfgs['traf_shadow']:
                run_cmd('%s "%s"' % (useradd_cmd_ptr, self.cfgs['traf_shadow']))
            else:
                run_cmd('%s "$(openssl passwd %s)"' % (useradd_cmd_ptr, self.cfgs['traf_pwd']))

            if OS_CENTOS in self.platform:
                if not os.path.exists("%s/.bash_profile" % self.traf_user_dir):
                    cmd_output('cp /etc/skel/.bash* %s' % self.traf_user_dir)
            elif OS_UBUNTU in self.platform or OS_KYLIN in self.platform:
                if not os.path.exists("%s/.profile" % self.traf_user_dir):
                    cmd_output('cp /etc/skel/.bash* %s' % self.traf_user_dir)
                    cmd_output('cp /etc/skel/.profile %s' % self.traf_user_dir)

        if self.distro == 'CDH_APACHE':
            if self.hadoop_group not in self.traf_groups:
                run_cmd('/usr/sbin/usermod -a -G %s %s' % (self.hadoop_group, self.traf_user))
        else:
            # add trafodion to hbase user group
            if self.hbase_group not in self.traf_groups:
                run_cmd('/usr/sbin/usermod -a -G %s %s' % (self.hbase_group, self.traf_user))
            # add trafodion to hdfs user group
            if self.hdfs_group not in self.traf_groups:
                run_cmd('/usr/sbin/usermod -a -G %s %s' % (self.hdfs_group, self.traf_user))
            # add trafodion to hive user group, assume hive group name is hive
            # don't check return value since hive may not be installed
            if "hive" not in self.traf_groups:
                output = cmd_output('/usr/sbin/usermod -a -G hive %s' % self.traf_user)
                if output: logger.info(output)

        if not os.path.exists(self.traf_user_dir):
            run_cmd('mkdir -p %s' % self.traf_user_dir)
            run_cmd('chmod 700 %s' % self.traf_user_dir)

        if OS_CENTOS in self.platform:
            if not os.path.exists("%s/.bash_profile" % self.traf_user_dir):
                run_cmd('cp /etc/skel/.bash* %s' % self.traf_user_dir)
        elif OS_UBUNTU in self.platform:
            if not os.path.exists("%s/.profile" % self.traf_user_dir):
                run_cmd('cp /etc/skel/.bash* %s' % self.traf_user_dir)
                run_cmd('cp /etc/skel/.profile %s' % self.traf_user_dir)

        ### set up trafodion passwordless ssh ###
        private_key_file = '%s/.ssh/id_rsa' % self.traf_user_dir
        # no ssh key generated in dbmgr agent mode
        if os.path.exists(TMP_SSHKEY_FILE) and not os.path.exists(private_key_file):
            logger.info('Setting up trafodion passwordless ssh ...')
            run_cmd('mkdir -p %s/.ssh/' % self.traf_user_dir)
            run_cmd('chmod 700 %s/.ssh/' % self.traf_user_dir)
            # the key is generated in copy_file script running on the installer node
            run_cmd('cp %s %s.pub %s/.ssh/' % (TMP_SSHKEY_FILE, TMP_SSHKEY_FILE, self.traf_user_dir))

            pub_key_file = '%s/.ssh/id_rsa.pub' % self.traf_user_dir
            auth_key_file = '%s/.ssh/authorized_keys' % self.traf_user_dir
            ssh_cfg_file = '%s/.ssh/config' % self.traf_user_dir

            run_cmd('cat %s >> %s' % (pub_key_file, auth_key_file))
            ssh_cfg_info = 'StrictHostKeyChecking=no\nNoHostAuthenticationForLocalhost=yes\n'
            if self.ssh_port: ssh_cfg_info += 'Port=%s\n' % self.ssh_port
            with open(ssh_cfg_file, 'w') as f:
                f.write(ssh_cfg_info)
            run_cmd('chmod 600 %s %s %s' % (private_key_file, auth_key_file, ssh_cfg_file))
        else:
            logger.info('Passwordless ssh is not set this time')

    def setup_package(self):
        ### untar traf package, package comes from copy_files.py ###
        logger.info('Extracting Trafodion binary package ...')
        run_cmd('mkdir -p %s' % self.traf_home)
        run_cmd('mkdir -p %s %s %s' % (TRAF_CFG_DIR, self.traf_var, self.traf_log))
        run_cmd('mkdir -p %s/sqllogs' % self.traf_user_dir) # logs for update statistics
        # donnot overwrite dcs-site settings in normal install
        if self.traf_package_file == TMP_TRAF_PKG_FILE:
            run_cmd('tar xf %s -C %s' % (self.traf_package_file, self.traf_home))
        else:
            run_cmd('tar xf %s --exclude dcs-site.xml -C %s' % (self.traf_package_file, self.traf_home))
        run_cmd('chmod 700 %s' % self.traf_home)

        ### copy trafodion bashrc ###
        logger.info('Setting up trafodion bashrc file ...')
        bashrc_template = '%s/sysinstall/home/trafodion/.bashrc' % self.traf_home
        bashrc_file = '%s/.bashrc' % self.traf_user_dir
        # backup orig bashrc
        if os.path.exists(bashrc_file):
            run_cmd('cp -f %s %s.bak' % (bashrc_file, bashrc_file))
        run_cmd('cp -f %s %s' % (bashrc_template, bashrc_file))
        # copy default config files
        if os.path.exists(TMP_CFG_DIR):
            run_cmd('cp -rf %s %s' % (TMP_CFG_DIR, EDB_LIC_DIR))
        else:
            if os.path.exists('%s/ms.env.custom' % TRAF_CFG_DIR):
                run_cmd('ls %s/conf | grep -v ms.env.custom | xargs -I {} cp -r %s/conf/{} %s/' % (self.traf_home, self.traf_home, TRAF_CFG_DIR))
            else:
                run_cmd('cp -rf %s/conf/* %s/' % (self.traf_home, TRAF_CFG_DIR))
        run_cmd('chmod 664 %s/ms.env.custom' % TRAF_CFG_DIR)

        # copy ms.env when add node
        if os.path.exists('/tmp/ms.env'):
            run_cmd('cp -f %s %s' % (TMP_MSENV_FILE, self.traf_var))

        ### soft link traf home to default traf home ###
        run_cmd('mkdir -p %s' % DEF_TRAF_HOME_DIR)
        run_cmd('rm -f %s' % DEF_TRAF_HOME)
        run_cmd('ln -s %s %s' % (self.traf_home, DEF_TRAF_HOME))

        # set core_pattern
        core_dir = '%s/cores' % self.cfgs['core_loc']
        run_cmd('umask 022; mkdir -p %s' % core_dir)
        run_cmd('chmod 777 %s' % core_dir)

        self._setup_permission()

    def _setup_permission(self):
        ### set permission for /etc/trafodion, TRAF_HOME and user home ###
        run_cmd('chown -R %s:%s %s %s %s %s %s' % (self.traf_user, self.traf_group, EDB_LIC_DIR, DEF_TRAF_HOME, self.traf_home, self.traf_user_dir, self.traf_var))
        # non-recursive, don't clobber install sub-dir for install user
        run_cmd('chown %s:%s %s' % (self.traf_user, self.traf_group, self.traf_log))
        # The system commands file contains platform commands that can be run through dbmgr.
        # Secure it by making root user as the owner to prevent malicious edit of the commands
        dbmgr_system_commands_file = '%s/dbmgr/conf/system_commands.json' % self.traf_home
        run_cmd('%s chown root:root %s' % (get_sudo_prefix(), dbmgr_system_commands_file))
        run_cmd('%s chmod 755 %s' % (get_sudo_prefix(), dbmgr_system_commands_file))

    def setup_aws(self):
        ### set aws configs in ~/.aws if on AWS EC2 environment ###
        if self.cfgs['aws_ec2'] == 'EC2':
            if not self.cfgs['aws_overwrite'] or self.cfgs['aws_overwrite'].upper() == 'Y':
                logger.info('Setting up AWS CLI')
                awscli_file = '/tmp/awscli-bundle.zip'
                if not os.path.exists(awscli_file):
                    logger.info('Downloading awscli-bundle.zip from aws website...')
                    run_cmd('curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o %s' % awscli_file, timeout=60)
                run_cmd('rm -rf /tmp/awscli-bundle')
                run_cmd('unzip %s -d /tmp' % awscli_file)
                run_cmd('/tmp/awscli-bundle/install -i /usr/local/aws -b /usr/bin/aws')
                run_cmd('mkdir -p %s/.aws' % self.traf_user_dir)

                aws_config_file = '%s/.aws/config' % self.traf_user_dir
                aws_cred_file = '%s/.aws/credentials' % self.traf_user_dir
                aws_config = """
[default]
aws_access_key_id = %s
aws_secret_access_key = %s
""" % (self.cfgs['aws_access_key_id'], self.cfgs['aws_secret_access_key'])
                aws_cred = """
[default]
output = %s
region = %s
""" % (self.cfgs['aws_output_format'], self.cfgs['aws_region_name'])
                write_file(aws_config_file, aws_config)
                write_file(aws_cred_file, aws_cred)

                self._setup_permission()

    def setup_system(self):
        ### kernel settings ###
        logger.info('Setting up sysctl configs ...')
        sysctl_items = {'kernel.msgmnb': 'kernel.msgmnb=65536',
                        'kernel.msgmax': 'kernel.msgmax=65536',
                        'kernel.pid_max': 'kernel.pid_max=786420',
                        'net.ipv4.ip_local_reserved_ports': 'net.ipv4.ip_local_reserved_ports=0-32767,50070,60000,60010,60020,60030',
                        'kernel.core_pattern': 'kernel.core_pattern=%s' % self.core_pattern}
        with open(SYSCTL_CONF_FILE, 'r') as f:
            lines = f.read()
        existed_items = [item for item in sysctl_items.keys() if item in lines]
        new_items = set(sysctl_items.keys()).difference(set(existed_items))
        for item in existed_items:
            mod_file(SYSCTL_CONF_FILE, {item + '=.*': sysctl_items[item]})
        for item in new_items:
            append_file(SYSCTL_CONF_FILE, sysctl_items[item])
        cmd_output('/sbin/sysctl -e -p /etc/sysctl.conf 2>&1 > /dev/null')

        ### copy init script ###
        logger.info('Setting up trafodion init script ...')
        init_script = '%s/sysinstall/etc/init.d/trafodion' % self.traf_home
        if os.path.exists(init_script):
            run_cmd('cp -rf %s /etc/init.d/' % init_script)
            # temporarily disable auto start in Ubuntu
            if OS_UBUNTU not in self.platform and OS_KYLIN not in self.platform:
                run_cmd('/sbin/chkconfig --add trafodion')
                run_cmd('/sbin/chkconfig --level 06 trafodion on')

        ### create and set permission for scratch file dir ###
        scratch_locs = self.cfgs['scratch_locs'].split(',')
        for loc in scratch_locs:
            if '$' in loc:
                logger.info('Shell variables exist in scratch folder name, skip creating folder [%s]' % loc)
            else:
                if not os.path.exists(loc):
                    logger.info('Creat scratch folder [%s]' % loc)
                    run_cmd('mkdir -p %s' % loc)
                run_cmd('chown %s %s' % (self.traf_user, loc))

    def setup_misc(self):
        """ mainly used for pyinstaller """
        ### only for elastic adding nodes ###
        if os.path.exists(TMP_LIC_FILE):
            run_cmd('mv -f %s %s' % (TMP_LIC_FILE, EDB_LIC_FILE))
            run_cmd('chown %s:%s %s' % (self.traf_user, self.traf_group, EDB_LIC_FILE))

        if os.path.exists(TMP_ID_FILE):
            run_cmd('mv -f %s %s' % (TMP_ID_FILE, EDB_ID_FILE))
            run_cmd('chown %s:%s %s' % (self.traf_user, self.traf_group, EDB_ID_FILE))

        if os.path.exists(TMP_CFG_FILE):
            run_cmd('mv -f %s %s' % (TMP_CFG_FILE, TRAF_CFG_FILE))
            run_cmd('chown %s:%s %s' % (self.traf_user, self.traf_group, TRAF_CFG_FILE))

        ### set up sudoers/ulimits file for trafodion user ###
        logger.info('Setting up sudoers/ulimits file for trafodion user ...')
        p = ParseXML(TEMPLATE_XML)
        sudoer_config = p.get_property('trafodion_sudoers_template')
        traf_ulimits_config = p.get_property('trafodion_ulimits_template')
        ulimits_config = p.get_property('ulimits_template')
        traf_ulimits_file = '%s/%s.conf' % (ULIMITS_DIR, self.traf_user)
        ulimits_file = ULIMITS_DIR + '/%s.conf'
        if self.cfgs['hbase_home']:
            hbase_home = self.cfgs['hbase_home']
        else:
            hbase_home = DEF_HBASE_HOME

        gen_template_file(traf_ulimits_config, traf_ulimits_file, {'traf_user': self.traf_user})
        if self.distro != 'CDH_APACHE':
            run_cmd('mkdir -p %s' % TRAF_SUDOER_DIR)
            gen_template_file(sudoer_config, TRAF_SUDOER_FILE, {'traf_user': self.traf_user, 'hbase_user': self.hbase_user, 'hdfs_user':  self.hdfs_user, 'hbase_home': hbase_home, 'default_traf_home': DEF_TRAF_HOME})
            gen_template_file(ulimits_config, ulimits_file % self.hbase_user, {'user': self.hbase_user, 'nofile': '65536', 'nproc': '65536'})
            gen_template_file(ulimits_config, ulimits_file % self.hdfs_user, {'user': self.hdfs_user, 'nofile': '128000', 'nproc': '65536'})
            gen_template_file(ulimits_config, ulimits_file % self.zookeeper_user, {'user': self.zookeeper_user, 'nofile': '32768', 'nproc': '16000'})

    def clean_up(self):
        # clean up sem files
        run_cmd('rm -rf /dev/shm/{sem.monitor.*,sem.rms.*}')
        # clean up hsperfdata
        run_cmd('rm -rf %s' % TRAF_HSPERFDATA_FILE)
        # clean up unused file at the last step
        run_cmd('rm -rf %s{,.pub}' % TMP_SSHKEY_FILE)
        run_cmd('rm -rf %s' % self.traf_package_file)
        # clean up trafodion config dir
        run_cmd('rm -rf %s' % TMP_CFG_DIR)
        # clean up ms.env
        run_cmd('rm -f %s' % TMP_MSENV_FILE)

    def setup_jars(self, copy_mode=True, pkg_mode=True):
        """ copy_mode=True:  copy trx to hbase lib, can be run on all RS nodes
            copy_mode=False: link trx to hbase lib, can be run on EsgynDB nodes
        """

        logger.info('Copying hbase-trx jar file to HBase lib folder ...')

        distro = self.cfgs['distro']
        if distro == 'APACHE':
            distro += self.cfgs['hbase_ver']
        elif distro == 'CDH_APACHE':
            distro = 'CDH5.16'

        distro, v1, v2 = re.search(r'(\w+)-*(\d+)\.(\d+)', distro).groups()
        if distro == 'CDH':
            if int(v2) == 4: trxver = '_4'
            if 4 < int(v2) < 7: trxver = '_5'
            if 7 < int(v2) < 16: trxver = '_7'
            if int(v2) == 16: trxver = '_16'
        elif distro == 'HDP':
            if int(v2) < 6:
                trxver = '_3'
            elif int(v2) == 6:
                # retrieve specific 2.6.x version
                fullver = cmd_output('/usr/bin/hbase version 2>&1 | head -1')
                # Hbase 3-part version, HDP 4-part version
                v3 = re.search(r'HBase \d+.\d+\.\d+\.\d+\.\d+\.(\d+)\..*', fullver).groups()[0]
                if int(v3) < 3:
                    trxver = '_3'
                else:
                    trxver = '63'
            else:
                trxver = '63'

        traf_lib_path = self.traf_home + '/export/lib'

        trafodion_utility_jar = 'trafodion-utility-%s.jar' % self.cfgs['traf_version']
        orig_trafodion_utility_file = '%s/%s' % (traf_lib_path, trafodion_utility_jar)
        dest_trafodion_utility_file = '%s/%s' % (self.hbase_lib_path, trafodion_utility_jar)

        hbase_trx_jar = 'hbase-trx-%s%s%s-%s.jar' % (distro.lower(), v1, trxver, self.cfgs['traf_version'])
        if copy_mode:
            orig_hbase_trx_file = '/tmp/%s' % hbase_trx_jar
        else:
            orig_hbase_trx_file = '%s/%s' % (traf_lib_path, hbase_trx_jar)
        dest_hbase_trx_file = '%s/%s' % (self.hbase_lib_path, hbase_trx_jar)

        esgyn_common_jar = 'esgyn-common-%s.jar' % self.cfgs['traf_version']
        if copy_mode:
            orig_esgyn_common_jar = '/tmp/%s' % esgyn_common_jar
        else:
            orig_esgyn_common_jar = '%s/%s' % (traf_lib_path, esgyn_common_jar)
        dest_esgyn_common_jar = '%s/%s' % (self.hbase_lib_path, esgyn_common_jar)

        logger.debug('Trafodion utility jar file is %s' % trafodion_utility_jar)
        logger.debug('HBase trx jar file is %s' % hbase_trx_jar)
        logger.debug('Esgyn common jar file is %s' % esgyn_common_jar)

        if not os.path.exists(orig_esgyn_common_jar):
            err('Cannot find Esgyn common jar \'%s\' for your Hadoop distribution' % esgyn_common_jar)

        if not os.path.exists(orig_hbase_trx_file):
            err('Cannot find HBase trx jar \'%s\' for your Hadoop distribution' % hbase_trx_jar)

        # reinstall mode, check if existing trx jar doesn't match the new trx jar file
        if self.cfgs.has_key('reinstall') and self.cfgs['reinstall'].upper() == 'Y':
            if not os.path.exists(dest_hbase_trx_file):
                err('The trx jar \'%s\' doesn\'t exist in hbase lib path, cannot do reinstall, please do regular install' % hbase_trx_jar)
            # no check esgyn-common-*.jar
        else:
            if copy_mode:
                ### copy trafodion-utility jar ###
                if os.path.exists(traf_lib_path):
                    # remove old one
                    run_cmd('rm -rf %s/trafodion-utility-*' % self.hbase_lib_path)
                    # copy new one
                    run_cmd('cp %s/trafodion-utility-* %s' % (traf_lib_path, self.hbase_lib_path))
                    # set permission
                    run_cmd('chmod a+r %s/trafodion-utility-*' % self.hbase_lib_path)

                    # esgyn-common.jar
                    # remove old trx jar files
                    run_cmd('rm -rf %s/esgyn-common-*' % self.hbase_lib_path)
                    # copy new one
                    run_cmd('cp %s %s' % (orig_esgyn_common_jar, dest_esgyn_common_jar))
                    # set permission
                    run_cmd('chmod a+r %s' % dest_esgyn_common_jar)
                    # remove common in tmp folder
                    run_cmd('rm -rf /tmp/esgyn-common-*')

                # remove old trx jar files
                run_cmd('rm -rf %s/hbase-trx-*' % self.hbase_lib_path)
                # copy new one
                run_cmd('cp %s %s' % (orig_hbase_trx_file, self.hbase_lib_path))
                # remove trx in tmp folder
                run_cmd('rm -rf /tmp/hbase-trx-*')
                # set permission
                run_cmd('chmod a+r %s/hbase-trx-*' % self.hbase_lib_path)
            else:
                # remove old trx jar files
                run_cmd('rm -rf %s' % dest_trafodion_utility_file)
                run_cmd('rm -rf %s' % dest_hbase_trx_file)
                run_cmd('rm -rf %s' % dest_esgyn_common_jar)
                if pkg_mode:
                    # trx cannot be read in trafodion's home dir, so copy directly
                    logger.info('Copying HBase trx jar file ...')
                    run_cmd('cp -f %s %s' % (orig_hbase_trx_file, dest_hbase_trx_file))
                    run_cmd('cp -f %s %s' % (orig_trafodion_utility_file, dest_trafodion_utility_file))
                    run_cmd('cp -f %s %s' % (orig_esgyn_common_jar, dest_esgyn_common_jar))
                    # set permission
                    run_cmd('chmod a+r %s %s %s' % (dest_hbase_trx_file, dest_trafodion_utility_file, dest_esgyn_common_jar))
                else:
                    # link new one
                    logger.info('Linking HBase trx jar file ...')
                    run_cmd('ln -s %s %s' % (orig_hbase_trx_file, dest_hbase_trx_file))
                    run_cmd('ln -s %s %s' % (orig_trafodion_utility_file, dest_trafodion_utility_file))
                    run_cmd('ln -s %s %s' % (orig_esgyn_common_jar, dest_esgyn_common_jar))
                    # set permission
                    run_cmd('chmod a+r %s %s %s' % (orig_hbase_trx_file, orig_trafodion_utility_file, orig_esgyn_common_jar))

    def setup_kerberos(self):
        """ setup Kerberos security """
        distro = self.cfgs['distro']
        admin_principal = self.cfgs['admin_principal']
        admin_passwd = self.cfgs['kdcadmin_pwd']
        kdc_server = self.cfgs['kdc_server']
        cluster_name = self.cfgs['cluster_name']
        # maxlife = self.cfgs['max_lifetime']
        # max_renewlife = self.cfgs['max_renew_lifetime']
        maxlife = '24hours'
        max_renewlife = '7days'
        kadmin_cmd = 'kadmin -p %s -w %s -s %s -q' % (admin_principal, admin_passwd, kdc_server)

        host_name = socket.getfqdn()
        realm = re.match('.*@(.*)', admin_principal).groups()[0]
        traf_keytab_dir = '/etc/%s/keytab' % self.traf_user
        traf_keytab = '%s/%s.keytab' % (traf_keytab_dir, self.traf_user)
        traf_principal = '%s/%s@%s' % (self.traf_user, host_name, realm)

        ### check KDC connection ###
        logger.info('Checking KDC server connection ...')
        run_cmd('%s listprincs' % kadmin_cmd)

        ### create principals and keytabs for trafodion user ###
        principal_exists = cmd_output('%s listprincs | grep -c %s' % (kadmin_cmd, traf_principal))
        if int(principal_exists) == 0: # not exist
            run_cmd('%s \'addprinc -randkey %s\'' % (kadmin_cmd, traf_principal))
            # Adjust principal's maxlife and maxrenewlife
            run_cmd('%s \'modprinc -maxlife %s -maxrenewlife %s %s\' >/dev/null 2>&1' % (kadmin_cmd, maxlife, max_renewlife, traf_principal))

        run_cmd('mkdir -p %s' % traf_keytab_dir)

        # TODO: need skip add keytab if exist?
        logger.info('Create keytab file for trafodion user')
        run_cmd('%s \'ktadd -k %s %s\'' % (kadmin_cmd, traf_keytab, traf_principal))
        run_cmd('chown %s %s' % (self.traf_user, traf_keytab))
        run_cmd('chmod 400 %s' % traf_keytab)

        ### create principals and keytabs for hdfs/hbase user ###
        logger.info('Create principals and keytabs for hdfs/hbase user')
        if 'CDH' in distro:
            hdfs_keytab = cmd_output('find /var/run/cloudera-scm-agent/process/ -name hdfs.keytab | head -n 1')
            hbase_keytab = cmd_output('find /var/run/cloudera-scm-agent/process/ -name hbase.keytab | head -n 1')
            hdfs_principal = '%s/%s@%s' % (self.hdfs_user, host_name, realm)
            hbase_principal = '%s/%s@%s' % (self.hbase_user, host_name, realm)
        elif 'HDP' in distro:
            hdfs_keytab = '/etc/security/keytabs/hdfs.headless.keytab'
            hbase_keytab = '/etc/security/keytabs/hbase.headless.keytab'
            hdfs_principal = '%s-%s@%s' % (self.hdfs_user, cluster_name.lower(), realm)
            hbase_principal = '%s-%s@%s' % (self.hbase_user, cluster_name.lower(), realm)

        sudo_prefix = get_sudo_prefix()
        kinit_cmd_ptr = '%s su - %s -s /bin/bash -c "kinit -kt %s %s"'
        if hdfs_keytab: # elastic changes, the newly added node may not contain hdfs service
            run_cmd(kinit_cmd_ptr % (sudo_prefix, self.hdfs_user, hdfs_keytab, hdfs_principal))
        run_cmd(kinit_cmd_ptr % (sudo_prefix, self.hbase_user, hbase_keytab, hbase_principal))

        ### set bashrc ###
        logger.info('Set automatic ticket renewal in trafodion bashrc file ...')
        kinit_bashrc = """

# ---------------------------------------------------------------
# if needed obtain and cache the Kerberos ticket-granting ticket
# start automatic ticket renewal process
# ---------------------------------------------------------------
klist -s >/dev/null 2>&1
if [[ $? -eq 1 ]]; then
    kinit -kt %s %s >/dev/null 2>&1
fi
""" % (traf_keytab, traf_principal)

        traf_bashrc = '%s/.bashrc' % self.traf_user_dir
        append_file(traf_bashrc, kinit_bashrc)

        ### Grant all privileges to the Trafodion principal in HBase ###
        first_node = self.cfgs['node_list'].split(',')[0]
        # run below commands on first node only
        if first_node in host_name or host_name in first_node:
            logger.info('Granting all privileges to the Trafodion principal in HBase ...')
            run_cmd('echo "grant \'%s\', \'RWXCA\'" | %s su - %s -s /bin/bash -c "hbase shell" > /tmp/hbase_shell.out' % (self.traf_user, sudo_prefix, self.hbase_user))
            has_err = cmd_output('grep -c ERROR /tmp/hbase_shell.out')
            if int(has_err):
                err('Failed to grant HBase privileges to %s' % self.traf_user)
            run_cmd('rm /tmp/hbase_shell.out')

    def setup_cgroups(self):
        def chk_kernel_ver(kernel_ver):
            # return true if kernel version > 2.6.32-573.7.1
            major = kernel_ver.split('-')[0]
            minor = kernel_ver.split('-')[1]
            if major == '2.6.32': # centos6 kernel major version
                minors = [m for m in minor.split('.') if m.isdigit()]
                if int(minors[0]) < 573:
                    return False
                elif int(minors[0]) == 573 and len(minors) > 1 and int(minors[1]) < 7:
                    return False
            return True

        ### cgroup settings ###
        try:
            lic = License(EDB_LIC_FILE)
        except StandardError as e:
            err(str(e))
        if lic.is_multi_tenancy_enabled():
            logger.info('Multi tenancy feature is enabled in License, setting up cgroups ...')
            cpu_pct = self.cfgs['cgroups_cpu_pct']
            mem_pct = self.cfgs['cgroups_mem_pct']

            if OS_UBUNTU not in self.platform and OS_KYLIN not in self.platform:
                service_cmd = '/sbin/service'
                chkconfig_cmd = '/sbin/chkconfig'
            else:
                service_cmd = '/usr/sbin/service'
                chkconfig_cmd = '/sbin/update-rc.d'

            if OS_UBUNTU in self.platform or OS_KYLIN in self.platform:
                if os.path.isfile('/etc/cgconfig.conf'):
                    logger.info('cgconfig.conf file exits ...')
                else:
                    logger.info('Generating cgconfig.conf file in /etc ...')
                    run_cmd('touch /etc/cgconfig.conf')

                if os.path.isfile('/etc/cgrules.conf'):
                    logger.info('cgrules.conf file exits ...')
                else:
                    logger.info('Generating cgrules.conf file in /etc ...')
                    run_cmd('touch /etc/cgrules.conf')

            # make sure cgroup service is running
            if OS_UBUNTU not in self.platform and OS_KYLIN not in self.platform:
                if cmd_output(service_cmd + ' cgconfig status') != 'Running':
                    run_cmd(service_cmd + ' cgconfig restart')
                    run_cmd(chkconfig_cmd + ' cgconfig on')

            # enable cgred if kernel version > 2.6.32-573.7.1
            kernel_ver = cmd_output('uname -r')
            if chk_kernel_ver(kernel_ver):
                logger.info('Enable cgred service ...')
                esgyn_cgrules = '%s cpu,memory %s/' % (self.traf_user, EDB_CGROUP_NAME)
                append_file(CGRULES_CONF_FILE, esgyn_cgrules)

                if OS_UBUNTU not in self.platform and OS_KYLIN not in self.platform:
                   if cmd_output(service_cmd + ' cgred status') != 'Running':
                      run_cmd(service_cmd + ' cgred restart')
		      run_cmd(chkconfig_cmd + ' cgred on')
                else:
                    if (cmd_output("ps -ef |grep  cgrulesengd |grep -v grep |awk '{print $8}'")) != 'cgrulesengd':
                        run_cmd('cgrulesengd --logfile=/var/log/cgrulesengd.log')
            else:
                logger.info('Kernel version is less than 2.6.32-573.7.1, cgred service is not enabled')

            if os.path.exists('%s/sql/scripts/edb_cgroup_cmd' % self.traf_home):
                result = cmd_output('source ~%s/.bashrc;%s/sql/scripts/edb_cgroup_cmd --add -u %s -g %s --pcgrp %s --cpu-pct \'%s\' --mem-pct \'%s\'' % (self.traf_user, self.traf_home, self.traf_user, self.traf_group, EDB_CGROUP_NAME, cpu_pct, mem_pct))
                logger.info(result)
        else:
             logger.info('Multi tenancy feature is not enabled in License, skipping ...')

    def setup_ha(self):
        if self.cfgs['enhance_ha'] == 'Y':
            # kernel settings
            logger.info('Setting up sysctl configs ...')
            ha_sysctl_items = {'net.ipv4.tcp_retries2': 'net.ipv4.tcp_retries2=5',
                               'net.ipv4.tcp_syn_retries': 'net.ipv4.tcp_syn_retries=3'}
            with open(SYSCTL_CONF_FILE, 'r') as f:
                lines = f.read()
            existed_items = [item for item in ha_sysctl_items.keys() if item in lines]
            new_items = set(ha_sysctl_items.keys()).difference(set(existed_items))
            for item in existed_items:
                mod_file(SYSCTL_CONF_FILE, {item + '=.*': ha_sysctl_items[item]})
            for item in new_items:
                append_file(SYSCTL_CONF_FILE, ha_sysctl_items[item])
            cmd_output('/sbin/sysctl -e -p /etc/sysctl.conf 2>&1 > /dev/null')

            # shutdown_HA.service
            logger.info('Creating shutdown_HA.service ...')
            net_interface = cmd_output('/sbin/ip route |grep default|awk \'{print $5}\'')
            shutdown_ha_script = '/var/local/shutdown_HA.sh'
            script_content = '''#!/bin/bash
ifconfig %s down
''' % net_interface
            write_file(shutdown_ha_script, script_content)
            run_cmd('chmod a+x %s' % shutdown_ha_script)

            shutdown_ha_srv = '/usr/lib/systemd/system/shutdown_HA.service'
            srv_content = '''[Unit]
Description=poweroff cust
After=getty@tty1.service display-manager.service plymouth-start.service
Before=systemd-poweroff.service systemd-reboot.service systemd-halt.service
DefaultDependencies=no

[Service]
ExecStart=%s
Type=forking

[Install]
WantedBy=poweroff.target
WantedBy=reboot.target
WantedBy=halt.target
''' % shutdown_ha_script
            write_file(shutdown_ha_srv, srv_content)

            if not os.path.exists('/usr/lib/systemd/system/poweroff.target.wants/shutdown_HA.service'):
                run_cmd('ln -s %s /usr/lib/systemd/system/poweroff.target.wants/' % shutdown_ha_srv)
            if not os.path.exists('/usr/lib/systemd/system/halt.target.wants/shutdown_HA.service'):
                run_cmd('ln -s %s /usr/lib/systemd/system/halt.target.wants/' % shutdown_ha_srv)
            if not os.path.exists('/usr/lib/systemd/system/reboot.target.wants/shutdown_HA.service'):
                run_cmd('ln -s %s /usr/lib/systemd/system/reboot.target.wants/' % shutdown_ha_srv)


# main
if __name__ == '__main__':
    exit(0)
