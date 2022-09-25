#!/usr/bin/env python
import os
import sys
import glob
import json
import socket
import Hadoop
import base64
import logging
from Setup import Setup
from collections import defaultdict
from gen_sshkey import del_key_file
from constants import TMP_SSH_DIR, TEMPLATE_XML, ULIMITS_DIR, TRAF_SUDOER_FILE, DEF_TRAF_HOME, \
                      SUDOER_FILE, TRAF_SUDOER_DIR, TMP_SSHKEY_FILE
from common import err, set_stream_logger, append_file, run_cmd, gen_template_file, ParseXML, cmd_output, \
                   mod_file, write_file

logger = logging.getLogger()


def create_hadoop_user():
    hadoop_user = 'hadoop'
    home_dir = '/var/lib/hadoop'

    # create user
    Hadoop.create_hadoop_user(hadoop_user, home_dir)

    run_cmd('mkdir -p %s/.ssh' % home_dir)
    run_cmd('cp %s/* %s/.ssh' % (TMP_SSH_DIR, home_dir))
    run_cmd('chmod 700 %s/.ssh' % home_dir)
    run_cmd('chown -R %s:%s %s' % (hadoop_user, hadoop_user, home_dir))
    run_cmd('chmod 700 %s' % home_dir)
    del_key_file()

    p = ParseXML(TEMPLATE_XML)
    ulimits_config = p.get_property('ulimits_template')
    ulimits_file = ULIMITS_DIR + '/%s.conf'
    gen_template_file(ulimits_config, ulimits_file % hadoop_user, {'user': hadoop_user, 'nofile': '131072', 'nproc': '65536'})


def create_traf_user(dbcfgs):
    setup = Setup(dbcfgs)

    ### create trafodion user and group ###
    ### set up trafodion passwordless ssh ###
    setup.setup_user()

    traf_user_dir = '%s/%s' % (dbcfgs['home_dir'], dbcfgs['traf_user'])
    run_cmd('chown -R %s:%s %s' % (setup.traf_user, setup.traf_group, traf_user_dir))

    if dbcfgs['distro'] == 'CDH_APACHE':
        p = ParseXML(TEMPLATE_XML)
        sudoer_config = p.get_property('cdh_trafodion_sudoers_template')
        run_cmd('mkdir -p %s' % TRAF_SUDOER_DIR)
        gen_template_file(sudoer_config, TRAF_SUDOER_FILE, {'traf_user': dbcfgs['traf_user'], 'hbase_user': dbcfgs['hbase_user'],
                                                            'hbase_home': dbcfgs['hbase_home'], 'default_traf_home': DEF_TRAF_HOME,
                                                            'hadoop_home': dbcfgs['hadoop_home'], 'zk_home': dbcfgs['zookeeper_home'],
                                                            'hive_home': dbcfgs['hive_home']})

    # modify /etc/sudoers
    output = cmd_output('cat %s | grep \'#includedir /etc/sudoers.d\'' % SUDOER_FILE)
    if not output:
        run_cmd('echo "#includedir /etc/sudoers.d" >> %s' % SUDOER_FILE)

    # modify /etc/chrony.conf
    if os.path.exists('/etc/chrony.conf'):
        append_file('/etc/chrony.conf', 'allow all', '# Allow NTP client access from local network.')

    # modify /etc/cron.allow
    if os.path.exists('/etc/cron.allow'):
        append_file('/etc/cron.allow', dbcfgs['traf_user'])
    else:
        write_file('/etc/cron.allow', dbcfgs['traf_user'])
    run_cmd('chmod 600 %s' % '/etc/cron.allow')

    if dbcfgs['enhance_ha'] == 'Y':
        setup.setup_ha()

    # cleanup
    run_cmd('rm -rf %s{,.pub}' % TMP_SSHKEY_FILE)


def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    # modify umask
    mod_file('/etc/bashrc', {'umask 0077': ''})
    run_cmd('source /etc/bashrc')

    if dbcfgs['install_qian'] == 'N':
        create_hadoop_user()
    create_traf_user(dbcfgs)


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')

    run()
