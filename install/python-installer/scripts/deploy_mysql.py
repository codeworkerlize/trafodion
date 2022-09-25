#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

## This script should be run on all nodes with sudo user ##

import os
import re
import sys
import json
import time
import base64
import socket
import logging
from collections import defaultdict
from constants import *
from common import run_cmd, run_cmd2, cmd_output, err, ParseInI, set_stream_logger, \
                   run_cmd3, get_host_ip, append_file, mod_file

logger = logging.getLogger()
MYSQL_DIR = '/usr/local/mysql'
MYSQL_DATADIR = '/usr/local/mysql/data'
MY_CNF_FILE = '/etc/my.cnf'

def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    mysql_yum = True if not dbcfgs['mysql_dir'] else False

    start_cmd = 'service mysqld start'
    stop_cmd = 'service mysqld stop'

    local_host = socket.getfqdn()
    host_ip = get_host_ip(socket)
    mysql_hosts = dbcfgs['mysql_hosts'].split(',')
    root_passwd = dbcfgs['root_passwd']
    repl_passwd = dbcfgs['repl_passwd']
    root_exec_cmd = '%s/bin/mysql -uroot -p%s -e' % (MYSQL_DIR, root_passwd)

    def install_mysql():
        logger.info("Removing mariadb ...")
        mariadb = cmd_output('rpm -qa | grep mariadb')
        if mariadb: run_cmd('rpm -qa | grep mariadb | xargs -L 1 rpm -e --nodeps')

        if mysql_yum:
            logger.info('Installing mysql by yum ')
            rc, stderr = run_cmd3('yum install -y mysql-server')
            if rc: err('Failed to install mysql: %s' % stderr)
        else:
            logger.info('Installing mysql by binary package ...')
            # create mysql group
            if cmd_output('getent passwd mysql'):
                logger.info('Mysql user already exists, skip creating mysql user')
            else:
                logger.info('Creating mysql user and group...')
                if not cmd_output('getent group mysql'):
                    run_cmd('/usr/sbin/groupadd mysql')
                # create mysql user
                if not cmd_output('getent passwd mysql'):
                    run_cmd('/usr/sbin/useradd -s /sbin/nologin -M -g mysql mysql')

            logger.info('Extracting mysql binary package ...')
            mysql_pkg = re.search(r'(.+).tar.gz', dbcfgs['mysql_dir']).group(1)
            if not os.path.exists(MYSQL_DIR):
                run_cmd('tar -xf /tmp/%s -C /usr/local' % dbcfgs['mysql_dir'])
                run_cmd('ln -s /usr/local/%s %s' % (mysql_pkg, MYSQL_DIR))
                run_cmd('cp %s/support-files/my-default.cnf %s' % (MYSQL_DIR, MY_CNF_FILE))

            # initialize mysql
            logger.info('Initializinf mysql database ...')
            run_cmd('chown -R mysql:mysql %s' % MYSQL_DIR)
            cmd_output('%s' % stop_cmd)
            run_cmd2('yum install -y autoconf libaio')
            ParseInI(MY_CNF_FILE, 'mysqld').save({"basedir": MYSQL_DIR, "datadir": MYSQL_DATADIR,
                                                  "pid_file": "%s/mysql.pid" % MYSQL_DATADIR,
                                                  "log-error": "%s/error.log" % MYSQL_DATADIR})

            init_option = '--defaults-file=%s --basedir=%s --datadir=%s --user=mysql' % (MY_CNF_FILE, MYSQL_DIR, MYSQL_DATADIR)
            cmd_output('%s/scripts/mysql_install_db %s' % (MYSQL_DIR, init_option))

            # set mysql environment variable
            logger.info('Setting mysql environment variable ...')
            run_cmd('cp %s/support-files/mysql.server /etc/init.d/mysqld' % MYSQL_DIR)
            run_cmd('chmod 755 /etc/init.d/mysqld')
            append_file('/etc/profile', 'PATH=%s/bin:$PATH' % MYSQL_DIR)
            run_cmd('source /etc/profile')

    # main process
    if local_host in mysql_hosts:
        install_mysql()

        logger.info('Setting mysql to start automatically at boot ...')
        run_cmd('%s' % start_cmd)
        if 'running' not in cmd_output('service mysqld status'):
            run_cmd('%s' % start_cmd)
        run_cmd('chkconfig --add mysqld')
        run_cmd('chkconfig mysqld on')
        # set password for root
        try:
            cmd_output('%s/bin/mysqladmin -u root password "%s"' % (MYSQL_DIR, root_passwd))
        except:
            logger.info('Mysql root user\'s password had already been set')

        grant_root_cmd = "grant all on *.* to root@'%s' identified by '%s'"
        run_cmd('%s "%s"' % (root_exec_cmd, grant_root_cmd % (host_ip, root_passwd)))
        run_cmd('%s "%s"' % (root_exec_cmd, grant_root_cmd % (local_host, root_passwd)))
        run_cmd('%s' % stop_cmd)

        # set /etc/my.cnf
        logger.info('Modifying configuration file /etc/my.cnf ...')
        mysqld_content = ParseInI(MY_CNF_FILE, 'mysqld').load()
        mysql_ha = json.loads(dbcfgs['mysql_ha'])
        mysqld_content.update(mysql_ha)
        if local_host in dbcfgs['first_node'] or dbcfgs['first_node'] in local_host:
            mysqld_content['server-id'] = 1
            mysqld_content['auto_increment_offset'] = 0
        else:
            mysqld_content['server-id'] = 2
            mysqld_content['auto_increment_offset'] = 1
        ParseInI(MY_CNF_FILE, 'mysqld').rewrite(mysqld_content)
        mod_file(MY_CNF_FILE, {'log-slave-updates.*': 'log-slave-updates'})
        run_cmd('%s' % start_cmd)

        # create repl user
        logger.info('Creating mysql user for backup ...')
        grant_repl_cmd = "grant all privileges on *.* to repl@'%s' identified by '%s'"
        if local_host in dbcfgs['first_node'] or dbcfgs['first_node'] in local_host:
            mysql_hosts.remove(local_host)
            repl_ip = ','.join(mysql_hosts)
        else:
            repl_ip = dbcfgs['first_node']
        run_cmd('%s "%s"' % (root_exec_cmd, grant_repl_cmd % (repl_ip, repl_passwd)))

        # set master info
        master_status = cmd_output("%s/bin/mysql -urepl -p%s -h '%s' -e 'show master status' | grep 'bin\.'" % (MYSQL_DIR, repl_passwd, repl_ip)).split()
        for i in range(20):
            if 'ERROR' in master_status:
                time.sleep(10)
                master_status = cmd_output("%s/bin/mysql -urepl -p%s -h '%s' -e 'show master status' | grep 'bin\.'" % (MYSQL_DIR, repl_passwd, repl_ip)).split()
            else:
                break

        log_file, pos = master_status
        change_master = 'CHANGE MASTER TO MASTER_HOST = "%s",MASTER_USER = "repl", MASTER_PASSWORD = "%s",MASTER_LOG_FILE="%s", MASTER_LOG_POS = %s' % (repl_ip, repl_passwd, log_file, pos)
        run_cmd('%s "stop slave"' % root_exec_cmd)
        run_cmd('%s \'%s\'' % (root_exec_cmd, change_master))
        run_cmd('%s "start slave"' % root_exec_cmd)

        # check status
        if 'No' in cmd_output("%s 'show slave status\G'|sed -n '12,13p'" % root_exec_cmd):
            err('Set mysql backup error ...\nCheck log file %s/error.log for details.' % MYSQL_DATADIR)

        logger.info('Mysql installed and configure successfully!')


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
