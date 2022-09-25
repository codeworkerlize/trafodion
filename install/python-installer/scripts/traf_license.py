#!/usr/bin/env python
### this script should be run on local node ###

import json
import base64
import sys
import logging
from constants import EDB_LIC_DIR, EDB_LIC_FILE, EDB_ID_FILE, TMP_LIC_FILE, TMP_ID_FILE, DEMO_LICENSE
from common import Remote, License, run_cmd, cmd_output, err_m, info, warn, get_sudo_prefix, set_stream_logger

logger = logging.getLogger()
set_stream_logger(logger)

def run(user, pwd):
    dbcfgs = json.loads(base64.b64decode(dbcfgs_json))

    nodes = dbcfgs['node_list'].split(',')
    license_str = dbcfgs['license_used']
    ssh_port = dbcfgs['ssh_port']

    try:
        lic = License(license_str)
        info('checking license file')
        lic.check_license(len(nodes))
    except StandardError as e:
        err_m(str(e))
    #get license type
    license_type = lic.get_license_type()

    remote = Remote(nodes[0], user=user, pwd=pwd, port=ssh_port)
    # consider pyinstaller can be run on any node which may not have HDFS access
    # so for now just check if esgyndb_id exists on local disks
    remote.execute('ls %s >/dev/null 2>&1' % EDB_ID_FILE, chkerr=False)
    if remote.rc != 0:
        lic.gen_id(TMP_ID_FILE)
        cl_id = 'generated'
        if lic.get_license_ver() > 1 and license_type == 'PRODUCT':
            err_m('License key not valid for new cluster ID. Contact Esgyn and send ID file: %s on node [%s].' % (EDB_ID_FILE, nodes[0]))
    else:
        cl_id = 'found'

    def copy_license_all():
        info('copying license/license_id file to all nodes')
        if lic.ispath:
            run_cmd('cp -rf %s %s' % (license_str, TMP_LIC_FILE), logger_output=False)
        else:
            with open(TMP_LIC_FILE, 'w') as f:
                f.write(license_str)

        files = [TMP_LIC_FILE]
        if cl_id == 'generated':
            files += [TMP_ID_FILE]
        copy_license(files)

    def copy_license_id():
        info('copying license_id file to all nodes')
        if cl_id == 'generated':
            files = [TMP_ID_FILE]
            copy_license(files)

    def copy_license(files):
        sudo_prefix = get_sudo_prefix()
        if user:
            if user == 'root':
                sudo_prefix = ''
            else:
                sudo_prefix = 'sudo -n'

        remotes = [Remote(node, user=user, pwd=pwd, port=ssh_port) for node in nodes]
        for remote in remotes:
            if TMP_LIC_FILE in files:
                remote.execute('%s rm -rf %s; %s mkdir -p %s; %s chmod 777 %s' % \
                    (sudo_prefix, EDB_LIC_FILE, sudo_prefix, EDB_LIC_DIR, sudo_prefix, EDB_LIC_DIR))
            else:
                remote.execute('%s mkdir -p %s; %s chmod 777 %s' % (sudo_prefix, EDB_LIC_DIR, sudo_prefix, EDB_LIC_DIR))
            remote.copy(files, remote_folder=EDB_LIC_DIR)
            if TMP_LIC_FILE in files:
                remote.execute('%s chmod a+r %s %s' % (sudo_prefix, EDB_ID_FILE, EDB_LIC_FILE))
            else:
                remote.execute('%s chmod a+r %s' % (sudo_prefix, EDB_ID_FILE))

        for f in files:
            run_cmd('rm -rf %s' % f, logger_output=False)

    if license_str == 'NONE':
        copy_license_id()
        if dbcfgs['req_license'].upper() == 'Y':
            warn('No license key -- a short grace period allowed to acquire a license.')
            warn('Contact Esgyn and send ID file: %s on node [%s].' % (EDB_ID_FILE, nodes[0]))
            info('Skip license check ...')
    elif license_str == DEMO_LICENSE:
        copy_license_all()
        warn('Demonstration license key -- a short grace period allowed to acquire a license.')
        warn('Contact Esgyn and send ID file: %s on node [%s].' % (EDB_ID_FILE, nodes[0]))
    else:
        copy_license_all()

# main
try:
    dbcfgs_json = sys.argv[1]
except IndexError:
    err_m('No db config found')

try:
    pwd = sys.argv[2]
except IndexError:
    user = pwd = ''

try:
    user = sys.argv[3]
except IndexError:
    user = ''

run(user, pwd)
