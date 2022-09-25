#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import sys
import json
import base64
import logging
from threading import Thread
from constants import NODE_EXPORTER, ESGYN_EXPORTER, NODE_EXPORTER_FILE, ESGYN_EXPORTER_FILE
from common import Remote, err, get_sudo_prefix, set_stream_logger

logger = logging.getLogger()

def run(user, pwd):
    """ copy exporter from local to all nodes
        and install them
    """
    def copy_file(remotes, files):
        threads = [Thread(target=r.copy, args=(files, '/tmp')) for r in remotes]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for r in remote_insts:
            if r.rc != 0: err('Failed to copy files to %s' % r.host)

    dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    exporter_nodes = dbcfgs['node_list'].split(',')
    remote_insts = [Remote(e, user=user, pwd=pwd) for e in exporter_nodes]

    sudo_prefix = get_sudo_prefix()

    # copy exporter to all nodes' /tmp folder
    exporter_files = [ESGYN_EXPORTER_FILE, NODE_EXPORTER_FILE]
    print 'Copy exporters to all nodes ...'
    copy_file(remote_insts, exporter_files)

    for remote in remote_insts:
        print 'Running exporters on node[%s] ...' % remote.host
        remote.execute('%s cp /tmp/%s /usr/bin/' % (sudo_prefix, NODE_EXPORTER))
        remote.execute('%s cp /tmp/%s /usr/bin/' % (sudo_prefix, ESGYN_EXPORTER))
        remote.execute('%s nohup /usr/bin/%s &' % (sudo_prefix, NODE_EXPORTER), shell=True)
        remote.execute('%s nohup /usr/bin/%s &' % (sudo_prefix, ESGYN_EXPORTER), shell=True)
        remote.execute('%s rm -f /tmp/%s' % (sudo_prefix, ESGYN_EXPORTER))

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')

    try:
        pwd = sys.argv[2]
    except IndexError:
        user = pwd = ''

    try:
        user = sys.argv[3]
    except IndexError:
        user = ''

    run(user, pwd)
