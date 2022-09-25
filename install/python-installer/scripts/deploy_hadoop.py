#!/usr/bin/env python
import sys
import json
import Hadoop
import base64
import logging
from collections import defaultdict
from constants import TMP_HADOOP_DIR
from common import err, set_stream_logger, run_cmd

logger = logging.getLogger()


def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    services = ['zookeeper', 'hadoop', 'yarn', 'hbase', 'hive']
    hadoop_factory = [Hadoop.install_cdh(srv, dbcfgs) for srv in services]
    for h in hadoop_factory: h.install()
    # for h in hadoop_factory: h.start()

    # clean file
    run_cmd('rm -rf %s' % TMP_HADOOP_DIR)


# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')

    run()
