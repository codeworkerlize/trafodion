#!/usr/bin/env python
### this script should be run on all nodes with sudo user ###

import sys
import base64
import json
import logging
from common import err, set_stream_logger
from Setup import Setup

logger = logging.getLogger()

def run(dbcfgs):
    setup = Setup(dbcfgs)
    setup.setup_cgroups()

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
        dbcfgs = json.loads(base64.b64decode(dbcfgs_json))
    except IndexError:
        err('No db config found')
    run(dbcfgs)
