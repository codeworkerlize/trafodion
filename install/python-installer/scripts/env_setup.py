#!/usr/bin/env python
### this script should be run on all nodes with sudo user ###

import sys
import json
import base64
import logging
from collections import defaultdict
from Setup import Setup
from common import err, set_stream_logger

logger = logging.getLogger()

def run():
    """ create trafodion user, extract binary package, setup system configs """
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    setup = Setup(dbcfgs)

    ### precheck ###
    setup.pre_check()

    ### create trafodion user and group ###
    ### set up trafodion passwordless ssh ###
    setup.setup_user()

    ### untar traf package, package comes from copy_files.py ###
    ### copy trafodion bashrc ###
    setup.setup_package()

    ### only for elastic adding nodes ###
    ### set up sudoers/ulimits file for trafodion user ###
    setup.setup_misc()

    ### set aws configs in ~/.aws if on AWS EC2 environment ###
    setup.setup_aws()

    ### kernel settings ###
    ### copy init script ###
    ### create and set permission for scratch file dir ###
    setup.setup_system()

    ### keepalive config ###
    setup.setup_keepalive()

    ### enhance HA ###
    setup.setup_ha()

    ### clean up ###
    setup.clean_up()

# main
if __name__ == '__main__':
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
