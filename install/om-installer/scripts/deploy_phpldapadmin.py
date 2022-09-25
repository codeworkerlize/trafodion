#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import re
import sys
import json
import base64
import logging
from collections import defaultdict
from common import run_cmd, err, set_stream_logger, mod_file

logger = logging.getLogger()
PHPLDAPADMIN_DIR = '/var/www/html/phpldapadmin'

def run(dbcfgs_json):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    phpldapadmin_zip = dbcfgs['phpldapadmin_zip'].split('/')[-1]
    phpldapadmin_name = re.match(r'(.*)\.zip', phpldapadmin_zip).group(1)

    # install phpLDAPadmin
    logger.info('Installing phpLDAPadmin ...')
    run_cmd('unzip /tmp/%s -d /tmp' % phpldapadmin_zip)
    run_cmd('cp -rf /tmp/%s %s' % (phpldapadmin_name, PHPLDAPADMIN_DIR))


    # config phpLDAPadmin
    logger.info('Modify phpLDAPadmin config file ...')
    run_cmd('cp %s/config/config.php.example %s/config/config.php' % (PHPLDAPADMIN_DIR, PHPLDAPADMIN_DIR))
    mod_file('%s/config/config.php' % PHPLDAPADMIN_DIR,
             {"// $servers->setValue('login','attr','dn')": "$servers->setValue('login','attr','dn')"})

    # restart phpLDAPadmin
    logger.info('Restarting httpd ...')
    run_cmd('service httpd restart')

    # clean up
    run_cmd('rm -rf /tmp/%s /tmp/%s' % (phpldapadmin_zip, phpldapadmin_name))


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run(dbcfgs_json)
