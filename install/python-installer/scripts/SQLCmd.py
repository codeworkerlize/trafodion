#!/usr/bin/env python

import subprocess
import logging
from common import get_sudo_prefix

logger = logging.getLogger()

class SQLException(Exception):
    pass

class SQLCmd(object):
    """ execute SQL statements using sqlci """
    def __init__(self, traf_user=''):
        self.traf_user = traf_user

    def _sqlci(self, cmd):
        if self.traf_user:
            sqlci_cmd = '%s su - %s -c \'echo "%s" | sqlci\'' % (get_sudo_prefix(), self.traf_user, cmd)
        else:
            sqlci_cmd = 'echo "%s" | sqlci' % cmd
        p = subprocess.Popen(sqlci_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True,executable='/bin/bash')
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            msg = stderr if stderr else stdout
            raise SQLException('Failed to run sqlci command: %s' % msg)
        logger.info(stdout)
        return stdout

    def drop_traf(self):
        output = self._sqlci('initialize trafodion, drop;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize trafodion, drop:\n %s' % output)

    def init_traf(self):
        output = self._sqlci('initialize trafodion;')
        # error 1392, 1395
        if 'Error' in output:
            if '1392' in output or '1395' in output:
                meta_current = self._sqlci('get version of metadata;')
                if not 'Metadata is current' in meta_current:
                    self.upgrade_traf()
            # other errors
            else:
                raise SQLException('Failed to initialize trafodion:\n %s' % output)

    def upgrade_traf(self):
        output = self._sqlci('initialize trafodion, upgrade;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize trafodion, upgrade:\n %s' % output)

    def create_xdc(self):
        output = self._sqlci('initialize trafodion, create xdc metadata;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize trafodion, create xdc metadata:\n %s' % output)

    def add_tenant(self):
        output = self._sqlci('initialize trafodion, add tenant usage;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize trafodion, add tenant usage:\n %s' % output)

    def upgrade_libmgmt(self):
        output = self._sqlci('initialize trafodion, upgrade library management;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize trafodion, upgrade library management:\n %s' % output)

    def create_lob_metadata(self):
        output = self._sqlci('initialize trafodion, create lob metadata;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize trafodion, create lob metadata:\n %s' % output)

    def cleanup_user_querycache(self):
        output = self._sqlci('generate user querycache cleanup;')
        if 'ERROR' in output:
            raise SQLException('Failed to cleanup user querycache:\n %s' % output)

    def init_auth(self, db_root_user, db_admin_user=''):
        output = self._sqlci('alter user DB__ROOT set external name \\\"%s\\\";' % db_root_user)
        if 'ERROR' in output:
            raise SQLException('Failed to setup security for trafodion:\n %s' % output)
        if db_admin_user:
            output = self._sqlci('alter user DB__ADMIN set external name \\\"%s\\\";' % db_admin_user)
            if 'ERROR' in output:
                raise SQLException('Failed to set external name for DB__ADMIN:\n %s' % output)

    def drop_auth(self):
        output = self._sqlci('initialize authorization, drop;')
        if 'ERROR' in output:
            raise SQLException('Failed to drop authorization for trafodion:\n %s' % output)

    def obey_cqd(self, sql_file):
        output = self._sqlci('obey %s;' % sql_file)
        if 'Error' in output:
            if '8102' in output:
                logger.info('CQD existed ...')

    def create_partition_tables(self):
        """enable partition tables"""
        output = self._sqlci('initialize trafodion, create partition tables;')
        if 'ERROR' in output:
            raise SQLException('failed to initialize partition tables')

    def drop_partition_tables(self):
        """disable partition tables"""
        output = self._sqlci('initialize trafodion, drop partition tables;')
        if 'ERROR' in output:
            raise SQLException('failed to drop partition tables')

    def init_authz(self):
        output = self._sqlci('initialize authorization;')
        if 'ERROR' in output:
            raise SQLException('Failed to initialize authorization for trafodion:\n %s' % output)

    def check_auth(self):
        #ignore error
        output = self._sqlci('initialize authentication;')
        if 'ERROR' in output:
            raise SQLException(
                'Failed to initialize authentication for trafodion:\n %s' % output)

    def change_admin_pwd(self,db_admin_pwd):
        output = self._sqlci('alter user DB__ROOT identified by \'%s\';' % db_admin_pwd)
        if 'ERROR' in output:
            raise SQLException('Failed to change password for DB__ROOT:\n %s' % output)
        output = self._sqlci('alter user DB__ADMIN identified by \'%s\';' % db_admin_pwd)
        if 'ERROR' in output:
            raise SQLException('Failed to change password for DB__ADMIN:\n %s' % output)

