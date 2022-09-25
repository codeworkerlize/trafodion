#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import re
import os
import sys
import glob
import json
import socket
import base64
import logging
from collections import defaultdict
from constants import LDAP_DB_CONFIG, LDAP_DB_CONFIG_EXAMPLE, LDAP_DB_CONFIG_DIR, LDAP_CONFIG_DIR, TEMPLATE_XML, \
                      SCRIPTS_DIR
from common import run_cmd, cmd_output, err, set_stream_logger, ParseXML, gen_template_file, get_host_ip, \
                   CentralConfig, append_file

logger = logging.getLogger()

def run(dbcfgs_json):
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))

    if dbcfgs['ldap_security'].upper() == 'N':
        return

    ldap_configs = defaultdict(str)

    template = ParseXML(TEMPLATE_XML)
    ldap_rootpw = dbcfgs['ldap_rootpw']
    ldap_nodes = dbcfgs['ldap_nodes'].split(',')
    host_ip = get_host_ip(socket)
    is_ldap_node = [n for n in ldap_nodes if n == host_ip]

    ldap_changes_ldif = '%s/changes.ldif' % LDAP_CONFIG_DIR
    ldap_ou_ldif = '%s/ou.ldif' % LDAP_CONFIG_DIR
    user_ldif = '%s/users.ldif' % LDAP_CONFIG_DIR
    mod_syncprov_ldif = '%s/mod_syncprov.ldif' % LDAP_CONFIG_DIR
    syncprov_ldif = '%s/syncprov.ldif' % LDAP_CONFIG_DIR
    ldap_ha_ldif = '%s/ldapha.ldif' % LDAP_CONFIG_DIR

    # install ldap
    def install_ldap():
        all_pkg_list = cmd_output('rpm -qa')
        install_pkg_list = []
        req_pkg_list = CentralConfig().get('packages_ldap')
        for pkg in req_pkg_list:
            if pkg + '-' in all_pkg_list:
                logger.info('Package %s had already been installed' % pkg)
            else:
                install_pkg_list.append(pkg)
        if install_pkg_list:
            run_cmd('yum install -y %s' % ' '.join(install_pkg_list))

    # config ldap
    def config_ldap():
        # configuring ldap
        logger.info('Configuring LDAP ...')
        run_cmd('cp %s %s' % (LDAP_DB_CONFIG_EXAMPLE, LDAP_DB_CONFIG))
        run_cmd('chown -R ldap:ldap %s' % LDAP_DB_CONFIG_DIR)
        run_cmd('chown -R ldap:ldap %s' % LDAP_CONFIG_DIR)

        # starting ldap service
        run_cmd('service slapd start && chkconfig slapd on')

        ldap_configs['ldap_rootpw'] = cmd_output('slappasswd -s %s' % ldap_rootpw)
        ldap_configs['ldap_dn'] = '%s,cn=config' % config_file
        ldap_configs['rootpw_mode'] = 'replace' if 'olcRootPW' in config_content else 'add'
        ldap_configs['access_mode'] = 'replace' if 'olcAccess' in config_content else 'add'
        gen_template_file(template.get_property('ldap_changes_template'), ldap_changes_ldif, ldap_configs)
        run_cmd('ldapmodify -Y EXTERNAL -H ldapi:/// -f %s/changes.ldif' % LDAP_CONFIG_DIR)

        # modify max open files
        service_file = '/usr/lib/systemd/system/slapd.service'
        append_file(service_file, 'LimitNOFILE=65536', '[Install]', True)
        run_cmd('systemctl daemon-reload')
        run_cmd('service slapd restart')

        # check config
        check_result = cmd_output('slaptest')
        if 'succeeded' not in check_result: err('LDAP configuration has error')

    def ldap_ha(server_id, provider):
        logger.info('Configuring LDAP HA ...')
        ldapadd_cmd = 'ldapadd -Y EXTERNAL -H ldapi:/// -f %s -w %s'
        module_file = glob.glob('%s/slapd.d/cn=config/cn=module*' % LDAP_CONFIG_DIR)
        if module_file:
            run_cmd('rm -f %s' % module_file[0])
        if os.path.exists('%s/slapd.d/cn=config/%s' % (LDAP_CONFIG_DIR, config_file)):
            run_cmd('rm -rf %s/slapd.d/cn=config/%s' % (LDAP_CONFIG_DIR, config_file))
        run_cmd('service slapd restart')

        gen_template_file(template.get_property('ldap_mod_syncprov'), mod_syncprov_ldif, ldap_configs)
        run_cmd(ldapadd_cmd % (mod_syncprov_ldif, ldap_rootpw))

        gen_template_file(template.get_property('ldap_syncprov'), syncprov_ldif, ldap_configs)
        run_cmd(ldapadd_cmd % (syncprov_ldif, ldap_rootpw))

        ldap_configs['server_id'] = server_id
        ldap_configs['ldap_provider'] = provider
        ldap_configs['ldap_rootpw'] = ldap_rootpw
        ldap_configs['mirror_mode'] = 'replace' if 'olcMirrorMode' in config_content else 'add'
        ldap_configs['uuid_mode'] = 'replace' if 'olcDbIndex: entryUUID eq' in config_content else 'add'
        ldap_configs['csn_mode'] = 'replace' if 'olcDbIndex: entryCSN eq' in config_content else 'add'
        gen_template_file(template.get_property('ldap_ha'), ldap_ha_ldif, ldap_configs)
        run_cmd('ldapmodify -Y EXTERNAL -H ldapi:/// -f %s -w %s' % (ldap_ha_ldif, ldap_rootpw))

    if is_ldap_node:
        logger.info('Install LDAP ...')
        install_ldap()

        config_file = glob.glob('%s/slapd.d/cn=config/olcDatabase=*db.ldif' % LDAP_CONFIG_DIR)[0]
        config_content = cmd_output('cat %s' % config_file)
        config_file = re.match(r'.*/(olcDatabase=.*)\.ldif', config_file).group(1)

        logger.info('Deploy LDAP ...')
        config_ldap()

        # load ldap schemas
        logger.info('Loading LDAP schemas ...')
        cmd_output('%s/load_schema.sh' % SCRIPTS_DIR)

        # create LDAP group
        logger.info('Creating LDAP group ...')
        ldapadd_cmd = "ldapadd -x -D 'cn=Manager,dc=esgyn,dc=local' -f %s -w %s"
        ldapsearch_info = cmd_output('ldapsearch -x -H ldapi:/// -b dc=esgyn,dc=local')
        if 'dn: ou=Users,dc=esgyn,dc=local' not in ldapsearch_info:
            gen_template_file(template.get_property('ldap_ou_template'), ldap_ou_ldif, ldap_configs)
            run_cmd(ldapadd_cmd % (ldap_ou_ldif, ldap_rootpw))
        else:
            logger.info('Organization "ou=Users,dc=esgyn,dc=local" has been existed')

        # create LDAP user
        logger.info('Creating LDAP users ...')
        if 'dn: uid=trafodion,ou=Users,dc=esgyn,dc=local' in ldapsearch_info:
            run_cmd('ldapdelete -x -H ldapi:/// -D "cn=Manager,dc=esgyn,dc=local" "uid=trafodion,ou=Users,dc=esgyn,dc=local" -w %s' % ldap_rootpw)
        if 'dn: uid=admin,ou=Users,dc=esgyn,dc=local' in ldapsearch_info:
            run_cmd('ldapdelete -x -H ldapi:/// -D "cn=Manager,dc=esgyn,dc=local" "uid=admin,ou=Users,dc=esgyn,dc=local" -w %s' % ldap_rootpw)
        ldap_configs['db_root_pwd'] = cmd_output('slappasswd -s %s' % dbcfgs['db_root_pwd'])
        ldap_configs['db_admin_pwd'] = cmd_output('slappasswd -s %s' % dbcfgs['db_admin_pwd'])
        gen_template_file(template.get_property('ldap_users_template'), user_ldif, ldap_configs)
        run_cmd(ldapadd_cmd % (user_ldif, ldap_rootpw))

        # enable ha for ldap
        if dbcfgs['enable_ldap_ha'].upper() == 'Y':
            if host_ip == dbcfgs['ldap_node_ip']:
                ldap_ha('1', dbcfgs['ldap_ha_node_ip'])
            else:
                ldap_ha('2', dbcfgs['ldap_node_ip'])

        # clean up
        run_cmd('rm -f %s %s %s %s %s %s' % (ldap_changes_ldif, ldap_ou_ldif, user_ldif, mod_syncprov_ldif, syncprov_ldif, ldap_ha_ldif))


# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run(dbcfgs_json)