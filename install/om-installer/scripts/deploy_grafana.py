#!/usr/bin/python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import os
import sys
import glob
import json
import base64
import socket
import logging
import httplib2
import ConfigParser
from collections import defaultdict
from constants import CONFIG_DIR, TMP_GRAFANA_PLUGS
from common import set_stream_logger, err, run_cmd, get_host_ip, append_file, mod_file

logger = logging.getLogger()
GRA_CONFILE = "/etc/grafana/grafana.ini"
MESSAGR_PORT = 8088


class Grafana(object):
    def __init__(self, ip, dbcfgs):
        self.h = httplib2.Http()
        self.admin_user = dbcfgs['grafana_admin_user']
        self.admin_psword = dbcfgs['grafana_admin_pwd']
        self.mgr_user = dbcfgs['mgr_user']
        self.mgr_pwd = dbcfgs['mgr_pwd']
        self.prometheus_url = 'http://%s:9090' % dbcfgs['management_node']
        self.cloudera_url = 'http://%s:7180' % dbcfgs['management_node']
        self.opentsdb_url = 'http://%s:5242' % dbcfgs['first_node']
        self.url = 'http://%s' % ip
        self.auth = base64.encodestring('%s:%s' % (self.admin_user, self.admin_psword))
        self.headers = {"Content-Type": 'application/json',
                        "Accept": 'application/json',
                        "Authorization": 'Basic %s' % self.auth}
        self.receive_mailbox = ';'.join(dbcfgs['receive_mailbox'].split(','))

    def switch_request(self, mode, api, data=""):
        url = self.url + api
        data = json.dumps(data)
        switcher = {
            "get": self.h.request(url, 'GET', headers=self.headers),
            "put": self.h.request(url, 'PUT', body=data, headers=self.headers),
            "post": self.h.request(url, 'POST', body=data, headers=self.headers),
            "patch": self.h.request(url, 'PATCH', body=data, headers=self.headers)
        }
        return switcher.get(mode, "Nothing")

    def set_admin_psw(self, new_admin_psword):
        psw_api = ':3000/api/user/password'
        data = {"oldPassword": self.admin_psword, "newPassword": new_admin_psword, "confirNew": new_admin_psword}
        put_res, put_content = self.switch_request("put", psw_api, data)
        if put_res['status'] == '200' or put_res['status'] == '401':
            logger.info("Admin password Updated!  %s %s" % (put_res['status'], put_content))
            self.admin_psword = new_admin_psword
            self.auth = base64.encodestring('%s:%s' % (self.admin_user, self.admin_psword))
            self.headers = {"Content-Type": 'application/json',
                            "Accept": 'application/json',
                            "Authorization": 'Basic %s' % self.auth}
        elif put_res['status'] == '400':
            err('New password is too short')
        else:
            err("Password Update Error %s %s" % (put_res['status'], put_content))

    def set_editor(self, editor, editor_psword, email):
        editor_api = ':3000/api/admin/users'
        data = {"name": editor, "email": email, "login": editor, "password": editor_psword}
        editor_res, editor_content = self.switch_request("post", editor_api, data)
        if editor_res['status'] == '200':
            editor_id = json.loads(editor_content)["id"]
            org_api = ':3000/api/org/users/' + str(editor_id)
            data = {"role": "Editor"}
            self.switch_request("patch", org_api, data)
            logger.info("Editor created successfully!  %s %s" % (editor_res['status'], editor_content))
        elif editor_res['status'] == '500':
            logger.info("This editor has been created.\nSkip create this editor.")
        else:
            err("Editor created Error %s %s" % (editor_res['status'], editor_content))

    def notify_channels(self, types, uid, name, webhook_url=''):
        noti_api = ':3000/api/alert-notifications'
        if types == 'email':
            data = {"uid": uid, "sendReminder": False, "type": types, "name": name, "isDefault": False, "settings": {"addresses": self.receive_mailbox}}
        elif types == 'webhook':
            data = {"uid": uid, "name": name, "type": types, "isDefault": False, "sendReminder": False,
                    "disableResolveMessage": False,
                    "frequency": "",
                    "settings": {"autoResolve": True, "httpMethod": "POST", "uploadImage": True, "url": webhook_url}}
        response, content = self.switch_request("post", noti_api, data)
        if response['status'] == '200':
            logger.info("Alert notification import successfully!")
        elif response['status'] == '500':
            logger.info("This notifiction has been existed.\nSkip import this notifiction.")
        else:
            err("Alert Notification Import Error %s %s" % (response['status'], content))

    def templet_import(self, mode, ds_name):  # import dashbord or datasource
        title = ds_name + ' ' + mode
        if mode == 'dashboard':
            get_api = ':3000/api/dashboards/uid/%s' % ds_name.lower()
            imp_url = self.url + ':3000/api/dashboards/db'
        elif mode == 'datasource':
            get_api = ':3000/api/datasources/name/%s' % ds_name
            imp_url = self.url + ':3000/api/datasources'
        logger.info("Check %s" % title)
        check_res, check_content = self.switch_request("get", get_api)
        logger.info("%s %s" % (check_res['status'], check_content))
        if check_res['status'] == '200':
            logger.info("This %s has been existed.\nSkip import this %s." % (title, title))
        elif check_res['status'] == '404':
            logger.info("%s dosen't exist." % title)
            logger.info("Start importing %s..." % title)
            if ds_name == 'Cloudera%20Manager': ds_name = 'cloudera'
            with open('%s/%s_%s.json' % (CONFIG_DIR, ds_name.lower(), mode), 'rb') as f:
                data = f.read()
            data = json.loads(data)
            if ds_name.lower() == "prometheus":
                data['url'] = self.prometheus_url
            elif ds_name == "cloudera":
                data['url'] = self.cloudera_url
                data['basicAuthUser'] = self.mgr_user
                data['basicAuthPassword'] = self.mgr_pwd
            elif ds_name.lower() == "opentsdb":
                data['url'] = self.opentsdb_url
            data = json.dumps(data)
            response, content = self.h.request(imp_url, 'POST', body=data, headers=self.headers)
            logger.info("%s %s" % (response['status'], content))
            if response['status'] == '200':
                logger.info("%s import successfully!" % title)
            else:
                err("%s Import Error %s %s" % (mode, response['status'], content))
        elif check_res['status'] != '200' and check_res['status'] != '404':
            err("Check error %s %s" % (check_res['status'], check_content))

    def start_db(self):
        search_api = ':3000/api/search'
        search_res, search_content = self.switch_request("get", search_api)
        data = json.loads(search_content)
        for d in data:
            if d["uid"] == "esgyndb":
                db_id = d["id"]
                break
        start_api = ':3000/api/user/stars/dashboard/' + str(db_id)
        start_res, start_content = self.switch_request("post", start_api)
        if start_res['status'] == '200':
            logger.info("%s %s" % (start_res['status'], start_content))
        elif start_res['status'] == '500':
            logger.info("%s %s" % (start_res['status'], start_content))
        else:
            err("Dashboard Start Error %s %s" % (start_res['status'], start_content))

    def set_smtp(self, sendmail, smtp_host, smtp_psword):
        data = """enabled=true                                                                                                                                              
host=%s
user=%s
password=%s
from_address=%s
from_name = Grafana
""" % (smtp_host, sendmail, smtp_psword, sendmail)
        logger.info("Modifying grafana.ini ...")
        if os.path.exists(GRA_CONFILE):
            confile = open(GRA_CONFILE).readlines()
            lines = []
            config = ConfigParser.ConfigParser()
            config.read(GRA_CONFILE)
            item = config.items('smtp')
            for i in range(len(confile)):
                lines.append(confile[i])
                if "[smtp]" in confile[i]:
                    linenum = i
            if item:
                logger.info("Skip set grafana.ini. It has been changed.")
            elif not item:
                lines.insert(linenum+1, data)
                s = ''.join(lines)
                with open(GRA_CONFILE, 'w') as confile:
                    confile.write(s)
                run_cmd('service grafana-server restart')
                logger.info("Modify Grafana.ini Complete")
        else:
            err("%s doesn't exist" % GRA_CONFILE)


def run():
    dbcfgs = defaultdict(str, json.loads(base64.b64decode(dbcfgs_json)))
    host_ip = get_host_ip(socket)

    # Install Grafana
    if host_ip in dbcfgs['management_node']:
        # start grafana
        logger.info('Starting Grafana server...')
        run_cmd('service grafana-server start')

        # Grafana plugins
        logger.info('Installing Grafana plugins ...')
        plugins = glob.glob('%s/*' % TMP_GRAFANA_PLUGS)
        for plug in plugins:
            run_cmd('cp -r %s /var/lib/grafana/plugins' % plug)

        # modify index.html
        index_file = '/usr/share/grafana/public/views/index.html'
        content = '''<script>
$(document).keydown(function(event){
 if(event.keyCode == 27){
    event.stopImmediatePropagation();
    return false;
 }
});
</script>
        '''
        append_file(index_file, '    .navbar-buttons--tv{display:none}', '</style>', upper=True)
        append_file(index_file, content, '</html>', upper=True)

        # modify grafana.ini
        ini_file = '/etc/grafana/grafana.ini'
        mod_file(ini_file, {';default_theme = .*': 'default_theme = light',
                            ';allow_embedding = .*': 'allow_embedding = true'})
        append_file(ini_file, 'enabled = true', '[auth.anonymous]')

        modify_adminpw = True if dbcfgs['modify_admin_passwd'] == 'Y' else False
        create_editor = True if dbcfgs['create_grafana_user'] == 'Y' else False

        ip = socket.gethostbyname(socket.getfqdn())
        admin_new_passwd = dbcfgs['grafana_admin_new_pwd']
        grafana_user = dbcfgs['grafana_user_name']
        grafana_user_passwd = dbcfgs['grafana_user_passwd']
        grafana_user_email = dbcfgs['grafana_user_email']

        alarm_method = dbcfgs['alarm_method']
        smtp_email = dbcfgs['smtp_email']
        smtp_server = dbcfgs['smtp_server']
        smtp_passwd = dbcfgs['smtp_passwd']

        grafana = Grafana(ip, dbcfgs)
        if modify_adminpw:
            logger.info("Modifying administrator password ...")
            grafana.set_admin_psw(admin_new_passwd)

        if create_editor:
            logger.info("Creating new Grafana user ...")
            grafana.set_editor(grafana_user, grafana_user_passwd, grafana_user_email)

        grafana.templet_import('datasource', 'Prometheus')
        grafana.templet_import('datasource', 'Cloudera%20Manager')
        # grafana.templet_import('datasource', 'OpenTSDB')
        grafana.templet_import('dashboard', 'Esgyndb')
        grafana.templet_import('dashboard', 'Alertdb')

        logger.info("Start importing alert notification channel ...")
        if alarm_method == 'smtp':
            grafana.notify_channels('email', 'esgyn-emial-alert', 'esgyn-email-alert')
            grafana.set_smtp(smtp_email, smtp_server, smtp_passwd)
        elif alarm_method == 'message':
            alone_url = 'http://%s:%s/message/alone' % (dbcfgs['management_node'], MESSAGR_PORT)
            grafana.notify_channels('webhook', 'message-broker-alone', 'message-broker-alone', alone_url)
            batch_url = 'http://%s:%s/message/batch' % (dbcfgs['management_node'], MESSAGR_PORT)
            grafana.notify_channels('webhook', 'message-broker-batch', 'message-broker-batch', batch_url)
        else:
            logger.info('Skip this process')

        logger.info("Starting Dashboard ...")
        grafana.start_db()

        # restart grafana
        logger.info('Restarting Grafana server...')
        run_cmd('service grafana-server restart')

        # clean all
        run_cmd('rm -rf %s' % TMP_GRAFANA_PLUGS)

# main
if __name__ == "__main__":
    set_stream_logger(logger)
    try:
        dbcfgs_json = sys.argv[1]
    except IndexError:
        err('No db config found')
    run()
