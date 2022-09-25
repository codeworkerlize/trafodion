#!/usr/bin/env python

import os
import sys
import xml.dom.minidom
from common import err, cmd_output_as_user


class generateCDHConfig(object):
    def __init__(self, user):
        self.rest_http_port = 0
        self.rest_https_port = 0
        self.restServerURI = {}
        self.traf_conf = cmd_output_as_user(user, 'echo $TRAF_CONF')
        if len(self.traf_conf) == 0:
            self.traf_conf = "/etc/trafodion/conf"

    def generateHbaseEnvString(self):
        self.getRestPort()
        if self.rest_http_port == 0 and self.rest_https_port == 0:
            err("can't get port for REST server")
        self.getRestHostname()
        if len(self.restServerURI) == 0:
            err("can't get hostname for DCS master server")
        return "REST_SERVER_URI=\"" + self.restServerURI + "\""

    def getRestPort(self):
        rest_xml_file = self.traf_conf + "/rest/rest-site.xml"
        if not os.path.exists(rest_xml_file):
            err(rest_xml_file + " is not exists")
        dom = xml.dom.minidom.parse(rest_xml_file)
        root = dom.documentElement
        subentry = root.getElementsByTagName("property")
        for entry in subentry:
            name = entry.getElementsByTagName("name")[0]
            value = entry.getElementsByTagName("value")[0]
            if name.childNodes[0].data == "rest.https.port":
                self.rest_https_port = int(value.childNodes[0].data)
            if name.childNodes[0].data == "rest.port":
                self.rest_http_port = int(value.childNodes[0].data)

    def getRestHostname(self):
        dcs_masters_file = self.traf_conf + "/dcs/masters"
        if not os.path.exists(dcs_masters_file):
            err(dcs_masters_file + " is not exists")
        restHostnames = ""
        with open(dcs_masters_file, "r+") as f:
            for line in f.readlines():
                restHostnames += (line.strip('\n') + ",")
            restHostnames = restHostnames[:-1]
        f.close()
        self.createRestServerConfigure(restHostnames)

    def createRestServerConfigure(self, restHostnames):
        #abc.locl,abc2.local -> https://abc.locl,abc2.local:3333
        if self.rest_https_port != 0:
            self.restServerURI = "https://" + \
                restHostnames + ":" + str(self.rest_https_port)
        else:
            self.restServerURI = "http://" + \
                restHostnames + ":" + str(self.rest_http_port)


# main
if __name__ == '__main__':
    exit(0)
