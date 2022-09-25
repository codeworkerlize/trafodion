
/**********************************************************************
* @@@ START COPYRIGHT @@@
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
**********************************************************************/
package org.trafodion.dcs.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;

public class DcsNetworkConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DcsNetworkConfiguration.class);
    private static Configuration conf;
    private InetAddress ia;
    private String intHostAddress;
    private String extHostAddress;
    private String canonicalHostName;
    private String extInterfaceName;
    private boolean matchedInterface = false;

    public DcsNetworkConfiguration(Configuration conf) throws Exception {
        this.conf = conf;
        ia = InetAddress.getLocalHost();

        String dcsDnsInterface = conf.get(Constants.DCS_DNS_INTERFACE,
                Constants.DEFAULT_DCS_DNS_INTERFACE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Dcs Dns Interface <{}>", dcsDnsInterface);
        }
        if (dcsDnsInterface.equalsIgnoreCase("default")) {
            intHostAddress = extHostAddress = ia.getHostAddress();
            canonicalHostName = ia.getCanonicalHostName();
            extInterfaceName = NetworkInterface.getByInetAddress(ia)
                    .getDisplayName();
            if (LOG.isInfoEnabled()) {
                LOG.info("Dcs Using localhost <{}, {}, {}>.", extInterfaceName, canonicalHostName,
                        extHostAddress);
            }
            return;
        } else {
            // For all nics get all hostnames and addresses
            // and try to match against dcs.dns.interface property
            Enumeration<NetworkInterface> nics = NetworkInterface
                    .getNetworkInterfaces();
            while (nics.hasMoreElements() && !matchedInterface) {
                InetAddress inet = null;
                NetworkInterface ni = nics.nextElement();
                if (dcsDnsInterface.equalsIgnoreCase(ni.getDisplayName())) {
                    inet = getInetAddress(ni);
                    getCanonicalHostName(ni, inet);
                    extInterfaceName = ni.getDisplayName();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Using interface <{}>, <{}, {}, {}>.",ni.getName(), ni.getDisplayName(), canonicalHostName,
                                extHostAddress);
                    }
                } else {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Found interface DisplayName <{}>.", ni.getDisplayName());
                    }
                    Enumeration<NetworkInterface> subIfs = ni
                            .getSubInterfaces();
                    for (NetworkInterface subIf : Collections.list(subIfs)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Sub Interface Display name <{}>", subIf.getDisplayName());
                        }
                        if (dcsDnsInterface.equalsIgnoreCase(subIf
                                .getDisplayName())) {
                            inet = getInetAddress(subIf);
                            getCanonicalHostName(subIf, inet);
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Matched subIf <{}>, <{}>, <{}>.", subIf.getName(),
                                        subIf.getDisplayName(), inet.getHostAddress());
                            }
                            extInterfaceName = ni.getDisplayName();
                            break;
                        }
                    }
                }
            }
        }
        if (!matchedInterface) {
            checkCloud();
        }
    }

    public void getCanonicalHostName(NetworkInterface ni, InetAddress inet) {
        intHostAddress = extHostAddress = inet.getHostAddress();
        canonicalHostName = inet.getCanonicalHostName();
        ia = inet;
    }

    public InetAddress getInetAddress(NetworkInterface ni) {
        InetAddress inet = null;
        Enumeration<InetAddress> rawAdrs = ni.getInetAddresses();
        while (rawAdrs.hasMoreElements()) {
            inet = rawAdrs.nextElement();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Match Found interface <{}, {}, {}>.", ni.toString(), ni.getDisplayName(),
                        inet.getHostAddress());
            }
        }
        matchedInterface = true;
        return inet;
    }

    public void checkCloud() {
        // Ideally we want to use http://jclouds.apache.org/ so we can support
        // all cloud providers.
        // For now, use OpenStack Nova to retrieve int/ext network address map.
        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking Cloud environment.");
        }
        String cloudCommand = conf.get(Constants.DCS_CLOUD_COMMAND,
                Constants.DEFAULT_DCS_CLOUD_COMMAND);
        ScriptContext scriptContext = new ScriptContext();
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        scriptContext.setCommand(cloudCommand);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<{}> exec <{}>", scriptContext.getScriptName(), scriptContext.getCommand());
        }
        ScriptManager.getInstance().runScript(scriptContext);// This will block
        // while script is
        // running

        StringBuilder sb = new StringBuilder();
        sb.append(scriptContext.getScriptName()).append(" exit code <")
                .append(scriptContext.getExitCode()).append(">");
        if (!scriptContext.getStdOut().toString().isEmpty()) {
            sb.append(", stdout <").append(scriptContext.getStdOut().toString()).append(">");
        }
        if (!scriptContext.getStdErr().toString().isEmpty()) {
            sb.append(", stderr <").append(scriptContext.getStdErr().toString()).append(">");
            if (LOG.isInfoEnabled()) {
                LOG.info(sb.toString());
            }
            if (!scriptContext.getStdOut().toString().isEmpty()) {
                Scanner scn = new Scanner(scriptContext.getStdOut().toString());
                scn.useDelimiter(",");
                intHostAddress = scn.next();// internal ip
                extHostAddress = scn.next();// external ip
                scn.close();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Cloud environment found");
                }
            } else if (LOG.isInfoEnabled()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Cloud environment not found");
                }
            }
        }
    }

    public String getHostName() {
        if (canonicalHostName != null) {
            return canonicalHostName.toLowerCase();
        } else {
            return null;
        }
    }

    public String getServerIp(){
        return ia.getHostAddress();
    }

    public String getIntHostAddress() {
        return intHostAddress.toLowerCase();
    }

    public String getExtHostAddress() {
        return extHostAddress.toLowerCase();
    }

    public String getExtInterfaceName() {
        return extInterfaceName.toLowerCase();
    }

}
