/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;

public class FloatingIp {
    private static final Logger LOG = LoggerFactory.getLogger(FloatingIp.class);
    private static FloatingIp instance = null;
    private DcsMaster master;
    private boolean isEnabled;
    private boolean isKeepalivedEnabled;

    public static FloatingIp getInstance(DcsMaster master) throws Exception {
        if (instance == null) {
            instance = new FloatingIp(master);
        }
        return instance;
    }

    private FloatingIp(DcsMaster master) {
        try {
            this.master = master;
            isEnabled = master.getConfiguration().getBoolean(
                    Constants.DCS_MASTER_FLOATING_IP,
                    Constants.DEFAULT_DCS_MASTER_FLOATING_IP);
            isKeepalivedEnabled = master.getConfiguration().getBoolean(
                    Constants.DCS_MASTER_KEEPALIVED, 
                    Constants.DEFAULT_DCS_MASTER_KEEPALIVED);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public synchronized int runScript() throws Exception {
        if (isKeepalivedEnabled) {
            if (LOG.isInfoEnabled()) {
                LOG.info("DCS HA managed externally ");
            }
           return 0;
        }

        if (isEnabled) {
            if (LOG.isInfoEnabled()) {
                LOG.info("<Floating IP is enabled>");
            }
        } else {
            if (LOG.isInfoEnabled()) {
                LOG.info("<Floating IP is disabled>");
            }
            return 0;
        }

        ScriptContext scriptContext = new ScriptContext();
        scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
        scriptContext.setStripStdOut(false);
        scriptContext.setStripStdErr(false);

        String externalInterface = master.getConfiguration().get(
                Constants.DCS_MASTER_FLOATING_IP_EXTERNAL_INTERFACE,
                Constants.DEFAULT_DCS_MASTER_FLOATING_IP_EXTERNAL_INTERFACE);
        if (externalInterface.equalsIgnoreCase("default")) {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "When Floating IP feature is enabled the property {} must be specified in your dcs-site.xml",
                        Constants.DCS_MASTER_FLOATING_IP_EXTERNAL_INTERFACE);
            }
            return 0;
        }

        String externalIpAddress = master.getConfiguration().get(
                Constants.DCS_MASTER_FLOATING_IP_EXTERNAL_IP_ADDRESS,
                Constants.DEFAULT_DCS_MASTER_FLOATING_IP_EXTERNAL_IP_ADDRESS);
        if (externalIpAddress.equalsIgnoreCase("default")) {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "When Floating IP feature is enabled the property {} must be specified in your dcs-site.xml",
                        Constants.DCS_MASTER_FLOATING_IP_EXTERNAL_IP_ADDRESS);
            }
            return 0;
        }

        int masterPort = master.getConfiguration().getInt(
                Constants.DCS_MASTER_PORT, Constants.DEFAULT_DCS_MASTER_PORT);

        String floatingIpCommand = master.getConfiguration().get(
                Constants.DCS_MASTER_FLOATING_IP_COMMAND,
                Constants.DEFAULT_DCS_MASTER_FLOATING_IP_COMMAND);

        String command = floatingIpCommand
                .replace("-i", "-i " + externalInterface + " ")
                .replace("-a", "-a " + externalIpAddress + " ")
                .replace("-p", "-p " + masterPort);
        scriptContext.setCommand(command);
        if (LOG.isInfoEnabled()) {
            LOG.info("Floating IP <{}>.", scriptContext.getCommand());
        }
        ScriptManager.getInstance().runScript(scriptContext);// Blocking call

        if (LOG.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("exit code <" + scriptContext.getExitCode() + ">");
            if (!scriptContext.getStdOut().toString().isEmpty())
                sb.append(", stdout <" + scriptContext.getStdOut().toString() + ">");
            if (!scriptContext.getStdErr().toString().isEmpty())
                sb.append(", stderr <" + scriptContext.getStdErr().toString() + ">");
            LOG.info(sb.toString());
        }

        if (scriptContext.getExitCode() == 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("<Floating IP successful>");
            }
        } else {
            LOG.error("<Floating IP failed>");
        }

        return scriptContext.getExitCode();
    }
}
