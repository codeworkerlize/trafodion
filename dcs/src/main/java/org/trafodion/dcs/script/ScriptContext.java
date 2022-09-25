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
package org.trafodion.dcs.script;

public final class ScriptContext {
    private String scriptName;
    private String hostName;
    private String command;
    private boolean debug = false;
    private boolean stripStdOut = true;
    private boolean stripStdErr = true;
    private int exitCode = 0;
    private StringBuilder stdOut = new StringBuilder();
    private StringBuilder stdErr = new StringBuilder();

    public void setScriptName(String value) {
        this.scriptName = value;
    }

    public String getScriptName() {
        return scriptName;
    }

    public void setHostName(String value) {
        this.hostName = value;
    }

    public String getHostName() {
        return hostName;
    }

    public void setCommand(String value) {
        this.command = value;
    }

    public String getCommand() {
        return command;
    }

    public void setDebug(boolean value) {
        this.debug = value;
    }

    public boolean getDebug() {
        return debug;
    }

    public void setStripStdOut(boolean value) {
        this.stripStdOut = value;
    }

    public boolean getStripStdOut() {
        return stripStdOut;
    }

    public void setStripStdErr(boolean value) {
        this.stripStdErr = value;
    }

    public boolean getStripStdErr() {
        return stripStdErr;
    }

    public void setExitCode(int value) {
        this.exitCode = value;
    }

    public int getExitCode() {
        return exitCode;
    }

    public StringBuilder getStdOut() {
        return stdOut;
    }

    public StringBuilder getStdErr() {
        return stdErr;
    }

    public void cleanStdDatas() {
        if (stdOut.length() > 0) {
            stdOut.delete(0, stdOut.length());
        }
        if (stdErr.length() > 0)
            stdErr.delete(0, stdErr.length());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Script name <" + getScriptName() + ">").append(System.lineSeparator());
        sb.append("Command <" + getCommand() + ">").append(System.lineSeparator());
        sb.append("Host name <" + getHostName() + ">").append(System.lineSeparator());
        sb.append("Exit code <" + getExitCode() + ">").append(System.lineSeparator());
        sb.append("StdOut <" + getStdOut().toString() + ">").append(System.lineSeparator());
        sb.append("StdErr <" + getStdErr().toString() + ">");
        return sb.toString();
    }
}
