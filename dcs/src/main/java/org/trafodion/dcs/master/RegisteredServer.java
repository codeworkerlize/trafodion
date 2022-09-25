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

public class RegisteredServer  {

	private boolean registered=false;
	private String dialogueId;
	private String nid;
	private String pid;
	private String processName;
	private String ipAddress;
	private String port;
	private String state;
	private long timestamp;
	private String clientName;
	private String clientIpAddress;
	private String clientPort;
	private String clientAppl;
	private long mtime;
	private String sla;
	private String cprofile;
    private String dprofile;
    private long ctime;

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("DialogueId : ").append(dialogueId);
        sb.append(", registered : ").append(registered);
        sb.append(", nid : ").append(nid);
        sb.append(", pid : ").append(pid);
        sb.append(", processName : ").append(processName);
        sb.append(", ipAddress : ").append(ipAddress);
        sb.append(", port : ").append(port);
        sb.append(", state : ").append(state);
        sb.append(", timestamp : ").append(timestamp);
        sb.append(", clientName : ").append(clientName);
        sb.append(", clientIpAddress : ").append(clientIpAddress);
        sb.append(", clientPort : ").append(clientPort);
        sb.append(", clientAppl : ").append(clientAppl);
        sb.append(", mtime : ").append(mtime);
        sb.append(", sla : ").append(sla);
        sb.append(", cprofile : ").append(cprofile);
        sb.append(", dprofile : ").append(dprofile);
        sb.append(", ctime : ").append(ctime);
        return sb.toString();
    }
	public void setIsRegistered() {
		registered = true;
	}
	public String isRegistered() {
		if(registered)
			return "YES";
		else
			return "NO";
	}
	public String getIsRegistered() {
		return isRegistered();
	}
	public void setState(String value) {
		state = value;
	}
	public String getState() {
		return state;
	}
	public void setNid(String value) {
		nid = value;
	}
	public String getNid() {
		return nid;
	}
	public void setPid(String value) {
		pid = value;
	}
	public String getPid() {
		return pid;
	}	
	public void setProcessName(String value) {
		processName = value;
	}
	public String getProcessName() {
		return processName;
	}	
	public void setIpAddress(String value) {
		ipAddress = value;
	}
	public String getIpAddress() {
		return ipAddress;
	}	
	public void setPort(String value) {
		port = value;
	}
	public String getPort() {
		return port;
	}	
	public void setDialogueId(String value) {
		dialogueId= value;
	}
	public String getDialogueId() {
		return dialogueId;
	}		
	public void setTimestamp(long value) {
		timestamp = value;
	}
	public long getTimestamp() {
		return timestamp;
	}	
	public void setClientName(String value) {
		clientName = value;
	}
	public String getClientName() {
		return clientName;
	}	
	public void setClientIpAddress(String value) {
		clientIpAddress = value;
	}
	public String getClientIpAddress() {
		return clientIpAddress;
	}	
	public void setClientPort(String value) {
		clientPort = value;
	}
	public String getClientPort() {
		return clientPort;
	}		
	public void setClientAppl(String value) {
		clientAppl = value;
	}
	public String getClientAppl() {
		return clientAppl;
	}	
	public void setMtime(long value){
	    mtime = value;
	}
	public long getMtime(){
	    return mtime;
	}
    public void setSla(String value){
        sla = value;
    }
    public String getSla(){
        return sla;
    }
    public void setConnectProfile(String value){
        cprofile = value;
    }
    public String getConnectProfile(){
        return cprofile;
    }
    public void setDisconnectProfile(String value){
        dprofile = value;
    }
    public String getDisconnectProfile(){
        return dprofile;
    }

	public long getCtime() {
		return ctime;
	}

	public void setCtime(long ctime) {
		this.ctime = ctime;
	}
}

