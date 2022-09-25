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
package org.trafodion.rest;

import java.util.*;

public class RegisteredServer  {
	//private static  final Log LOG = LogFactory.getLog(RegisteredServer.class);

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
	private String sla;
	private String connectProfile;
    private String disconnectProfile;
	private long connectedInterval;
	private long connectTime;
	private long createTime;
	private String tenantName;
	
	public long getCreateTime() {
		return createTime;
	}
    public Date getCreateTimeAsDate(){
        return new Date(createTime);
    }
	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}
	private String userName;

	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
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
	public Date getTimestampAsDate() {
	    return new Date(timestamp);
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
	public String getSla(){
	    return sla;
	}
    public void setSla(String value){
        sla = value;
    }
    public String getConnectProfile(){
        return connectProfile;
    }
    public void setConnectProfile(String value){
        connectProfile = value;
    }
    public String getDisconnectProfile(){
        return disconnectProfile;
    }
    public void setDisconnectProfile(String value){
        disconnectProfile = value;
    }
    public Date getConnectTimeAsDate(){
        return new Date(connectTime);
    }
    public long getConnectTime(){
        return connectTime;
    }
    public void setConnectTime(long value){
        connectTime = value;
    }
    public long getConnectedInterval(){
        return connectedInterval;
    }
    public void setConnectedInterval(long value){
        connectedInterval = value;
    }
	public String getTenantName() {
		return tenantName;
	}
	public void setTenantName(String tenantName) {
		this.tenantName = tenantName;
	}
}

