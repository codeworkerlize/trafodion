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

import org.trafodion.dcs.master.listener.nio.ListenerMetrics;

public class Metrics  {

	private ListenerMetrics listenerMetrics;
	private int totalRunning;
	private int totalRegistered;
	private int totalAvailable;
	private int totalConnecting;
	private int totalConnected;
	private int totalRebalance;
	private int totalRestart;

	public String getLoad(){               
		int mb = 1024*1024;
		long total;
		long free;
		long max;
		long used;
    
		Runtime runtime = Runtime.getRuntime();                   
		used = (runtime.totalMemory() - runtime.freeMemory()) / mb;           
		free = runtime.freeMemory() / mb;                   
		total = runtime.totalMemory() / mb;           
		max = runtime.maxMemory() / mb;
		return "totalHeap=" + total + ", usedHeap=" + used + ", freeHeap=" + free + ", maxHeap=" + max;
	}
	
	public void initListenerMetrics(long timestamp){
		listenerMetrics = new ListenerMetrics(timestamp);
	}

	public void listenerStartRequest(long timestamp){
		if (listenerMetrics != null)
			listenerMetrics.listenerStartRequest(timestamp);
	}
	public void listenerEndRequest(long timestamp){
		if (listenerMetrics != null)
			listenerMetrics.listenerEndRequest(timestamp);
	}
	public void listenerRequestRejected(){
		if (listenerMetrics != null)
			listenerMetrics.listenerRequestRejected();
	}
	public void listenerWriteTimeout(){
		if (listenerMetrics != null)
			listenerMetrics.listenerWriteTimeout();
	}
	public void listenerReadTimeout(){
		if (listenerMetrics != null)
			listenerMetrics.listenerReadTimeout();
	}
	public void listenerNoAvailableServers(){
		if (listenerMetrics != null)
			listenerMetrics.listenerNoAvailableServers();
	}
	private String getListenerMatrics(){
		String report = "";
		if (null != listenerMetrics){
			report = listenerMetrics.toString();
		}
		return report;
	}
	public void setTotalRunning(int value){
		totalRunning=value;
	}
	public void setTotalRegistered(int value){
		totalRegistered=value;
	}
	public void setTotalAvailable(int value){
		totalAvailable=value;
	}
	public void setTotalConnecting(int value){
		totalConnecting=value;
	}
	public void setTotalConnected(int value){
		totalConnected=value;
	}
	public void setTotalRebalance(int value){
	    totalRebalance=value;
	}
	public void setTotalRestart(int value){
	    totalRestart=value;
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getLoad());
		sb.append(", ");
		sb.append(getListenerMatrics());
		sb.append(", ");
		sb.append("totalRunning=").append(totalRunning);
		sb.append(", ");
		sb.append("totalRegistered=").append(totalRegistered);
		sb.append(", ");
		sb.append("totalAvailable=").append(totalAvailable);
		sb.append(", ");
		sb.append("totalConnecting=").append(totalConnecting);
		sb.append(", ");
		sb.append("totalConnected=").append(totalConnected);
		sb.append(", ");
		sb.append("totalRebalance=").append(totalRebalance);
		sb.append(", ");
		sb.append("totalRestart=").append(totalRestart);
		return sb.toString();
	}
}

