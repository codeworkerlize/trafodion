/**
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
**/

package org.apache.hadoop.hbase.pit;

import java.io.IOException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class SnapshotImportJob {
  Process impProcess;
  String  impLog;
  long    impEpoch;
  long timeout = 180000; //3 minutes 
   
  public SnapshotImportJob(Process p, String log) throws IOException {
    impProcess = p;
    impLog = log;
    impEpoch = EnvironmentEdgeManager.currentTime();
  }
  
  public Process getProcess() { return impProcess; }
  
  public String  getLog() { return impLog; }
  
  public void renewTimeout() {
   impEpoch = EnvironmentEdgeManager.currentTime();
  }
  
  public boolean isExpired() {
   long now = EnvironmentEdgeManager.currentTime();
   if(now - impEpoch > timeout)
     return true;
   else
     return false;
   }
   
 }
