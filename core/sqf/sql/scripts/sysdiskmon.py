# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#
#  This script is useful on workstations when doing overnight
#  development regression runs using the local hadoop. It
#  periodically checks to see if the HMaster is up. If it
#  isn't, it attempts to restart it.
#

import os
import subprocess
import socket
import time

diskusagelimit = 99 #how much percent of disk usage will make node down, default is 99
diskleftlimit = 1  #how much G byte left will make node down, default is 1 G
checkInterval = 2 #second

trafvar=os.getenv("TRAF_VAR")
file_name = trafvar+"/sqnodedown.cmd"
hn = socket.gethostname()
if os.path.isfile(file_name) == False:
    temp = open(file_name, 'w+b')
    temp.write(b'node down '+hn)
    temp.close()

def sqcheck():
    cmd='ps -ef | grep service_monitor | grep -v grep'
    p=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,
                       stdin=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    line = p.stdout.readline()
    if line == '':
        return 0
    else:	
        return 1

def runShellCmd(cmd ):
    p=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,
                   stdin=subprocess.PIPE,
                   stderr=subprocess.PIPE)

def shutdownThisNode():
    print("Bring down the node")
    runShellCmd("sqshell -a " + file_name)

#get the root dir disk usage
def checkDisk():
    cmd="df . -h | sed 1d | awk '{print $5}'"

    p=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,
                   stdin=subprocess.PIPE,
                   stderr=subprocess.PIPE)
    line = p.stdout.readline()
    line = line.strip()
    perct=''
    for i in range(0,len(line)-1,1):
        perct += ''.join(line[i])
    disku = int(perct)

    cmd="df . -h | sed 1d | awk '{print $4}'"

    p=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,
                   stdin=subprocess.PIPE,
                   stderr=subprocess.PIPE)
    line = p.stdout.readline()
    line = line.strip()
    lefts=''
    for i in range(0,len(line)-1,1):
        lefts += ''.join(line[i])
    left = float(lefts)
    #check disk usage threthold
    if disku >= diskusagelimit:
      if left < diskleftlimit :
        shutdownThisNode()


#main loop, if trafodion is down, exit, else run forever
while sqcheck():
    checkDisk()
    time.sleep(checkInterval)	
