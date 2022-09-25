#!/bin/bash
#
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

sudo yum -y install cmake
export PATH=$PATH:${TOOLSDIR}/apache-maven-3.0.5/bin
ORC_VER=1.5.0
ORC_BUILD_DIR=${TOOLSDIR}/orc-${ORC_VER}
ORC_JAR_DIR=${TOOLSDIR}/ORC-${ORC_VER}-Linux/share
ORC_UBER_JAR_FILE=orc-tools-${ORC_VER}-uber.jar
ORC_CORE_JAR_FILE=orc-core-${ORC_VER}.jar
ORC_SOURCE_FILE=orc-${ORC_VER}.tar.gz
cd ${TOOLSDIR}
wget https://archive.apache.org/dist/orc/orc-${ORC_VER}/${ORC_SOURCE_FILE}
tar -xvf ${ORC_SOURCE_FILE}
cd ${ORC_BUILD_DIR}
cd java
mvn package
mkdir -p ${ORC_JAR_DIR}
cd ${TOOLSDIR}
cp -p ${ORC_BUILD_DIR}/java/core/target/${ORC_CORE_JAR_FILE} ${ORC_JAR_DIR}
cp -p ${ORC_BUILD_DIR}/java/tools/target/${ORC_UBER_JAR_FILE} ${ORC_JAR_DIR}

