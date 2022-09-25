#! /bin/bash
#/* -*-C++-*-
#// @@@ START COPYRIGHT @@@
#//
#// Licensed to the Apache Software Foundation (ASF) under one
#// or more contributor license agreements.  See the NOTICE file
#// distributed with this work for additional information
#// regarding copyright ownership.  The ASF licenses this file
#// to you under the Apache License, Version 2.0 (the
#// "License"); you may not use this file except in compliance
#// with the License.  You may obtain a copy of the License at
#//
#//   http://www.apache.org/licenses/LICENSE-2.0
#//
#// Unless required by applicable law or agreed to in writing,
#// software distributed under the License is distributed on an
#// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#// KIND, either express or implied.  See the License for the
#// specific language governing permissions and limitations
#// under the License.
#//
#// @@@ END COPYRIGHT @@@
# *****************************************************************************
# */

# This script prepares an Apache ORC C++ source tree as $SOURCE to create 
# a small source as $TARGET to be used inside the Trafodion project.  This 
# step is only necessary when porting a newer version of orc. The current
# version in Trafodion is 1.1.2.

export SOURCE=/mnt/qfc/orc-1.1.2
export TARGET=~/myorc

SOURCE=`readlink -f $SOURCE`
TARGET=`readlink -f $TARGET`

rm -rf $TARGET
mkdir $TARGET

cp -r $SOURCE/c++/src $TARGET/src
cp -r $SOURCE/c++/include $TARGET/include

cd $SOURCE/proto
protoc --cpp_out=$TARGET/src orc_proto.proto

cd $SOURCE
cp $TRAF_HOME/../sql/orc/Makefile.forORC .
make -f Makefile.forORC all

cp $SOURCE/build/c++/include/orc/orc-config.hh $TARGET/include/orc
cp $SOURCE/build/c++/src/Adaptor.hh $TARGET/include/orc
cp $SOURCE/c++/src/wrap/orc-proto-wrapper.cc $TARGET/src

cd $TARGET/src
for i in `ls *cc`
do
   fname=`basename $i .cc`
   mv $fname.cc $fname.cpp
done

