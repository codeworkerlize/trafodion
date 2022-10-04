// **********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
// **********************************************************************

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include "ComMemLog.h"

void help() {
  printf("Usage: memlogshow pid\n");
  return;
}

int main(int argc, char *argv[]) {
  if (argc != 2) {
    help();
    exit(0);
  }

  long pid = atol(argv[1]);
  if (pid == 0) {
    help();
    exit(0);
  }

  int status;
  status = ComMemLog::instance().initialize(pid, true);
  if (status != 0) {
    printf("init failed: %s\n", strerror(status));
    return -1;
  }
  ComMemLog::instance().memshow();

  return 0;
}
