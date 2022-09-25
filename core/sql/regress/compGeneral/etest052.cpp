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


#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include "sqludr.h"

extern "C" {

SQLUDR_LIBFUNC SQLUDR_INT32 getNodeName(SQLUDR_CHAR *nodeName,
                                        SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;
  
  char hostName[100];
  int result = gethostname(hostName,100);
  if (result !=0)
  {
    strcpy(nodeName, "bad");
    return SQLUDR_ERROR;
  }
   
  char * at_sign = strchr((char *)hostName, '.');
  std::string shortName =  (at_sign) ? std::string (hostName, (at_sign-hostName)) : hostName;

  sprintf(nodeName, "'%s'", shortName.c_str());
  return SQLUDR_SUCCESS;
}

SQLUDR_LIBFUNC SQLUDR_INT32 getZookeeperTenantStatus (
  SQLUDR_CHAR *tenantName,
  SQLUDR_CHAR *status,
  SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;

  // Trim trailing spaces
  const char *endPos = strchr(tenantName, ' ');
  int j = endPos - tenantName;
  char adjustedName[j+1];
  strncpy (adjustedName, tenantName, j);
  adjustedName [j] = 0;
 
  char buffer[500];
  char* sqroot = getenv("DCS_INSTALL_DIR");
  snprintf (buffer, sizeof(buffer), "%s/bin/dcs zkcli "
            "get /trafodion/wms/tenants/%s",
            sqroot, adjustedName);
  
  bool foundTenant = false;

  FILE* rqst = popen(buffer, "r");
  if (!rqst) throw std::runtime_error("call to zkcli failed!");
  try 
  {
    while (!feof(rqst)) 
    {
      if (fgets(buffer, 258, rqst) != NULL)
      {
        const char * keywordLoc = strstr(buffer, "lastUpdate");
        if (keywordLoc)
        {
          strcpy(status, buffer);
          foundTenant = true;
          break;
        }
      }
    }
  } 
  catch (...) 
  {
    pclose(rqst);
    strcpy(status,"exception running cmd");
    return SQLUDR_ERROR;
  }
  pclose(rqst);
  
  if (!foundTenant)
  {
    strcpy(buffer, "Tenant not found: ");
    strcat(buffer, adjustedName);
    std::string outputLine(buffer);
    strcpy(status, outputLine.c_str());
  }

  return SQLUDR_SUCCESS;
}

}
