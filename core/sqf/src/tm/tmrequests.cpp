/*
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
* @@@ END COPYRIGHT @@@                                                          */

#include <string.h>
#include <iostream>
#include "tmrequests.h"
#include "dtm/tm.h"
#include "../../inc/fs/feerrors.h"
#include "seabed/ms.h"


using namespace std;

int initialized = 0;

int init()
{
  int lv_error = FEOK;
  int lv_argc = 1;
  char **lp_argv;
  char la_argv[100];
  lp_argv = (char **) &la_argv;
  lp_argv[0] = (char *) "t";
  const char lc_name[10] = "";
  char * lp_name = (char *) &lc_name;
  
    
  try {
    lv_error = msg_init_attach(&lv_argc, &lp_argv, true, lp_name);
    if (lv_error)  {
      printf("msg_init_attach failed");
      return lv_error; 
    }
    
    lv_error = msg_mon_process_startup3(false, false); 
    if (lv_error)  {
      printf("msg_mon_process_startup failed");
      return lv_error; 
    }
  }catch(SB_Fatal_Excep lv_except) {
    printf("Exception connecting to the monitor. \n");
    return FEINVALOP;
  }
  
  initialized = 1;
  return lv_error;

}

/*
 * Class:     org_trafodion_pit_XDC
 * Method:    lockTMReq
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_pit_XDC_lockTMReq
  (JNIEnv *, jobject)
{
  int lv_error = FEOK;
  
  if(!initialized)
  {
    lv_error = init();
    if(lv_error)
      return lv_error;
  }
  
  lv_error  = DTM_LOCKTM();
  return lv_error;
}

/*
 * Class:     org_trafodion_pit_XDC
 * Method:    unlockTMReq
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_pit_XDC_unlockTMReq
  (JNIEnv *, jobject)
{
  int lv_error = FEOK;
  
  if(!initialized)
  {
    lv_error = init();
    if(lv_error)
      return lv_error;
  }
  
  lv_error  = DTM_UNLOCKTM();
  return lv_error;
}

/*
 * Class:     org_trafodion_pit_XDC
 * Method:    isSDNDrainedTMReq
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_pit_XDC_sdnTranReq
  (JNIEnv *, jobject)
{
  int lv_error = FEOK;
  
  if(!initialized)
  {
    lv_error = init();
    if(lv_error)
      return lv_error;
  }
  
  lv_error  = DTM_SDNTRANSACTIONS();
  return lv_error;
}

int main()
{
  return init();
  
}
