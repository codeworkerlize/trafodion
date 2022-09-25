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

#include "tmregisterregion.h"
#include "dtm/tm.h"
#include <string.h>
#include <iostream>
using namespace std;

/*
 * Class:     org_apache_hadoop_hbase_client_transactional_TransactionState
 * Method:    registerRegion2
 * Signature: (JJI[BJ[BII)V
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_client_transactional_TransactionState_registerRegion
(JNIEnv *pp_env, jobject pv_object, jlong pv_transid, jlong pv_startid, jint pv_port, jbyteArray pv_hostname, jlong pv_startcode, jbyteArray pv_dos, jint pv_peerid, jint pv_tmFlags)
{
   char la_hostname[TM_MAX_REGIONSERVER_STRING];
   char la_dos[TM_MAX_REGIONSERVER_STRING];
   memset(la_hostname, 0, TM_MAX_REGIONSERVER_STRING);
   memset(la_dos, 0, TM_MAX_REGIONSERVER_STRING);

   int lv_hostname_length = pp_env->GetArrayLength(pv_hostname);
   jbyte *lp_hostname = pp_env->GetByteArrayElements(pv_hostname, 0);

   int lv_dos_length = pp_env->GetArrayLength(pv_dos);
   jbyte *lp_dos = pp_env->GetByteArrayElements(pv_dos, 0);

   memcpy(la_hostname,
          lp_hostname,
          lv_hostname_length);
   memcpy(la_dos,
          lp_dos,
          lv_dos_length);

  /*
  cout << "ENTER registerRegion1 JNI. "
       << ", transid: "  << pv_transid 
       << ", startid: " << pv_startid 
       << ", hostname: " << la_hostname
       << ", port: " << pv_port 
       << ", region start code: " << pv_startcode 
       << ", tmFlags: " << pv_tmFlags
       << "\n";
   */

   int lv_err = REGISTERREGION(pv_transid, pv_startid, pv_port, la_hostname, lv_hostname_length, pv_startcode, la_dos, lv_dos_length, pv_peerid, pv_tmFlags);

   pp_env->ReleaseByteArrayElements(pv_hostname, lp_hostname, 0);
   pp_env->ReleaseByteArrayElements(pv_dos, lp_dos, 0);

   return lv_err;

}

JNIEXPORT void JNICALL Java_io_esgyn_client_MTransactionState_registerMRegion(JNIEnv *pp_env,
									      jobject pv_object, 
									      jlong pv_transid,
									      jlong pv_startid, 
									      jint pv_port, 
									      jbyteArray pv_hostname, 
									      jlong pv_startcode, 
									      jbyteArray pv_location, 
									      jint pv_peerid,
                                                                              jint pv_tmFlags)
{
  
   char la_hostname[TM_MAX_REGIONSERVER_STRING];
   char la_location[TM_MAX_REGIONSERVER_STRING];
   memset(la_hostname, 0, TM_MAX_REGIONSERVER_STRING);
   memset(la_location, 0, TM_MAX_REGIONSERVER_STRING);

   int lv_hostname_length = pp_env->GetArrayLength(pv_hostname);
   jbyte *lp_hostname = pp_env->GetByteArrayElements(pv_hostname, 0);

   int lv_location_length = pp_env->GetArrayLength(pv_location);
   jbyte *lp_location = pp_env->GetByteArrayElements(pv_location, 0);

   memcpy(la_hostname,
          lp_hostname,
          lv_hostname_length);
   memcpy(la_location,
          lp_location,
          lv_location_length);

 /*
 cout << "ENTER registerMRegion2 JNI. "
       << ", transid: "  << pv_transid 
       << ", startid: " << pv_startid 
       << ", hostname: " << la_hostname
       << ", port: " << pv_port 
       << ", region start code: " << pv_startcode 
       << ", location: " << la_location
       << "\n";

 */
   REGISTERREGION(pv_transid,
		 pv_startid,
		 pv_port,
		 la_hostname,
		 lv_hostname_length,
		 pv_startcode,
		 la_location,
		 lv_location_length,
		 pv_peerid,
                 pv_tmFlags);

   pp_env->ReleaseByteArrayElements(pv_hostname, lp_hostname, 0);
   pp_env->ReleaseByteArrayElements(pv_location, lp_location, 0);

}

