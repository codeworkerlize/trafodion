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

#ifndef SQ_LICENSE_H
#define SQ_LICENSE_H

#define LICENSE_SPLIT              '?'
#define LICENSE_NEVER_EXPIRES      99999
#define LICENSE_VERSION_LEN        2
#define LICENSE_CUSTOMER_LEN       10 
#define LICENSE_NODENUM_LEN        4
#define LICENSE_EXPIRE_LEN         4
#define LICENSE_PACKAGE_INSTALLED  4
#define LICENSE_INSTALL_TYPE       4
#define LICENSE_UNIQUEID_LEN       12
#define LICENSE_RESERVED_FIELD1    4
#define LICENSE_FEATURE_LEN        2
#define LICENSE_PRODUCT_LEN        4
#define LICENSE_MODULE_LEN          ((LICENSE_MAX_MODULE%32 == 0) ?((LICENSE_MAX_MODULE/32)*4):((LICENSE_MAX_MODULE/32+1)*4))
#define LICENSE_LEFT_LEN           (14 - LICENSE_MODULE_LEN)

#define LICENSE_PACKAGE_ENT  1
#define LICENSE_PACKAGE_ADV  2
#define LICENSE_PACKAGE_ENT_TEXT  "ENT"
#define LICENSE_PACKAGE_ADV_TEXT  "ADV"
#define LICENSE_TYPE_DEMO     1
#define LICENSE_TYPE_POC      2
#define LICENSE_TYPE_PRODUCT  3
#define LICENSE_TYPE_INTERNAL 4
#define LICENSE_TYPE_DEMO_TEXT "DEMO"
#define LICENSE_TYPE_POC_TEXT "POC"
#define LICENSE_TYPE_PRODUCT_TEXT "PRODUCT"
#define LICENSE_TYPE_INTERNAL_TEXT "INTERNAL"
#define LICENSE_PRODUCT_DLAKE 0
#define LICENSE_PRODUCT_CBANK 1
#define LICENSE_PRODUCT_DLAKE_TEXT "DATALAKE"
#define LICENSE_PRODUCT_CBANK_TEXT "COREBANK"

#define LICENSE_VERSION_OFFSET 0
#define LICENSE_NAME_OFFSET LICENSE_VERSION_OFFSET + LICENSE_VERSION_LEN
#define LICENSE_NODES_OFFSET LICENSE_NAME_OFFSET + LICENSE_CUSTOMER_LEN
#define LICENSE_EXPIRE_OFFSET LICENSE_NODES_OFFSET + LICENSE_NODENUM_LEN
#define LICENSE_PACKAGE_OFFSET LICENSE_EXPIRE_OFFSET + LICENSE_EXPIRE_LEN
#define LICENSE_TYPE_OFFSET LICENSE_PACKAGE_OFFSET + LICENSE_PACKAGE_INSTALLED
#define LICENSE_RESERVED_OFFSET LICENSE_TYPE_OFFSET + LICENSE_INSTALL_TYPE
#define LICENSE_UNIQUEID_OFFSET LICENSE_RESERVED_OFFSET + LICENSE_RESERVED_FIELD1
#define LICENSE_FEATURE_OFFSET LICENSE_UNIQUEID_OFFSET + LICENSE_UNIQUEID_LEN
#define LICENSE_PRODUCT_OFFSET LICENSE_FEATURE_OFFSET + LICENSE_FEATURE_LEN
#define LICENSE_MODULE_OFFSET LICENSE_PRODUCT_OFFSET + LICENSE_PRODUCT_LEN

#define LICENSE_NUM_BYTES1 32
#define LICENSE_NUM_BYTES_ENC1 64

#define LICENSE_NUM_BYTES2 64
#define LICENSE_NUM_BYTES_ENC2 128

#define PACKAGE_ENT  1
#define PACKAGE_ADV  2
#define PACKAGE_ENT_TEXT  "ENT"
#define PACKAGE_ADV_TEXT  "ADV"

//define the enum of installed type
#define TYPE_DEMO     1
#define TYPE_POC      2
#define TYPE_PRODUCT  3
#define TYPE_INTERNAL 4

#define TYPE_DEMO_TEXT "DEMO"
#define TYPE_POC_TEXT "POC"
#define TYPE_PRODUCT_TEXT "PRODUCT"
#define TYPE_INTERNAL_TEXT "INTERNAL"

#define LICENSE_ONE_DAY 86400
#define LICENSE_SEVEN_DAYS 604800

#define LICENSE_LOWEST_SUPPORTED_VERSION 1
#define LICENSE_HIGHEST_SUPPORTED_VERSION 2

enum LICENSE_FEATURES
{
   LF_ADVANCED          = 0x1,
   LF_ENTERPRISE        = 0x2,   
   LF_MULTITENANCY      = 0x4
};

#define LICENSE_MAX_FEATURES 7

enum LICENSE_MODULES
{
   LM_ROW_LOCK          = 1,
   LM_DDL_ONLINE        = 2,   
   LM_STORED_PROCEDURE  = 4,
   LM_CONNECT_BY        = 5,
   LM_SEC_AUDIT         = 6,
   LM_SEC_MAC           = 7,
   LM_LOCAL_AUTH        = 8,
   LM_LITTLE_SHEET_CACHE = 9,
   LM_HIVE              = 10,
   LM_MULTITENANCY      = 11,
   LM_BACKUP_RESTORE    = 12,
   LM_BINLOG            = 13,
   LM_LOB               = 14, 
   LM_MAX_MODULE       
};
#define LICENSE_MAX_MODULE (LM_MAX_MODULE - 1)
unsigned char* decode (int inputLen, int inputSizeOf, char *pEncStr);
char * strpack(char *src, size_t len, char *dst);

#endif
