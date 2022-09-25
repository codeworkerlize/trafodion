/**********************************************************************
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
**********************************************************************/
#ifndef COMENCRYPTION_H
#define COMENCRYPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComEncryption.h
 * Description:  
 *
 * Created:      5/27/2017
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include "Platform.h"
#include "ComSmallDefs.h"
#include "NAString.h"

class ComEncryption : public NABasicObject
{
public:
  ComEncryption ();
  
  struct EncryptionInfo
  {
    Int32 header;
    Int16 rowidCipherType;
    Int16 dataCipherType;
    Int32 flags;
    Int64 encryptionKeyID;
    Int32 rowIdKeyLen;
    unsigned char  rowIdKey[EVP_MAX_KEY_LENGTH];
    Int32 dataKeyLen;
    unsigned char  dataKey[EVP_MAX_KEY_LENGTH];
    Int32 rowidInitVecLen;
    unsigned char  rowidInitVec[32];
    Int32 dataInitVecLen;
    unsigned char  dataInitVec[32];

    NABoolean operator==(const EncryptionInfo& other) const
    {
      return ( header == other.header &&
               rowidCipherType == other.rowidCipherType &&
               dataCipherType == other.dataCipherType &&
               flags == other.flags &&
               encryptionKeyID == other.encryptionKeyID &&
               COMPARE_VOID_PTRS((char*)rowIdKey, rowIdKeyLen, (char*)other.rowIdKey, other.rowIdKeyLen) &&
               COMPARE_VOID_PTRS((char*)dataKey, dataKeyLen, (char*)other.dataKey, other.dataKeyLen) &&
               COMPARE_VOID_PTRS((char*)rowidInitVec, rowidInitVecLen, (char*)other.rowidInitVec, other.rowidInitVecLen) &&
               COMPARE_VOID_PTRS((char*)dataInitVec, dataInitVecLen, (char*)other.dataInitVec, other.dataInitVecLen)
             );
    }
  };

  enum EncryptionFlags {
    ROWID_ENCRYPT  = 0x1,
    DATA_ENCRYPT   = 0x2
  };

  enum CipherAlgorithmType {
    AES_128_ECB = 0,    
    AES_192_ECB,        
    AES_256_ECB,       
    AES_128_CBC,        
    AES_192_CBC,        
    AES_256_CBC,       
    AES_128_CFB1,       
    AES_192_CFB1,       
    AES_256_CFB1,       
    AES_128_CFB8,      
    AES_192_CFB8,       
    AES_256_CFB8,       
    AES_128_CFB128,     
    AES_192_CFB128,     
    AES_256_CFB128,     
    AES_128_OFB,        
    AES_192_OFB,       
    AES_256_OFB,           
  };

  static NABoolean validateEncryptionOptions(NAString *encryptionOptions,
                                             NABoolean &encryptRowid,
                                             NABoolean &encryptData);

  static const EVP_CIPHER * encryptionAlgorithm[];

  static NABoolean isFlagSet(EncryptionInfo &ei, Int32 bitFlags)
  {
    return (ei.flags & bitFlags) != 0; 
  }

  static void setFlag(EncryptionInfo &ei,
                      Int32 bitFlags)
  {
    ei.flags |= bitFlags;
  }

  static void resetFlag(EncryptionInfo &ei,
                        Int32 bitFlags)
  {
    ei.flags &= ~bitFlags;
  }

  static short initEncryptionInfo(Int64 encryptionKeyID,
                                  Int16 rowidCipherType,
                                  Int16 dataCipherType,
                                  EncryptionInfo &ei);

  static short setRowidEncryptionKey(EncryptionInfo &ei);
  static short setDataEncryptionKey(EncryptionInfo &ei);

  static short getKeyFromKeyStore(
       char * keyId, Int32 keyIdLen,
       unsigned char * keyVal, Int32 &keyValLen);

  static short setEncryptionKeys(EncryptionInfo &ei);

  static short setRowidInitVector(EncryptionInfo &ei);
  static short setDataInitVector(EncryptionInfo &ei);

  static int getInitVecLen(int cipherType);

  // generates initialization vectors.
  static short setInitVectors(EncryptionInfo &ei);

  static void initializeEVP();

  static void cleanupEVP();

  // For all encrypt and decrypt calls, the key and iv pointers
  // should point to allocated buffers with required key/iv.
  // 
  static int encryptData(int dataCipherType,
                         unsigned char *plaintext, int plaintext_len, 
                         unsigned char *key, unsigned char *iv, 
                         unsigned char *ciphertext, int &encryptLen);
  
  static int decryptData(int dataCipherType,
                         unsigned char *ciphertext, int ciphertext_len, 
                         unsigned char *key, unsigned char *iv, 
                         unsigned char *plaintext, int &decryptLen);
  
  static int encryptRowId(unsigned char *plaintext, int plaintext_len, 
                          unsigned char *key, unsigned char *iv, 
                          unsigned char *ciphertext, int &encryptLen);
  
  static int decryptRowId(unsigned char *ciphertext, int ciphertext_len, 
                          unsigned char *key, unsigned char *iv, 
                          unsigned char *plaintext, int &decryptLen);
  
  static short extractEncryptionInfo(
       const EncryptionInfo &encInfo,
       Int32 &header,
       Int16 &rowidCipherType, Int16 &dataCipherType,
       unsigned char* &rowIdKey, Int32 &rowIdKeyLen,
       unsigned char* &dataKey, Int32 &dataKeyLen,
       unsigned char* &rowidInitVec, Int32 &rowidInitVecLen,
       unsigned char* &dataInitVec, Int32 &dataInitVecLen);

  static int encrypt_ConvertBase64_Data(int dataCipherType,
                                        unsigned char *plaintext, int plaintext_len,
                                        unsigned char *key, unsigned char *iv,
                                        unsigned char *converttext, int &convertLen);
                                        

private:
}; 

#endif




