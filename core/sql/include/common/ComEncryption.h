
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

#include "common/Platform.h"
#include "common/ComSmallDefs.h"
#include "common/NAString.h"

class ComEncryption : public NABasicObject {
 public:
  ComEncryption();

  struct EncryptionInfo {
    int header;
    Int16 rowidCipherType;
    Int16 dataCipherType;
    int flags;
    long encryptionKeyID;
    int rowIdKeyLen;
    unsigned char rowIdKey[EVP_MAX_KEY_LENGTH];
    int dataKeyLen;
    unsigned char dataKey[EVP_MAX_KEY_LENGTH];
    int rowidInitVecLen;
    unsigned char rowidInitVec[32];
    int dataInitVecLen;
    unsigned char dataInitVec[32];

    NABoolean operator==(const EncryptionInfo &other) const {
      return (
          header == other.header && rowidCipherType == other.rowidCipherType &&
          dataCipherType == other.dataCipherType && flags == other.flags && encryptionKeyID == other.encryptionKeyID &&
          COMPARE_VOID_PTRS((char *)rowIdKey, rowIdKeyLen, (char *)other.rowIdKey, other.rowIdKeyLen) &&
          COMPARE_VOID_PTRS((char *)dataKey, dataKeyLen, (char *)other.dataKey, other.dataKeyLen) &&
          COMPARE_VOID_PTRS((char *)rowidInitVec, rowidInitVecLen, (char *)other.rowidInitVec, other.rowidInitVecLen) &&
          COMPARE_VOID_PTRS((char *)dataInitVec, dataInitVecLen, (char *)other.dataInitVec, other.dataInitVecLen));
    }
  };

  enum EncryptionFlags { ROWID_ENCRYPT = 0x1, DATA_ENCRYPT = 0x2 };

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

  static NABoolean validateEncryptionOptions(NAString *encryptionOptions, NABoolean &encryptRowid,
                                             NABoolean &encryptData);

  static const EVP_CIPHER *encryptionAlgorithm[];

  static NABoolean isFlagSet(EncryptionInfo &ei, int bitFlags) { return (ei.flags & bitFlags) != 0; }

  static void setFlag(EncryptionInfo &ei, int bitFlags) { ei.flags |= bitFlags; }

  static void resetFlag(EncryptionInfo &ei, int bitFlags) { ei.flags &= ~bitFlags; }

  static short initEncryptionInfo(long encryptionKeyID, Int16 rowidCipherType, Int16 dataCipherType,
                                  EncryptionInfo &ei);

  static short setRowidEncryptionKey(EncryptionInfo &ei);
  static short setDataEncryptionKey(EncryptionInfo &ei);

  static short getKeyFromKeyStore(char *keyId, int keyIdLen, unsigned char *keyVal, int &keyValLen);

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
  static int encryptData(int dataCipherType, unsigned char *plaintext, int plaintext_len, unsigned char *key,
                         unsigned char *iv, unsigned char *ciphertext, int &encryptLen);

  static int decryptData(int dataCipherType, unsigned char *ciphertext, int ciphertext_len, unsigned char *key,
                         unsigned char *iv, unsigned char *plaintext, int &decryptLen);

  static int encryptRowId(unsigned char *plaintext, int plaintext_len, unsigned char *key, unsigned char *iv,
                          unsigned char *ciphertext, int &encryptLen);

  static int decryptRowId(unsigned char *ciphertext, int ciphertext_len, unsigned char *key, unsigned char *iv,
                          unsigned char *plaintext, int &decryptLen);

  static short extractEncryptionInfo(const EncryptionInfo &encInfo, int &header, Int16 &rowidCipherType,
                                     Int16 &dataCipherType, unsigned char *&rowIdKey, int &rowIdKeyLen,
                                     unsigned char *&dataKey, int &dataKeyLen, unsigned char *&rowidInitVec,
                                     int &rowidInitVecLen, unsigned char *&dataInitVec, int &dataInitVecLen);

  static int encrypt_ConvertBase64_Data(int dataCipherType, unsigned char *plaintext, int plaintext_len,
                                        unsigned char *key, unsigned char *iv, unsigned char *converttext,
                                        int &convertLen);

 private:
};

#endif
