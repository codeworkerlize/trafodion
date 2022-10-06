

#include "common/ComEncryption.h"

#include <cstdlib>
#include <ctime>

#include "common/ComCextdecs.h"
#include "common/str.h"

const EVP_CIPHER *ComEncryption::encryptionAlgorithm[] = {
    /* Enum value  Algorithm type   */
    EVP_aes_128_ecb(),    /*    0        aes-128-ecb      */
    EVP_aes_192_ecb(),    /*    1        aes_192_ecb      */
    EVP_aes_256_ecb(),    /*    2        aes_256_ecb      */
    EVP_aes_128_cbc(),    /*    3        aes_128_cbc      */
    EVP_aes_192_cbc(),    /*    4        aes_192_cbc      */
    EVP_aes_256_cbc(),    /*    5        aes_256_cbc      */
    EVP_aes_128_cfb1(),   /*    6        aes_128_cfb1     */
    EVP_aes_192_cfb1(),   /*    7        aes_192_cfb1     */
    EVP_aes_256_cfb1(),   /*    8        aes_256_cfb1     */
    EVP_aes_128_cfb8(),   /*    9        aes_128_cfb8     */
    EVP_aes_192_cfb8(),   /*    10       aes_192_cfb8     */
    EVP_aes_256_cfb8(),   /*    11       aes_256_cfb8     */
    EVP_aes_128_cfb128(), /*    12       aes_128_cfb128   */
    EVP_aes_192_cfb128(), /*    13       aes_192_cfb128   */
    EVP_aes_256_cfb128(), /*    14       aes_256_cfb128   */
    EVP_aes_128_ofb(),    /*    15       aes_128_ofb      */
    EVP_aes_192_ofb(),    /*    16       aes_192_ofb      */
    EVP_aes_256_ofb(),    /*    17       aes_256_ofb      */
};

ComEncryption::ComEncryption() {}

// validates that input encryptionOptions are valid.
// These are set during create of schema or table.
// Currently, encryptionOptions can contain combination of
// 'r' (rowid encryption) and 'd' (data encryption).
// Returns TRUE, if encryptionOptions is null or a string with valid parameter,
//         FALSE otherwise.
NABoolean ComEncryption::validateEncryptionOptions(NAString *encryptionOptions, NABoolean &encryptRowid,
                                                   NABoolean &encryptData) {
  encryptRowid = FALSE;
  encryptData = FALSE;
  if ((!encryptionOptions) || (encryptionOptions->isNull())) return TRUE;

  if (*encryptionOptions == "r" || *encryptionOptions == "rd" || *encryptionOptions == "dr")
    encryptRowid = TRUE;
  else if (*encryptionOptions == "d" || *encryptionOptions == "rd" || *encryptionOptions == "dr")
    encryptData = TRUE;
  else
    return FALSE;

  return TRUE;
}

int ComEncryption::getInitVecLen(int cipherType) {
  const EVP_CIPHER *cipher = encryptionAlgorithm[cipherType];
  return EVP_CIPHER_iv_length(cipher);
}

short ComEncryption::initEncryptionInfo(long encryptionKeyID, Int16 rowidCipherType, Int16 dataCipherType,
                                        EncryptionInfo &ei) {
  ei.header = 0;
  ei.flags = 0;

  ei.rowidCipherType = -1;
  ei.dataCipherType = -1;
  ei.dataKeyLen = 0;
  ei.dataInitVecLen = 0;
  ei.rowIdKeyLen = 0;
  ei.rowidInitVecLen = 0;

  ei.encryptionKeyID = encryptionKeyID;

  if (rowidCipherType >= 0) {
    setFlag(ei, ComEncryption::ROWID_ENCRYPT);

    ei.rowidCipherType = rowidCipherType;

    ei.rowIdKeyLen = EVP_CIPHER_key_length(encryptionAlgorithm[rowidCipherType]);

    ei.rowidInitVecLen = EVP_CIPHER_iv_length(encryptionAlgorithm[rowidCipherType]);
  }

  if (dataCipherType >= 0) {
    setFlag(ei, ComEncryption::DATA_ENCRYPT);

    ei.dataCipherType = dataCipherType;

    ei.dataKeyLen = EVP_CIPHER_key_length(encryptionAlgorithm[dataCipherType]);

    ei.dataInitVecLen = EVP_CIPHER_iv_length(encryptionAlgorithm[dataCipherType]);
  }

  return 0;
}

/////////////////////////////////////////////////////////////////////////////
// this method communicates with KeyStore and returns encryption key
// based on input keyId.
//
// Input:        keyId of length keyIdLen.
// Input/Output: On input, keyValLen is maxSize of keyVal
//               On output, keyValLen is actualSize of keyVal
// Output:       keyVal containing key of length keyVal
/////////////////////////////////////////////////////////////////////////////
short ComEncryption::getKeyFromKeyStore(char *keyId, int keyIdLen, unsigned char *keyVal, int &keyValLen) {
  // Code below is temporary. Will be replaced with KeyStore communication
  // code.

  // returns 256 bit keys.
  if (keyValLen != 32) return -1;

  if (keyIdLen < 8)  // 64 bit id
    return -1;

  // generate 256 bit key based on input 64-bit keyId
  long v64 = *(long *)keyId;
  str_cpy_all((char *)&keyVal[0], (const char *)&v64, sizeof(v64));
  str_cpy_all((char *)&keyVal[sizeof(v64)], (const char *)&v64, sizeof(v64));
  str_cpy_all((char *)&keyVal[2 * sizeof(v64)], (const char *)&v64, sizeof(v64));
  str_cpy_all((char *)&keyVal[3 * sizeof(v64)], (const char *)&v64, sizeof(v64));

  keyValLen = 4 * sizeof(v64);

  return 0;
}

short ComEncryption::setRowidEncryptionKey(EncryptionInfo &ei) {
  if (getKeyFromKeyStore((char *)&ei.encryptionKeyID, sizeof(ei.encryptionKeyID), ei.rowIdKey, ei.rowIdKeyLen))
    return -1;

  return 0;
}

short ComEncryption::setDataEncryptionKey(EncryptionInfo &ei) {
  if (getKeyFromKeyStore((char *)&ei.encryptionKeyID, sizeof(ei.encryptionKeyID), ei.dataKey, ei.dataKeyLen)) return -1;

  return 0;
}

short ComEncryption::setEncryptionKeys(EncryptionInfo &ei) {
  if (isFlagSet(ei, ROWID_ENCRYPT)) {
    if (setRowidEncryptionKey(ei)) return -1;
  }

  if (isFlagSet(ei, DATA_ENCRYPT)) {
    if (setDataEncryptionKey(ei)) return -1;
  }

  return 0;
}

short ComEncryption::setRowidInitVector(EncryptionInfo &ei) {
  if ((NOT isFlagSet(ei, ROWID_ENCRYPT)) || (ei.rowidInitVecLen <= 0)) return 0;

  srand((unsigned)time(0));

  *(int *)&ei.rowidInitVec[0] = rand();
  *(int *)&ei.rowidInitVec[sizeof(int)] = rand();
  *(int *)&ei.rowidInitVec[2 * sizeof(int)] = rand();
  *(int *)&ei.rowidInitVec[3 * sizeof(int)] = rand();

  return 0;
}

short ComEncryption::setDataInitVector(EncryptionInfo &ei) {
  if ((NOT isFlagSet(ei, DATA_ENCRYPT)) || (ei.dataInitVecLen <= 0)) return 0;

  srand((unsigned)time(0));

  *(int *)&ei.dataInitVec[0] = rand();
  *(int *)&ei.dataInitVec[sizeof(int)] = rand();
  *(int *)&ei.dataInitVec[2 * sizeof(int)] = rand();
  *(int *)&ei.dataInitVec[3 * sizeof(int)] = rand();

  return 0;
}

short ComEncryption::setInitVectors(EncryptionInfo &ei) {
  if ((isFlagSet(ei, ROWID_ENCRYPT)) && (setRowidInitVector(ei))) return -1;

  if ((isFlagSet(ei, DATA_ENCRYPT)) && (setDataInitVector(ei))) return -1;

  return 0;
}

void ComEncryption::initializeEVP() {
  /* Initialize the library */
  ERR_load_crypto_strings();
  OpenSSL_add_all_algorithms();
  OPENSSL_config(NULL);
}

void ComEncryption::cleanupEVP() {
  /* Clean up */
  EVP_cleanup();
  ERR_free_strings();
}

short ComEncryption::extractEncryptionInfo(const EncryptionInfo &encInfo, int &header, Int16 &rowidCipherType,
                                           Int16 &dataCipherType, unsigned char *&rowIdKey, int &rowIdKeyLen,
                                           unsigned char *&dataKey, int &dataKeyLen, unsigned char *&rowidInitVec,
                                           int &rowidInitVecLen, unsigned char *&dataInitVec, int &dataInitVecLen) {
  header = encInfo.header;
  rowidCipherType = encInfo.rowidCipherType;
  dataCipherType = encInfo.dataCipherType;
  rowIdKeyLen = encInfo.rowIdKeyLen;
  rowIdKey = (unsigned char *)encInfo.rowIdKey;
  dataKeyLen = encInfo.dataKeyLen;
  dataKey = (unsigned char *)encInfo.dataKey;
  rowidInitVecLen = encInfo.rowidInitVecLen;
  rowidInitVec = (unsigned char *)encInfo.rowidInitVec;
  dataInitVecLen = encInfo.dataInitVecLen;
  dataInitVec = (unsigned char *)encInfo.dataInitVec;

  return 0;
}

int ComEncryption::encryptRowId(unsigned char *plaintext, int plaintext_len, unsigned char *key, unsigned char *iv,
                                unsigned char *ciphertext, int &encryptLen) {
  str_cpy_all((char *)ciphertext, (const char *)plaintext, plaintext_len);
  encryptLen = plaintext_len;

  return 0;
}

int ComEncryption::decryptRowId(unsigned char *ciphertext, int ciphertext_len, unsigned char *key, unsigned char *iv,
                                unsigned char *plaintext, int &decryptLen) {
  str_cpy_all((char *)plaintext, (const char *)ciphertext, ciphertext_len);
  decryptLen = ciphertext_len;

  return 0;
}

int ComEncryption::encryptData(int dataCipherType, unsigned char *plaintext, int plaintext_len, unsigned char *key,
                               unsigned char *iv, unsigned char *ciphertext, int &encryptLen) {
  EVP_CIPHER_CTX *ctx;

  int len;

  int ciphertext_len;

  encryptLen = -1;

  const EVP_CIPHER *cipher = encryptionAlgorithm[dataCipherType];

  /* Create and initialise the context */
  if (!(ctx = EVP_CIPHER_CTX_new())) {
    goto label_error;
  }

  /* Initialise the encryption operation. IMPORTANT - ensure you use a key
   * and IV size appropriate for your cipher
   * In this example we are using 256 bit AES (i.e. a 256 bit key). The
   * IV size for *most* modes is the same as the block size. For AES this
   * is 128 bits */
  if (1 != EVP_EncryptInit_ex(ctx, cipher, NULL, key, iv)) {
    goto label_error;
  }

  /* Provide the message to be encrypted, and obtain the encrypted output.
   * EVP_EncryptUpdate can be called multiple times if necessary
   */

  if (1 != EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, plaintext_len)) {
    goto label_error;
  }

  ciphertext_len = len;

  /* Finalise the encryption. Further ciphertext bytes may be written at
   * this stage.
   */
  if (1 != EVP_EncryptFinal_ex(ctx, ciphertext + len, &len)) {
    goto label_error;
  }

  ciphertext_len += len;

  /* Clean up */
  EVP_CIPHER_CTX_free(ctx);

  encryptLen = ciphertext_len;

  return 0;

label_error:
  ERR_clear_error();
  if (ctx) EVP_CIPHER_CTX_free(ctx);
  return -1;
}

int ComEncryption::decryptData(int dataCipherType, unsigned char *ciphertext, int ciphertext_len, unsigned char *key,
                               unsigned char *iv, unsigned char *plaintext, int &decryptLen) {
  EVP_CIPHER_CTX *ctx;

  int len;

  int plaintext_len;

  decryptLen = -1;

  const EVP_CIPHER *cipher = encryptionAlgorithm[dataCipherType];

  /* Create and initialise the context */
  if (!(ctx = EVP_CIPHER_CTX_new())) {
    goto label_error;
  }

  /* Initialise the decryption operation. IMPORTANT - ensure you use a key
   * and IV size appropriate for your cipher
   * In this example we are using 256 bit AES (i.e. a 256 bit key). The
   * IV size for *most* modes is the same as the block size. For AES this
   * is 128 bits */
  if (1 != EVP_DecryptInit_ex(ctx, cipher, NULL, key, iv)) {
    goto label_error;
  }

  /* Provide the message to be decrypted, and obtain the plaintext output.
   * EVP_DecryptUpdate can be called multiple times if necessary
   */
  if (1 != EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, ciphertext_len)) {
    goto label_error;
  }

  plaintext_len = len;

  /* Finalise the decryption. Further plaintext bytes may be written at
   * this stage.
   */
  if (1 != EVP_DecryptFinal_ex(ctx, plaintext + len, &len)) {
    goto label_error;
  }

  plaintext_len += len;

  /* Clean up */
  EVP_CIPHER_CTX_free(ctx);

  decryptLen = plaintext_len;
  return 0;

label_error:
  ERR_clear_error();
  EVP_CIPHER_CTX_free(ctx);
  return -1;
}

int ComEncryption::encrypt_ConvertBase64_Data(int dataCipherType, unsigned char *plaintext, int plaintext_len,
                                              unsigned char *key, unsigned char *iv, unsigned char *converttext,
                                              int &convertLen) {
  int cryptlen = 0;
  int rc = 0;
  char *encryPassword = new char[plaintext_len + 16 + 1];
  memset(encryPassword, 0, plaintext_len + 16 + 1);

  rc = ComEncryption::encryptData(dataCipherType, plaintext, plaintext_len, key, iv, (unsigned char *)encryPassword,
                                  cryptlen);

  if (rc) {
    return -1;
  }

  // convert base64
  convertLen = EVP_EncodeBlock(converttext, (const unsigned char *)encryPassword, cryptlen);

  delete[] encryPassword;
  encryPassword = NULL;

  if (convertLen > 0)
    return 0;
  else
    return -1;
}
