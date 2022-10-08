

#include "common/ComSSL.h"
void aes_create_key(const unsigned char *input, int input_len, unsigned char *key, int aes_mode) {
  const int key_len = EVP_CIPHER_key_length(aes_algorithm_type[aes_mode]);

  memset(key, 0, key_len);

  const unsigned char *source;
  unsigned char *ptr;
  const unsigned char *input_end = input + input_len;
  const unsigned char *key_end = key + key_len;

  // loop through all the characters of the input string, and does an assignment
  // with bitwise XOR between the input string and key string. If it iterate until
  // hit the end of key_len byte buffer, just start over from begining of the key
  // and continue doing ^=
  // If the length of input string shorter than key_len, it will stop and the end of
  // the input string.
  for (ptr = key, source = input; source < input_end; source++, ptr++) {
    if (ptr == key_end) ptr = key;
    *ptr ^= *source;
  }
}
