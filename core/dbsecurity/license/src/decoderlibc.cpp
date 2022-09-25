/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/des.h>
#include <openssl/aes.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "decoderlib.h"



unsigned char* decode (int inputlen, int inputSizeOf, char *encstr)
{

    //decodedbuf contains the DES/AES encryted string
    //input string is in ASCII format, each encrypted char is represented as 2 bytes in HEX value
    //strpack transform the HEX into original value
    char decodedbuf[inputlen/2 + LICENSE_ENC_LEN2 + 1];
    memset(decodedbuf, 0 , sizeof(decodedbuf) );
    strpack(encstr, inputSizeOf, (char*) decodedbuf);

#ifdef USING_DES
    DES_cblock key[1];
    DES_key_schedule key_schedule;
   
    DES_string_to_key("nonstop2016",key);

    if (DES_set_key_checked(key, &key_schedule) != 0)
      exit(1);


    size_t len =(sizeof(decodedbuf)+7)/8 * 8;
    unsigned char *decrypt_string= new unsigned char[len+1];
    DES_cblock ivec;
    memset((char*)&ivec, 0, sizeof(ivec));

    //decipher
    DES_ncbc_encrypt((const unsigned char *)decodedbuf, decrypt_string, len, &key_schedule, &ivec, 0);

#else
    AES_KEY aes;
    unsigned char key[AES_BLOCK_SIZE];        // AES_BLOCK_SIZE = 16
    unsigned char iv[AES_BLOCK_SIZE];        // init vector
    unsigned char* decrypt_string;
    for (i=0; i<16; ++i) {
        key[i] = 32 + i;
    }
    for (i=0; i<AES_BLOCK_SIZE; ++i) {
        iv[i] = 0;
    }
    if (AES_set_decrypt_key(key, 128, &aes) < 0) {
        fprintf(stderr, "Unable to set decryption key in AES\n");
        exit(-1);
    }
    int aesenclen = TOTAL_LEN;
    size_t len;

    if ( aesenclen % AES_BLOCK_SIZE == 0) {
        len = aesenclen + 1;
    } else {
        len = ( aesenclen / AES_BLOCK_SIZE + 1) * AES_BLOCK_SIZE;
    }

    decrypt_string = (unsigned char*)calloc(len, sizeof(unsigned char));
    // decrypt
    AES_cbc_encrypt((const unsigned char *)decodedbuf, decrypt_string, len, &aes, iv, AES_DECRYPT);

#endif

    return decrypt_string;
}

char * strpack(char *src, size_t len, char *dst)
{
    unsigned char *from, *to, *end;
 
    from = (unsigned char *)src;
    to = (unsigned char *)dst;
 
    for (end = to + len / 2; to < end; from += 2, to++)
    {
        *to  = (asc2hex[*from] << 4) | asc2hex[*(from + 1)];
    }
    return dst;
}
