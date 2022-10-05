
#ifndef __COMAES_H__
#define __COMAES_H__
#include <openssl/evp.h>
#include <openssl/err.h>

#include <string.h>
#include <stdint.h>

#include "common/Platform.h"
/**
 *  some helper struct and function for AES
 *
 */

const EVP_CIPHER *aes_algorithm_type[] = {
    /* CQD value   Algorithm type   */
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

void aes_create_key(const unsigned char *input, int input_len, unsigned char *key, int aes_mode);

#endif
