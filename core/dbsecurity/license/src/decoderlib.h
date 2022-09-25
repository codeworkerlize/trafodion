/* ** All rights reserved by Esgyn Corporation 2015 */

#ifndef DECODERLIB_H
#define DECODERLIB_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../sqf/export/include/common/sq_license.h"

#define USING_DES 1

#define MAX_STR_LEN  512
//ASCII is smaller than 256, so 2 bytes to show in HEX 
#define MAX_ENC_STR_LEN  MAX_STR_LEN*2

#define LICENSE_TOTAL_LEN1 (LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE + LICENSE_RESERVED_FIELD1)
#define LICENSE_ENC_LEN1  (LICENSE_TOTAL_LEN1)*2

#define LICENSE_TOTAL_LEN2 (LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE + LICENSE_RESERVED_FIELD1 + LICENSE_UNIQUEID_LEN  + LICENSE_FEATURE_LEN + LICENSE_PRODUCT_LEN + LICENSE_MODULE_LEN + LICENSE_LEFT_LEN)
#define LICENSE_ENC_LEN2  (LICENSE_TOTAL_LEN2)*2

static unsigned char asc2hex[] = {
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
     0, 10, 11, 12, 13, 14, 15,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0, 10, 11, 12, 13, 14, 15,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0
};

#endif
