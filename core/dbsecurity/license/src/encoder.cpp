/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/des.h>
#include <openssl/aes.h>
#include <ctype.h>
#include "decoderlib.h"

#define USING_DES 1

void printHelp()
{
    printf("\
encoder –v [version]\n\
        -c [customer name]\n\
        -n [node number]\n\
        -e [expire date]\n\
        -p [package installed]\n\
        -t [install type]\n\
        -r [reserved field]\n\
        -b [product edition]\n\
        -f [feature field]\n\
        -m [module code]\n\
        -u [cluster unique id (1)]\n\
        -x [cluster unique id (2)]\n\
        support modules switch as below:\n\
        LM_ROW_LOCK          = 1\n\
        LM_DDL_ONLINE        = 2\n\
        LM_STORED_PROCEDURE  = 4\n\
        LM_CONNECT_BY        = 5\n\
        LM_SEC_AUDIT         = 6\n\
        LM_SEC_MAC           = 7\n\
        LM_LOCAL_AUTH        = 8\n\
        LM_LITTLE_SHEET_CACHE = 9\n\
        LM_HIVE              = 10\n\
        LM_MULTITENANCY      = 11\n\
        LM_BACKUP_RESTORE    = 12\n\
        LM_BINLOG            = 13\n\
        LM_LOB               = 14\n"
        );
}

int main(int argc, char *argv[])
{
    int ret = 0;
    int ch = 0;
    int i = 0;
    short v = 0;
    int n = 0;
    int argnum=0;
    int da = 0;
    int len = 0;
    int package=0, type=0;
    int product_edition = 0;
    
    char version[LICENSE_VERSION_LEN+1];
    char customer[LICENSE_CUSTOMER_LEN+1];
    int nodenumber = 0;
    int expiredate = 0;
    char packageInstalled[LICENSE_PACKAGE_INSTALLED+1];
    char prodInstalled[LICENSE_PRODUCT_LEN+1];
    char installType[LICENSE_INSTALL_TYPE+1];
    char typeUpper[16]; 
    char pkgUpper[16]; 
    char prodUpper[16]; 
    char reserved1[LICENSE_RESERVED_FIELD1+1];
    char module[LICENSE_MODULE_LEN+1]; 
    long long unique_Time = 0;
    int random = 0;
    short feature = 0;
    
    AES_KEY aes;
    unsigned char aeskey[AES_BLOCK_SIZE];        // AES_BLOCK_SIZE = 16
    unsigned char aesiv[AES_BLOCK_SIZE];        // init vector
    
    /* initialize string buffer */
    memset(customer,0,LICENSE_CUSTOMER_LEN+1);
    memset(packageInstalled,0,LICENSE_PACKAGE_INSTALLED+1);
    memset(prodInstalled,0,LICENSE_PRODUCT_LEN+1);
    memset(installType,0,LICENSE_INSTALL_TYPE+1);
    memset(reserved1,0,LICENSE_RESERVED_FIELD1+1);
    memset(module,0,LICENSE_MODULE_LEN+1);
    
    while((ch=getopt(argc,argv,"v:c:n:e:p:t:b:r:x:u:f:m:"))!=-1)
    {
        switch(ch)
        {
	  case 'f':
                feature = atoi(optarg);
                argnum++;
                break;
          case 'm':
                {
                  char * tmp = optarg;
                  int moduleId;
                  for (int i =0 ;i < LICENSE_MODULE_LEN / sizeof(int);i++)
                  {
		      moduleId = atoi(tmp); 
                      memcpy(module+i*sizeof(int),&moduleId,sizeof(int));
  		      tmp = strchr(tmp,LICENSE_SPLIT);
                      if (tmp == NULL)
                      {
                         break;
		      }
                      tmp+=1; 
                  }
                }
                argnum++;
                break;
          case 'u':
                unique_Time = atoll(optarg);
                argnum++;
                break;
          case 'x':
                random = atoi(optarg);
                argnum++;
                break;
            case 'v':
                v = atoi(optarg);
                if ( v < LICENSE_LOWEST_SUPPORTED_VERSION || v > LICENSE_HIGHEST_SUPPORTED_VERSION )  
                {
                    printf("Version %s is invalid\n", optarg);
                    exit(1);
                }
                argnum++;
                break;
            case 'c':
                sprintf(customer,"%10s",optarg );
                argnum++;
                break;
            case 'n':
                nodenumber = atoi(optarg);
                if ( nodenumber < 1 || nodenumber > 9999 )
                {
                    printf("node number %s is invalid\n", optarg);
                    exit(1);
                }               
                argnum++;
                break;
            case 'e':
                // 32-bit integer, # of days after Jan 1, 1970, 4 bytes
                expiredate= atoi(optarg);
                argnum++;
                break;
            case 'p':
                package = 0;
                memset(pkgUpper, 0, sizeof(pkgUpper)); 
                if (strlen(optarg) > 16) {
                  printf("Invalid package\n");
                  exit(1);
                }
                for(i = 0; i < strlen(optarg); i++)
                  pkgUpper[i]=toupper(optarg[i]);
                if(strcmp(pkgUpper,LICENSE_PACKAGE_ADV_TEXT) == 0 ) 
                  package=LICENSE_PACKAGE_ADV;
                else if(strcmp(pkgUpper,LICENSE_PACKAGE_ENT_TEXT) == 0)
                  package=LICENSE_PACKAGE_ENT;
		else
		{		
		  printf("Invalid package \n");
                  exit(1);
                }
                memcpy(packageInstalled,(void*)&package,sizeof(int));
                argnum++;
                break;
            case 'b':
                product_edition = 0;
                memset(prodUpper, 0, sizeof(prodUpper)); 
                if (strlen(optarg) > 16) {
                  printf("Invalid product edition\n");
                  exit(1);
                }
                for(i = 0; i < strlen(optarg); i++)
                  prodUpper[i]=toupper(optarg[i]);
                if(strcmp(prodUpper,LICENSE_PRODUCT_DLAKE_TEXT) == 0 ) 
                  product_edition=LICENSE_PRODUCT_DLAKE;
                else if(strcmp(prodUpper,LICENSE_PRODUCT_CBANK_TEXT) == 0)
                  product_edition=LICENSE_PRODUCT_CBANK;
		else
		{		
		  printf("Invalid product edition \n");
                  exit(1);
                }
                memcpy(prodInstalled,(void*)&product_edition,sizeof(int));
                argnum++;
                break;
            case 't':
                type= 0;
                memset(typeUpper, 0, sizeof(typeUpper)); 
                if (strlen(optarg) > 16) {
                  printf("Invalid type \n");
                  exit(1);
                }
                for(i = 0; i < strlen(optarg); i++)
                  typeUpper[i]=toupper(optarg[i]);
                if(strcmp(typeUpper,LICENSE_TYPE_DEMO_TEXT) == 0 ) 
                  type=LICENSE_TYPE_DEMO;
                else if(strcmp(typeUpper,LICENSE_TYPE_POC_TEXT) == 0)
                  type=LICENSE_TYPE_POC;
                else if(strcmp(typeUpper, LICENSE_TYPE_PRODUCT_TEXT) == 0)
                  type=LICENSE_TYPE_PRODUCT;
                else if(strcmp(typeUpper, LICENSE_TYPE_INTERNAL_TEXT) == 0)
                  type=LICENSE_TYPE_INTERNAL;
                else
		{
		  printf("Invalid type \n");
                  exit(1);
                }
                memcpy(installType,(void*)&type,sizeof(int));
                argnum++;
                break;
            case 'r':
                strncpy(reserved1,optarg,LICENSE_RESERVED_FIELD1);
                break;
            default:
                printf("\n Invalid Option %c\n", ch);
                printHelp();
                exit(1);
        }
    }
    if(argnum < 6)
    {
        printHelp();
        exit(1);
    }

    char output[MAX_ENC_STR_LEN];
    memset(output,0,MAX_ENC_STR_LEN);
   
    memcpy(output + LICENSE_VERSION_OFFSET, &v, sizeof(short));
    memcpy(output + LICENSE_NAME_OFFSET, customer, LICENSE_CUSTOMER_LEN );
    memcpy(output + LICENSE_NODES_OFFSET , &nodenumber, sizeof(int));

    if (expiredate == 0)  // never expires
    {
        expiredate = LICENSE_NEVER_EXPIRES;
    }
    memcpy(output + LICENSE_EXPIRE_OFFSET , &expiredate , sizeof(int));

    memcpy(output + LICENSE_PACKAGE_OFFSET , packageInstalled , sizeof(int));
    memcpy(output + LICENSE_TYPE_OFFSET , installType, sizeof(int));
    memcpy(output + LICENSE_RESERVED_OFFSET, reserved1, LICENSE_RESERVED_FIELD1);
    if (v > 1)
    {
       memcpy(output + LICENSE_UNIQUEID_OFFSET, &unique_Time,sizeof (long long));
       memcpy(output + LICENSE_UNIQUEID_OFFSET + sizeof (long long), &random,sizeof(int));
       memcpy(output + LICENSE_FEATURE_OFFSET, &feature, LICENSE_FEATURE_LEN);
       memcpy(output + LICENSE_PRODUCT_OFFSET , prodInstalled , sizeof(int));
       memcpy(output + LICENSE_MODULE_OFFSET, module, LICENSE_MODULE_LEN);
    }
    
    //encrpt
#ifdef USING_DES 
    DES_cblock key[1];
    DES_key_schedule key_schedule;

    DES_string_to_key("nonstop2016",key);

    if (DES_set_key_checked(key, &key_schedule) != 0)
      exit(1);

    //DES requires the output buffer len to be  ( inputLen + 7 ) /8 * 8  , i.e. output is longer than input
    size_t lenenc;
    if ( v == 1)
    {
      lenenc = (LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE + LICENSE_RESERVED_FIELD1 +7)/8 * 8;
    }
    else
    {
      lenenc = (LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE + LICENSE_RESERVED_FIELD1 + LICENSE_FEATURE_LEN + LICENSE_UNIQUEID_LEN + LICENSE_MODULE_LEN+ LICENSE_LEFT_LEN + 7)/8 * 8;
    }

    unsigned char *outputenc = (unsigned char*) malloc(lenenc+1);
    memset(outputenc, 0 , sizeof(outputenc) );

    DES_cblock ivec;

    memset((char*)&ivec, 0, sizeof(ivec));
    DES_ncbc_encrypt((const unsigned char *)output, outputenc, lenenc, &key_schedule, &ivec, 1);

    //convert each character into HEX and output 
    for( i=0 ; i < lenenc; i++)
    {
        printf("%.2x",outputenc[i]);
    }
    
#else
    int aesenclen;
    if (v == 1)
    {
      aesenclen = LICENSE_TOTAL_LEN1;
    }
    else
    {
      aesenclen = LICENSE_TOTAL_LEN2;
    }
    
    if ( aesenclen % AES_BLOCK_SIZE == 0) {
        len = aesenclen + 1;
    } else {
        len = ( aesenclen / AES_BLOCK_SIZE + 1) * AES_BLOCK_SIZE;
    } 

    // Generate AES 128-bit key
    for (i=0; i<16; ++i) {
       aeskey[i] = 32 + i;
    }

    for (i=0; i<AES_BLOCK_SIZE; ++i) {
        aesiv[i] = 0;
    } 

    if (AES_set_encrypt_key(aeskey, 128, &aes) < 0) {
        fprintf(stderr, "Unable to set encryption key in AES\n");
        exit(-1);
    }

    unsigned char * encrypt_string  =  (unsigned char*)calloc(aesenclen, sizeof(unsigned char));

    AES_cbc_encrypt(output, encrypt_string, len, &aes, aesiv, AES_ENCRYPT);

    for( i=0 ; i < aesenclen; i++)
    {
        printf("%.2x",encrypt_string[i]);
    }

    
    
#endif 

    return ret;
}
