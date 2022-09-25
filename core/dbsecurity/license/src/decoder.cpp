/* ** All rights reserved by Esgyn Corporation 2015 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/des.h>
#include <openssl/aes.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "decoderlib.h"


void printHelp()
{
    printf("\
decoder -u <cluster ID file>            (INPUT: the file containing the unique id)\n\
        -s <encrypted license string>   (INPUT: the encoded string)\n\
        -f <license file>               (INPUT: the file containing an encoded license string)\n\
        -c                              (OUTPUT: the customer string)\n\
        -n                              (OUTPUT: the number of node)\n\
        -p                              (OUTPUT: the package installed)\n\
        -b                              (OUTPUT: the product edition installed)\n\
        -r                              (OUTPUT: the content of the reserved field)\n\
        -t                              (OUTPUT: the type)\n\
        -v                              (OUTPUT: the version)\n\
        -e                              (OUTPUT: the expire date, an integer of days after 1970)\n\
        -z                              (OUTPUT: expiration status using the current date\n\
        -x                              (OUTPUT: the feature code\n\
        -i                              (OUTPUT: whether the cluster ID matches license (requires -f and -u)\n\
        -m                              (OUTPUT: the module code\n\
        -a                              (OUTPUT: display all license info\n\
        -j                              (OUTPUT: display all license info in json format)\n");
}

bool validationSuccess(int version, int packageinstalled, int installtype)
{
   if ((version < LICENSE_LOWEST_SUPPORTED_VERSION) || (version > LICENSE_HIGHEST_SUPPORTED_VERSION)) 
       return false;
   
   if ((packageinstalled != LICENSE_PACKAGE_ENT) && (packageinstalled != LICENSE_PACKAGE_ADV))
     return false;
   
   if ((installtype != TYPE_DEMO) &&
       (installtype != TYPE_POC) &&
       (installtype != TYPE_PRODUCT) &&
       (installtype != TYPE_INTERNAL))
     return false;
   
   return true;
}

int main(int argc, char *argv[])
{
    int ret = 0;
    int ch = 0;
    int i = 0;
    int v = 0;
    int n = 0;
    int argnum=0;
    int copt=0;
    int sopt=0;
    int nopt=0;
    int eopt=0;
    int topt=0;
    int popt=0;
    int vopt=0;
    int fopt=0;
    int ropt=0;
    int bopt=0;
    int aopt=0;
    int zopt=0;
    int gopt=0;
    int uopt=0;
    int iopt=0;
    int jopt=0;
    int mopt=0;
    short xopt = 0;
    char encfile[1024];
    char reserved[LICENSE_RESERVED_FIELD1+1];
    int nodenumber = 0;
    time_t exday = 0;
    int fd = 0;
    short vernum = 0;
    int packageinstalled = 0;
    int prodinstalled = 0;
    int installtype=0;
    char encstr[LICENSE_ENC_LEN2+1];  // MAX
    char uniqueId[LICENSE_UNIQUEID_LEN+1];
    memset(encstr, 0, sizeof(encstr));
    memset(uniqueId, 0, sizeof(uniqueId));
    memset(encfile, 0, sizeof(encfile));
    FILE *pFile;
                
    while((ch=getopt(argc,argv,"gacnetpivrbxmzjf:s:u:"))!=-1)
    {
        switch(ch)
        {
            // special case
            case 'g':
                gopt=1;
                argnum++;
                break;
	    case 'x':
                xopt=1;
                argnum++;
                break;       
	    case 'm':
                mopt=1;
                argnum++;
                break;
            case 'z':
                zopt=1;
                argnum++;
                break;
            case 's':
                strncpy(encstr,optarg,LICENSE_ENC_LEN2);
                sopt=1;
                argnum++;
                break;
            case 'c':
                copt=1;
                argnum++;
                break;
            case 'n':
                nopt=1;
                argnum++;
                break;
            case 't':
                topt=1;
                argnum++;
                break;
            case 'p':
                popt=1;
                argnum++;
                break;
            case 'e':
                eopt=1;
                argnum++;
                break;
            case 'v':
                vopt=1;
                argnum++;
                break;
            case 'r':
                ropt=1;
                argnum++;
                break;
            case 'b':
                bopt=1;
                argnum++;
                break;
            case 'j':
                jopt=1;
                argnum++;
                break;
            case 'f':
                strcpy(encfile,optarg); 
                pFile = fopen(encfile, "r" );
                
                if ( pFile )
                {
                     int   bytesRead = 0;
                     bytesRead = fread (encstr,sizeof(char), sizeof(encstr),pFile);
                     fclose(pFile);
                     if (bytesRead == 0)
                     {
                         printf("\nInvalid License File\n");
                         return 1;
                     }
                }
                else
                {
                     printf("\nInvalid License File\n");
                     return 1;
                }

                fopt=1;
                argnum++;
                break;
            case 'u':
                strcpy(encfile,optarg); 
                pFile = fopen(encfile, "r" );
                
                if ( pFile )
                {
                     int   bytesRead = 0;
                     bytesRead = fread (uniqueId,sizeof(char), sizeof(uniqueId),pFile);
                     fclose(pFile);
                     if (bytesRead == 0)
                     {
                         printf("\nInvalid ID File\n");
                         return 1;
                     }
                }

                uopt=1;
                argnum++;
                break;
            case 'i':
                iopt = 1;
                argnum++;
                break;
            case 'a':
                aopt = 1;
                argnum++;
                break;
            default:
                printHelp();
                exit(1);
        }
    }
    if(iopt == 0 && (argnum < 1 || argnum > 2))
    {
        printHelp();
        exit(1);
    }


    // Deal with special cases first
    if (gopt == 1)
    {
        struct timespec currTime;
        clock_gettime(CLOCK_REALTIME, &currTime);
	srand (time (NULL));
        int randomNum = rand() % 1000 + 1;  // random number between 1 and 1000
        memcpy (uniqueId, &currTime.tv_sec, sizeof (long long));
        memcpy (uniqueId + sizeof (long long), &randomNum, sizeof (int));
        
        //output (to installer)
        for( i=0 ; i < LICENSE_UNIQUEID_LEN; i++)
        {
            printf("%c",uniqueId[i]);
        }
        return 0;
    }
    else if (uopt == 1 && iopt == 0)
    {
        long long tempTime;
        int tempRandom;
        memcpy ((void*)&tempTime, &uniqueId, sizeof (long long));
        memcpy ((void *)&tempRandom, (char*)(uniqueId + sizeof(long long)), sizeof (int));
        printf("\nUnique time in seconds %lld", tempTime);
        printf("\nRandom number %d\n", tempRandom);
        return 0;
    }
    
    int inputlen = strlen(encstr); 
    short version;

    unsigned char * decrypt_string = decode (strlen (encstr), sizeof(encstr), encstr);
    memcpy(&version, (char*)decrypt_string + LICENSE_VERSION_OFFSET,  sizeof(short));
    memcpy(&packageinstalled, (char*)decrypt_string+ LICENSE_PACKAGE_OFFSET,  sizeof(int));
    memcpy(&installtype, (char*)decrypt_string+ LICENSE_TYPE_OFFSET,  sizeof(int));
    memcpy(&nodenumber, (char*)decrypt_string+ LICENSE_NODES_OFFSET ,  sizeof(int));
    memcpy(&exday, (char*)decrypt_string+ LICENSE_EXPIRE_OFFSET,  sizeof(int));
 
    if (!validationSuccess(version, packageinstalled, installtype))
    {
         printf("\nInvalid License\n");
	 return 1;
    }
    
    if (zopt == 1)
    {
        struct timespec currTime;
        clock_gettime(CLOCK_REALTIME, &currTime);
        memcpy(&exday, (char*)decrypt_string+ LICENSE_EXPIRE_OFFSET,  sizeof(int));
        if (exday == 99999)
        {
           printf("\nNO EXPIRATION\n\n");
        }
        else
        {
           time_t timetoepoc = (time_t)(exday * 86400);

           if (currTime.tv_sec > timetoepoc) {
              printf("\n***ERROR*** LICENSE EXPIRED\n\n");
              return 2;
           } else
           {
              time_t daystoexpire = (time_t)(timetoepoc-currTime.tv_sec);
              daystoexpire = (time_t)(daystoexpire/86400);
              printf("\nLICENSE EXPIRING in %ld days\n\n", daystoexpire);

           }
        }

         return 0;
      }

    
    //parse the encstr
    char display[LICENSE_ENC_LEN2]; memset(display, 0, sizeof(display)); 
    
    if(aopt == 1 || jopt == 1) //print all
    {
        if(jopt == 1)
           printf("{ ");

        strncpy(display, (char*)decrypt_string+ LICENSE_NAME_OFFSET,  LICENSE_CUSTOMER_LEN);
        for(i = 0 ; i < strlen(display) ; i ++)
        { if(display[i] != ' ' ) break; }

        if(jopt == 1)
           printf("\"%s\" : \"%s\"", "licensed_to", display+i);
        else
           printf("%s : %s\n", "Licensed To", display+i);

        if(jopt == 1)
           printf(", \"%s\" : %d","licensed_nodes", nodenumber);
        else
           printf("%s : %d nodes(s)\n", "Licensed for", nodenumber);

        if (version > 1)
        {
            memcpy((void *)&prodinstalled, (char*)decrypt_string+ LICENSE_PRODUCT_OFFSET,  sizeof(int));

            char ptypebuf[32];
            memset(ptypebuf, 0, sizeof(ptypebuf));
            switch(prodinstalled)
            {
               case LICENSE_PRODUCT_DLAKE:
                  sprintf(ptypebuf, "Data Lake");
                  break;
               case LICENSE_PRODUCT_CBANK:
                 sprintf(ptypebuf, "Core Banking");
                 break;
               default:
                 sprintf(ptypebuf, "UNKNOWN");
            } 
            if(jopt == 1) 
               printf(", \"%s\" : \"%s\"", "licensed_product", ptypebuf);
            else
               printf("%s : %s\n", "Licensed Product", ptypebuf);
        }

	char ptbuf[30];
        memset (ptbuf, 0, sizeof(ptbuf));
        switch(packageinstalled)
        {
          case LICENSE_PACKAGE_ENT:
	    sprintf(ptbuf, "Enterprise");
            break;
          case LICENSE_PACKAGE_ADV:
	    sprintf(ptbuf, "Enterprise Advanced");
            break;
          default:
	    sprintf(ptbuf, "UNKNOWN");
        }
        if(jopt == 1) 
           printf(", \"%s\" : \"%s\"", "licensed_package", ptbuf);
        else
           printf("%s : %s\n", "Licensed Package", ptbuf);

	char ltbuf[12];
        memset (ltbuf, 0, sizeof(ltbuf));
        switch(installtype) 
        {
          case LICENSE_TYPE_DEMO:
	    sprintf(ltbuf, "Demo");
            break;
          case LICENSE_TYPE_POC:
	    sprintf(ltbuf, "POC");
            break;
          case LICENSE_TYPE_PRODUCT:
	    sprintf(ltbuf, "Product");
            break;
          case LICENSE_TYPE_INTERNAL:
	    sprintf(ltbuf, "Internal");
            break;
          default:
	    sprintf(ltbuf, "UNKNOWN");
        }
        if(jopt == 1) 
           printf(", \"%s\" : \"%s\"", "license_type", ltbuf);
        else
           printf("%s : %s\n", "License Type", ltbuf);

        char tmbuf[32];
        memset(tmbuf, 0, sizeof(tmbuf));
	if (exday == LICENSE_NEVER_EXPIRES)
	{
	    sprintf(tmbuf, "No Expiration"); 
	}
	else
	{
	    time_t timetoepoc = exday * 3600 * 24;
            struct tm *nowtm;
            nowtm = localtime(&timetoepoc);
            strftime(tmbuf, sizeof(tmbuf), "%Y-%m-%d", nowtm);
	}
        if(jopt == 1){
           printf(", \"%s\" : \"%s\"", "expiry_date", tmbuf);
           printf(", \"%s\" : %d", "expiry_days", exday);
        }
        else
           printf("%s : %s\n", "Expiry Date", tmbuf);
	
        if (version > 1)
        {
            memset(display, 0, sizeof(display)); 
            strncpy(display, (char*)decrypt_string+  LICENSE_UNIQUEID_OFFSET , LICENSE_UNIQUEID_LEN);
            long long tempTime;
            int tempRandom;
            
            memcpy ((void*)&tempTime, &display, sizeof (long long));            
            memset(display, 0, sizeof(display)); 
            strncpy(display, (char*)decrypt_string+  LICENSE_UNIQUEID_OFFSET + sizeof(long long), sizeof(int));          
            memcpy ((void *)&tempRandom, &display, sizeof (int));
	    if(jopt == 1) 
	      printf(", \"%s\" : \"%lld,%d\"", "cluster_identifier", tempTime, tempRandom);
	    else
	      printf("%s : %lld,%d\n", "Cluster Identifier", tempTime, tempRandom);
	    

 	    memset(display, 0, sizeof(display)); 
            strncpy(display, (char*)decrypt_string+  LICENSE_FEATURE_OFFSET, LICENSE_FEATURE_LEN);
            short feature;
            memcpy ((void*)&feature, &display, LICENSE_FEATURE_LEN);  
            if(jopt == 1) 
               printf(", \"%s\" : %d", "feature_bits", feature);
            else
               printf("%s : %d\n", "Feature Code", feature);
            
            memset(display, 0, sizeof(display)); 
            char module[LICENSE_ENC_LEN2+1]={0};
            int offSet = 0;
	    int round = LICENSE_MODULE_LEN / sizeof(int);
            for(int i = 0;i < round;i++)
            {
                 sprintf (module+offSet,"%u",*(unsigned int*)(decrypt_string+LICENSE_MODULE_OFFSET+(sizeof(int)*i)));
                 offSet += strlen(module + offSet);
                 if(i + 1 >=round)
                 {
                      break;
                 } 
                 sprintf (module+offSet,"%c",LICENSE_SPLIT);
                 offSet += sizeof(char);
            }
            if(jopt == 1) 
               printf(", \"%s\" :\"%s\"", "Module Code",module);
            else
               printf("%s : %s\n", "Module Code", module);

            memset(display, 0, sizeof(display)); 
        }

        if(jopt == 1)
           printf(" }\n");

    }

    if(copt == 1)  //print customer
    {
        strncpy(display, (char*)decrypt_string+ LICENSE_NAME_OFFSET,  LICENSE_CUSTOMER_LEN);
        printf("%s\n",  display);
    }
    else if(vopt == 1)
    {
        printf("%d\n", version); 
    }
    else if(eopt == 1)  //print expire date
    {
        printf("%d\n", (int) exday); 
    }
    else if(nopt == 1)  //print node number
    {
         printf("%d\n",  nodenumber);
    }
    else if(topt == 1)  //print type
    {
        switch(installtype) 
        {
          case LICENSE_TYPE_DEMO:
            printf("%s\n",LICENSE_TYPE_DEMO_TEXT);
            break;
          case LICENSE_TYPE_POC:
            printf("%s\n",LICENSE_TYPE_POC_TEXT);
            break;
          case LICENSE_TYPE_PRODUCT:
            printf("%s\n", LICENSE_TYPE_PRODUCT_TEXT);
            break;
          case LICENSE_TYPE_INTERNAL:
            printf("%s\n", LICENSE_TYPE_INTERNAL_TEXT);
            break;
          default:
            printf("UNKNOWN\n");
        }
    }
    else if(popt == 1)  //print package
    {
        switch(packageinstalled)
        {
          case LICENSE_PACKAGE_ENT:
            printf("%s\n",LICENSE_PACKAGE_ENT_TEXT);
            break;
          case LICENSE_PACKAGE_ADV:
            printf("%s\n",LICENSE_PACKAGE_ADV_TEXT);
            break;
          default:
            printf("UNKNOWN\n");
        } 
    }
    else if(bopt == 1)  //print product edititon
    {
        memcpy((void *)&prodinstalled, (char*)decrypt_string+ LICENSE_PRODUCT_OFFSET,  sizeof(int));
        switch(prodinstalled)
        {
          case LICENSE_PRODUCT_DLAKE:
            printf("%s\n",LICENSE_PRODUCT_DLAKE_TEXT);
            break;
          case LICENSE_PRODUCT_CBANK:
            printf("%s\n",LICENSE_PRODUCT_CBANK_TEXT);
            break;
          default:
            printf("UNKNOWN\n");
        } 
    }
    else if(ropt == 1) //print reserved field
    {
       memcpy(reserved,(char*)decrypt_string+ LICENSE_RESERVED_OFFSET, LICENSE_RESERVED_FIELD1);
       printf("%s\n",reserved); 
    }
    // special case for dbmanager.  Dont display text, just feature code
    else if (xopt == 1)
    {
         memset(display, 0, sizeof(display)); 
         strncpy(display, (char*)decrypt_string+  LICENSE_FEATURE_OFFSET, LICENSE_FEATURE_LEN);
         short feature;
         memcpy ((void*)&feature, &display, LICENSE_FEATURE_LEN);  
         printf("%d\n",feature); 
    }
    else if (mopt == 1)
    {
         memset(display, 0, sizeof(display)); 
         char module[LICENSE_ENC_LEN2+1]={0};
         int offSet = 0;
	 int round = LICENSE_MODULE_LEN / sizeof(int);
         for(int i = 0;i < round;i++)
         {
             sprintf (module+offSet,"%u",*(unsigned int*)(decrypt_string+LICENSE_MODULE_OFFSET+(sizeof(int)*i)));
             offSet += strlen(module + offSet);
             if(i + 1 >=round)
             {
                  break;
             } 
             sprintf (module+offSet,"%c",LICENSE_SPLIT);
             offSet += sizeof(char);
          }
         printf("%s\n",module); 
    }
    else if (iopt == 1)
    {
      if (uopt == 0)
      {
         printf("\nMissing ID file to compare to license\n");
	 return 1;
      }
      // license ID
      long long licTime;
      int licRandom;

      memset(display, 0, sizeof(display)); 
      strncpy(display, (char*)decrypt_string+  LICENSE_UNIQUEID_OFFSET, LICENSE_UNIQUEID_LEN);
      memcpy ((void*)&licTime, &display, sizeof (long long));
      memset(display, 0, sizeof(display));
      strncpy(display, (char*)decrypt_string+  LICENSE_UNIQUEID_OFFSET + sizeof(long long), sizeof(int));
      memcpy ((void *)&licRandom, &display, sizeof (int));

      // cluster ID
      long long idTime;
      int idRandom;
      memcpy ((void*)&idTime, &uniqueId, sizeof (long long));
      memcpy ((void *)&idRandom, (char*)(uniqueId + sizeof(long long)), sizeof (int));
      if (licTime == idTime && licRandom == idRandom)
      {
        printf("License matches cluster ID\n");
        return 0;
      }
      else
      {
        printf("***ERROR*** LICENSE DOES NOT MATCH CLUSTER ID\n");
        return 2;
      }
    }
    delete decrypt_string;
    return ret;
}
