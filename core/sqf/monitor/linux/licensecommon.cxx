#include <string.h>   //strncpy
#include <time.h>
#include "licensecommon.h"
char CLicenseCommon::uniqueId_[LICENSE_UNIQUEID_LEN] = {'0'};
bool CLicenseCommon::uniqueIdExists_ = false;
time_t CLicenseCommon::timeInitiatedInSecs_ = 0;
static const int MAX_FILE_NAME = 64;
static const int MAX_PROCESS_PATH = 256;
CLicenseCommon::CLicenseCommon()
{    
    struct timespec currTime;

    secsToStartWarning_ = LICENSE_SEVEN_DAYS;
    char *daysToStartWarning = getenv("SQ_DAYS_START_WARNING");
    if (daysToStartWarning)
    {
       secsToStartWarning_ = atoi (daysToStartWarning);
       secsToStartWarning_ = secsToStartWarning_*LICENSE_ONE_DAY; // Convert to seconds       
    }

    action_ = HC_KEEP_WARNING;
    char *actionToTake = getenv("SQ_LICENSE_ACTION");
    if (actionToTake)
    {
       action_ = (HealthCheckLicenseAction)atoi (actionToTake);
    }

    gracePeriod_ = LICENSE_SEVEN_DAYS;
    char *gracePeriod = getenv("SQ_GRACE_PERIOD");
    if (gracePeriod)
    {
       int gracePeriodInDays = atoi (gracePeriod);
       gracePeriod_ = (time_t)(gracePeriodInDays * LICENSE_ONE_DAY);
    }
    
    if (uniqueIdExists_ == false)
    {
       memset (uniqueId_,0, sizeof (uniqueId_));
       char  buffer[MAX_FILE_NAME+MAX_PROCESS_PATH];
       sprintf(buffer, "/etc/trafodion/esgyndb_id");
       FILE *pFile = fopen ( buffer, "r");
       if ( pFile != NULL )
       { 
           fread (uniqueId_,sizeof(char), sizeof(uniqueId_),pFile);
 
#ifdef SQ_MON_VIRTUAL_LICENSE_TESTING
           long long tempTime;
           int tempRandom;
           printf("\nDEBUG read %d bytes, unique id : %.12s\n", bytesRead, uniqueId_);            
           memcpy ((void*)&tempTime, &uniqueId_, sizeof (long long));
           memcpy ((void *)&tempRandom, (char*)(uniqueId_ + sizeof(long long)), sizeof (int));
           printf("\nDEBUG time %lld, randNum %d", tempTime, tempRandom);
#endif  
           fclose(pFile);
           uniqueIdExists_ = true;
       }
    }
    if (uniqueIdExists_ == true)
    {
          memcpy ((void*)&timeInitiatedInSecs_, &uniqueId_, sizeof (long long));
          //printf("\nTRK Unique time in seconds %lld", timeInitiatedInSecs_);
    }
    else
    {
         clock_gettime(CLOCK_REALTIME, &currTime);
         timeInitiatedInSecs_ = currTime.tv_sec;
    }
    numNodes_ = expireDays_ = version_ = features_ = 0;
    memset (customerName_,0, sizeof (customerName_)); 
    memset (modules_,0, sizeof (modules_));
    if (!parseLicense())
    {
        licenseReady_ = false;
    }
    else
    {
        licenseReady_ = true;
    }
}

CLicenseCommon::~CLicenseCommon()
{
  
}
   
bool CLicenseCommon::parseLicense()
{
    FILE *pFile;
    int   bytesRead = 0;
    char  myLicense[LICENSE_NUM_BYTES_ENC_MAX+1]; 
    char  buffer[MAX_FILE_NAME+MAX_PROCESS_PATH];

    memset(myLicense,0,sizeof(myLicense)); 
   
    char *licenseFile = getenv("SQ_MON_LICENSE_FILE");
    if (!licenseFile)
    {
        // let's check to see if it is in /etc/trafodion (the default/standard place)
        sprintf(buffer, "/etc/trafodion/esgyndb_license");
    }
    else
    {
        sprintf (buffer, "%s", licenseFile);
    } 
    pFile = fopen( buffer, "r" );
    if ( pFile )
    {
        bytesRead = fread (myLicense,sizeof(char), sizeof(myLicense),pFile);
        fclose(pFile);
    }
    // allow extra byte for copying license key into file
    if (( bytesRead != LICENSE_NUM_BYTES_ENC1) && ( bytesRead != LICENSE_NUM_BYTES_ENC2)
        &&( bytesRead != (LICENSE_NUM_BYTES_ENC1 + 1)) && ( bytesRead != (LICENSE_NUM_BYTES_ENC2 + 1)))
    {
       return false;
    }

 // Disable the following license code when using DUMA
#if 1
    unsigned char *output = decode (strlen(myLicense)/* bytesRead*/, sizeof(myLicense)/*(bytesRead/8)*/, myLicense);
 
    if (output)
    {
        memcpy ((void*)license_, output,LICENSE_NUM_BYTES_MAX );
        delete [] output;
    }
    else
    {
       return false;
    }
    
    memcpy ((void*)&version_, &(license_[LICENSE_VERSION_OFFSET]), LICENSE_VERSION_LEN);
    memcpy ((void*)&numNodes_, &(license_[LICENSE_NODES_OFFSET]), LICENSE_NODENUM_LEN);
    memcpy ((void*)&expireDays_, &(license_[LICENSE_EXPIRE_OFFSET]), LICENSE_EXPIRE_LEN);
    strncpy (customerName_, (char *)license_ + LICENSE_NAME_OFFSET, LICENSE_CUSTOMER_LEN);
    memcpy ((void*)&package_, &(license_[LICENSE_PACKAGE_OFFSET]), LICENSE_PACKAGE_INSTALLED);
    memcpy ((void*)&type_, &(license_[LICENSE_TYPE_OFFSET]), LICENSE_INSTALL_TYPE);   
    memcpy ((void*)reserved_, (char*)license_+  LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE, LICENSE_RESERVED_FIELD1);
    if (version_ > 1)
    {
        memcpy( (void*)&modules_, (char*)license_+  LICENSE_MODULE_OFFSET, LICENSE_MODULE_LEN ); 
        memcpy( (void*)&features_, (char*)license_+  LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE + LICENSE_RESERVED_FIELD1 + LICENSE_UNIQUEID_LEN, LICENSE_FEATURE_LEN );
        // We only want to check unique id if we are production
        if (type_ == TYPE_PRODUCT)
        {
            char unique_id_license[LICENSE_UNIQUEID_LEN];
            memset( unique_id_license, 0, sizeof (unique_id_license) );
            strncpy( unique_id_license, (char*)license_+  LICENSE_VERSION_LEN + LICENSE_CUSTOMER_LEN + LICENSE_NODENUM_LEN + LICENSE_EXPIRE_LEN + LICENSE_PACKAGE_INSTALLED + LICENSE_INSTALL_TYPE + LICENSE_RESERVED_FIELD1, LICENSE_UNIQUEID_LEN );
            // If the uniqueId doesn't match then this license belongs to another subsystem.  Do not allow
            if (strncmp( unique_id_license, uniqueId_, LICENSE_UNIQUEID_LEN )!= 0)
            {
                return false;
            }
        }
        else if ((type_ == TYPE_INTERNAL) && (features_ == 0))
        {
            features_ = LICENSE_MAX_FEATURES;
        }
    }
#else
    version_    = 2;
    numNodes_   = 1024;
    expireDays_ = LICENSE_NEVER_EXPIRES;
    sprintf(customerName_, "%s", "Esgyn Corporation");
    package_    = LICENSE_PACKAGE_ADV;
    type_       = LICENSE_TYPE_INTERNAL;
    features_   = LICENSE_MAX_FEATURES;
#endif    

   return true;
}
   
bool CLicenseCommon::isInternal()
{
     return( type_ == TYPE_INTERNAL );
}

bool CLicenseCommon::doesLicenseExpire()
{
     if (expireDays_ == LICENSE_NEVER_EXPIRES)
     {
         return false;
     }
     return true;
}
bool CLicenseCommon::isModuleOpen(int ModuleId)
{
    if(ModuleId < 1 || ModuleId > LICENSE_MAX_MODULE || !getLicenseReady())
    {
        return false;   
    }
    int tmpId = 0;
    if (ModuleId %32 == 0)
    {
        memcpy(&tmpId,modules_+(((ModuleId/32) -1 )*sizeof(int)),sizeof(int));
        return ((1<<(32-1)) & tmpId);
    } 
    memcpy(&tmpId, (modules_+((ModuleId/32)*sizeof(int))),sizeof(int));
    return (1<<(((ModuleId % 32)-1))& tmpId);
}
