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

#ifndef _CMP_SEABASE_TENANT_H_
#define _CMP_SEABASE_TENANT_H_

#include "CmpSeabaseDDL.h"
#include "CmpSeabaseDDLmd.h"
#include "ComSmallDefs.h"
#include "dbUserAuth.h"
#include "SchemaDB.h"
#include "NAWNodeSet.h"

// =============================================================================
// *
// * File:         CmpSeabaseTenant.h
// * Description:  Implements tenant DDL features for EsgynDB
// *
// * Contains:
// *   Metadata description
// *   CmpSeabaseDDL tenant features
// *   Classes to support tenant features
// * 
// =============================================================================


// *****************************************************************************
// Classes:
// *****************************************************************************
class TenantResources;
class TenantResourceUsage;
class TenantResourceUsageList;
class TenantUsage;
class TenantUsageList;
class TenantInfo;
class TenantSchemaInfo;
class TenantSchemaInfoList;
class TenantNodeInfo;
class TenantNodeInfoList;
class CmpSeabaseDDLtenant;


// *****************************************************************************
// Metadata definitions for schema SEABASE_TENANT_SCHEMA:
// *****************************************************************************
static const QString seabaseTenantsDDL[] =
{ 
  {" create table " SEABASE_TENANTS" "},
  {" ( "},
  {"  tenant_id int not null not serialized, "},
  {"  admin_role_id int not null not serialized, "},
  {"  default_schema_uid largeint not null not serialized, "},
  {"  affinity int not null not serialized, "},
  {"  cluster_size int not null not serialized, "}, 
  {"  tenant_size int not null not serialized, "},
  {"  tenant_details varchar (3000) not null not serialized, "},
  {"  flags largeint not null not serialized "},
  {" ) "},
  {" primary key (tenant_id) "},
  {" attribute hbase format storage hbase "},
  {" ; "}
};

static const QString seabaseTenantUsageDDL[] =
{
  {" create table " SEABASE_TENANT_USAGE" "},
  {" ( "},
  {"  usage_uid largeint not null not serialized, "},
  {"  tenant_id int not null not serialized, "},
  {"  usage_type char(2) character set iso88591 not null not serialized, "},
  {"  flags largeint not null not serialized "},
  {" ) "},
  {" primary key (tenant_id, usage_uid) "},
  {" attribute hbase format storage hbase "},
  {" ; "}
};

static const QString seabaseResourcesDDL[] =
{
  {" create table " SEABASE_RESOURCES" "},
  {" ( "},
  {"   resource_uid largeint not null not serialized, "},
  {"   resource_name varchar(256 bytes) character set utf8 not null not serialized, "},
  {"   resource_type char(2) character set iso88591 not null not serialized, "},
  {"   resource_creator int unsigned not null not serialized, "},
  {"   resource_is_valid char(2) character set iso88591 not null not serialized, "},
  {"   resource_create_time largeint not null not serialized, "},
  {"   resource_redef_time largeint not null not serialized, "},
  {"   resource_details1 varchar(256 bytes) character set utf8 not null not serialized, "},
  {"   resource_details2 varchar (3000) not null not serialized, "},
  {"   flags largeint not null not serialized "},
  {" ) "},
  {" primary key (resource_uid) "},
  {" attribute hbase format storage hbase "},
  {" ; "}
};

static const QString seabaseResourceUsageDDL[] =
{
  {" create table " SEABASE_RESOURCE_USAGE" "},
  {" ( "},
  {"   resource_uid largeint not null not serialized, "},
  {"   resource_name varchar(256 bytes) character set utf8 not null not serialized, "},
  {"   usage_uid largeint not null not serialized, "},
  {"   usage_name varchar(256 bytes) character set utf8 not null not serialized, "},
  {"   usage_type char(2) character set iso88591 not null not serialized, "},
  {"   usage_value largeint not null not serialized, "},
  {"   flags largeint not null not serialized "},
  {" ) "},
  {" primary key (resource_uid, usage_uid) "},
  {" attribute hbase format storage hbase "},
  {" ; "}
};

static const MDTableInfo allMDtenantsInfo[] = {
  {SEABASE_TENANTS,
   seabaseTenantsDDL, sizeof(seabaseTenantsDDL),
   NULL, 0, FALSE, TRUE},

  {SEABASE_TENANT_USAGE,
   seabaseTenantUsageDDL, sizeof(seabaseTenantUsageDDL),
   NULL, 0, FALSE, TRUE},

  {SEABASE_RESOURCES,
   seabaseResourcesDDL, sizeof(seabaseResourcesDDL),
   NULL, 0, FALSE, TRUE},

  {SEABASE_RESOURCE_USAGE,
   seabaseResourceUsageDDL, sizeof(seabaseResourceUsageDDL),
   NULL, 0, FALSE, TRUE},
};


// *****************************************************************************
// Class TenantResourceUsage
//   Interface to the RESOURCE_USAGE table
//   Usages:
//       resource group -> node
//       node -> tenant
// *****************************************************************************
class TenantResourceUsage
{
  public:
  
  enum ResourceUsageType { RESOURCE_USAGE_UNKNOWN = 0,
                           RESOURCE_USAGE_NODE    = 1,
                           RESOURCE_USAGE_TENANT  = 2
                         };
  
  TenantResourceUsage()
  : resourceUID_(0),
    usageUID_(0),
    usageType_(RESOURCE_USAGE_UNKNOWN),
    usageValue_(0),
    flags_(0),
    isNew_(false),
    isObsolete_(false)
  {}

  TenantResourceUsage( const Int64 resourceUID,
                       const NAString &resourceName,
                       const Int64 usageUID,
                       const NAString &usageName,
                       const ResourceUsageType usageType,
                       const Int64 usageValue = -1 )
  : resourceUID_(resourceUID),
    resourceName_(resourceName),
    usageUID_(usageUID),
    usageName_(usageName),
    usageType_(usageType),
    usageValue_(usageValue),
    flags_(0),
    isNew_(false),
    isObsolete_(false)
  {}
  
  TenantResourceUsage(const TenantResourceUsage &other)
  {
    isNew_        = other.isNew_;
    flags_        = other.flags_;
    resourceName_ = other.resourceName_;
    resourceUID_  = other.resourceUID_;
    usageName_    = other.usageName_;
    usageType_    = other.usageType_;
    usageUID_     = other.usageUID_;
    usageValue_   = other.usageValue_;
  }

  virtual ~TenantResourceUsage(){};

  static ResourceUsageType getUsageTypeAsEnum(NAString &usageType);
  static NAString          getUsageTypeAsString(ResourceUsageType usageType);

  const Int64             getFlags()        { return flags_; }
  const NAString          getResourceName() { return resourceName_; }
  const Int64             getResourceUID()  { return resourceUID_; }
  const NAString          getUsageName()    { return usageName_; }
  const ResourceUsageType getUsageType()    { return usageType_; }
  const Int64             getUsageUID()     { return usageUID_; }
  const Int64             getUsageValue()   { return usageValue_; }

  bool isNew()         {return isNew_; }
  bool isObsolete()    {return isObsolete_; }
  bool isNodeUsage()   {return usageType_ == RESOURCE_USAGE_NODE; }
  bool isTenantUsage() {return usageType_ == RESOURCE_USAGE_TENANT; }

  const void setFlags(Int64 flags)                      { flags_ = flags; }
  const void setIsNew(bool isNew)                       { isNew_ = isNew; }
  const void setIsObsolete(bool isObsolete)             { isObsolete_ = isObsolete; }
  const void setResourceName(NAString &resourceName)    { resourceName_ = resourceName; }
  const void setResourceUID(Int64 resourceUID)          { resourceUID_ = resourceUID; }
  const void setUsageName(NAString &usageName)          { usageName_ = usageName; }
  const void setUsageType (ResourceUsageType usageType) { usageType_ = usageType; }
  const void setUsageUID(Int64 usageUID)                { usageUID_ = usageUID; }
  const void setUsageValue(Int64 usageValue)            { usageValue_ = usageValue; }

  Int32 deleteRow(ExeCliInterface *cliInterface);
  Int32 insertRow(ExeCliInterface *cliInterface);
  Int32 updateRow(ExeCliInterface *cliInterface);

  private:

  Int64                  flags_;
  NAString               resourceName_;
  Int64                  resourceUID_;
  NAString               usageName_;
  ResourceUsageType      usageType_;
  Int64                  usageUID_;
  Int64                  usageValue_;
  bool                   isNew_;
  bool                   isObsolete_;

};


// *****************************************************************************
// Class TenantResourceUsageList
//   List of TenantResourceUsage
// *****************************************************************************
class TenantResourceUsageList : public LIST(TenantResourceUsage *)
{
  public:

  // constructor
  TenantResourceUsageList(ExeCliInterface *cliInterface,
                          NAHeap *heap)
   : LIST(TenantResourceUsage *)(heap),
     cliInterface_ (cliInterface),
     heap_(heap)
  {}

  // virtual destructor
  virtual ~TenantResourceUsageList();

  ExeCliInterface     * getCli() { return cliInterface_; }
  NAHeap              * getHeap () { return heap_; }

  const void setCli   (ExeCliInterface *cli)   { cliInterface_ = cli; }
  const void setHeap  (NAHeap *heap)           { heap_ = heap; }

  inline void resetValue(bool obsolete = false)
  {
    for (Int32 i = 0; i < entries(); i++)
    {
       TenantResourceUsage *rusage = (*this)[i];
       rusage->setUsageValue(-1);
       if (obsolete)
         rusage->setIsObsolete(true);
    }
  }

  Int64 deleteUsages(const NAString &whereClause);
  short fetchUsages(const NAString &whereClause, const NAString &orderByClause);
  TenantResourceUsage * findResource(
    const NAString *resourceName,
    const Int64 usageUID,
    const TenantResourceUsage::ResourceUsageType &resourceType);
  TenantResourceUsage * findUsage(
    const NAString *usageName,
    const TenantResourceUsage::ResourceUsageType &usageType);
  Int32 getNumNodeUsages();
  Int32 getNumTenantUsages();
  short insertUsages();
  short modifyUsages();
  short updateUsages();
  void updateUsageValue(
   const NAString &resourceName, 
   const NAString &usageName, 
   TenantResourceUsage::ResourceUsageType usageType,
   Int64 newUsageValue);

  private:

  ExeCliInterface *cliInterface_;
  NAHeap        *heap_;

}; // class TenantResourceUsageList


// *****************************************************************************
// Class TenantResource
//   Interface to the RESOURCES table
//   Two resources are supported:
//      resource group
//      node
// *****************************************************************************
class TenantResource
{
  public:

  enum  TenantResourceType { RESOURCE_TYPE_UNKNOWN = 0,
                             RESOURCE_TYPE_NODE = 1,
                             RESOURCE_TYPE_GROUP = 2 
                           };

  TenantResource()
  : cliInterface_(NULL),
    flags_(0),
    createTime_(0),
    creator_ (NA_UserIdDefault),
    isValid_ (true),
    redefTime_ (0),
    type_(TenantResourceType::RESOURCE_TYPE_UNKNOWN),
    UID_ (0),
    heap_(NULL),
    usageList_(NULL),
    details1_(NAString(" ")),
    details2_(NAString(" "))
  {}

  TenantResource(ExeCliInterface *cliInterface,
                 NAHeap * heap)
  : cliInterface_(cliInterface),
    flags_(0),
    createTime_(0),
    creator_ (NA_UserIdDefault),
    isValid_ (true),
    redefTime_ (0),
    type_(TenantResourceType::RESOURCE_TYPE_UNKNOWN),
    UID_ (0),
    heap_ (heap),
    usageList_(NULL),
    details1_(NAString(" ")),
    details2_(NAString(" "))
  {}

  virtual ~TenantResource(void)
  {
    if (usageList_)
    {
      NADELETE(usageList_, TenantResourceUsageList, usageList_->getHeap());
      usageList_ = NULL;
    }
  }

  static TenantResourceType getResourceTypeAsEnum(NAString &resourceType);
  static NAString           getResourceTypeAsString(TenantResourceType resourceType);
  static bool               validNodeNames(ConstStringList *usageList, NAString &invalidNames);

  ExeCliInterface         *getCli()              { return cliInterface_; }
  const Int64              getCreateTime()       { return createTime_; }
  const Int32              getCreator()          { return creator_; }
  const NAString          &getDetails1()         { return details1_; }
  const NAString          &getDetails2()         { return details2_; }
  const NAString          &getResourceName()     { return name_; }
  const Int64              getResourceUID()      { return UID_; }
  NAHeap                  *getHeap()             { return heap_; }
  const Int64              getRedefTime()        { return redefTime_; }
  TenantResourceUsageList *getUsageList()        { return usageList_; };
  const TenantResourceType getType()             { return type_; } 
  const bool               isValid()             { return isValid_; }

  const void setCli          (ExeCliInterface *cli)    { cliInterface_ = cli; }
  const void setCreateTime   (Int64 createTime)        { createTime_ = createTime; }
  const void setCreator      (Int32 creator)           { creator_ = creator; }
  const void setDetails1     (NAString &details1)      { details1_ = details1; }
  const void setDetails2     (NAString &details2)      { details2_ = details2; }
  const void setFlags        (Int64 flags)             { flags_ = flags; }
  const void setResourceName (NAString &resourceName)  { name_ = resourceName; }
  const void setResourceUID  (Int64 resourceUID)       { UID_ = resourceUID; }
  const void setHeap         (NAHeap *heap)            { heap_ = heap; }
  const void setIsValid      (bool isValid)            { isValid_ = isValid; }
  const void setRedefTime    (Int64 redefTime)         { redefTime_ = redefTime; }
  const void setType         (TenantResourceType type) { type_ = type; }

  short alterNodesForRGroupTenants(
    ExeCliInterface *cliInterface,
    const NAList<TenantInfo *> *tenantInfoList,
    const NAList<NAString> &deletedNodes);

  bool assignedTenant(const NAString &tenantName);

  short backoutAssignedNodes(
    ExeCliInterface *cliInterface,
    const NAList<TenantInfo *> &tenantInfoList);

  short createStandardResources();
  bool  describe(const NAString &resourceName, NAString &resourceText);
  short generateNodeResource(NAString *nodeName, const Int32 grantee);

  short getNodesToDropFromTenant(
    ExeCliInterface *cliInterface,
    const TenantInfo *tenantInfo,
    const NAList<NAString> &deletedNodes,
    NAList<NAString> &nodesNotFound);

  short getTenantsForNodes(
    const NAList<NAString> &nodeList, 
    NAString &tenantNames);

  short getTenantsForResource(NAString &tenantNames);

  short getTenantInfoList(
    const NAString &tenantNames,
    NAList<TenantInfo *> &tenantInfoList);

  short removeObsoleteNodes(const TenantResourceUsageList *usageList);

  short deleteRow();
  short fetchMetadata(
    const NAString &resourceClause, 
    const NAString &resourceUsageClause,
    bool fetchUsages);
  short insertRow();
  short selectRow(const NAString &whereClause);
  short updateRows(const char *setClause, const char *whereClause);
  short updateRedefTime();

  private:

  ExeCliInterface         *cliInterface_;
  Int64                    createTime_;
  Int32                    creator_;
  NAString                 details1_;
  NAString                 details2_;
  Int64                    flags_;
  NAHeap                  *heap_;
  bool                     isValid_;
  NAString                 name_;
  Int64                    redefTime_;
  TenantResourceType       type_;
  Int64                    UID_;
  TenantResourceUsageList *usageList_;
};


// *****************************************************************************
// Class TenantUsage
//   Interface to the TENANT_USAGE table
// Class that corresponds to the TENANT_USAGE table
// *****************************************************************************
class TenantUsage
{
  public:
  
  enum TenantUsageType { TENANT_USAGE_UNKNOWN   = 0,
                         TENANT_USAGE_SCHEMA    = 1,
                         TENANT_USAGE_USER      = 2,
                         TENANT_USAGE_ROLE      = 3,
                         TENANT_USAGE_GROUP     = 4,
                         TENANT_USAGE_RESOURCE  = 5
                       };

  TenantUsage()
  : tenantID_(NA_UserIdDefault),
    tenantUsageID_(0),
    tenantUsageType_(TENANT_USAGE_UNKNOWN),
    flags_(0),
    isNew_(false),
    isObsolete_(false),
    isUnchanged_(false)
  {}

  TenantUsage( const Int32 tenantID,
               const Int64 tenantUsageID,
               const TenantUsageType tenantUsageType )
  : tenantID_(tenantID),
    tenantUsageID_(tenantUsageID),
    tenantUsageType_(tenantUsageType),
    flags_(0),
    isNew_(false),
    isObsolete_(false),
    isUnchanged_(false)
  {}

  TenantUsage(const TenantUsage &other)
  {
    tenantID_        = other.tenantID_;
    tenantUsageID_   = other.tenantUsageID_;
    tenantUsageType_ = other.tenantUsageType_;
    flags_           = other.flags_;
    isNew_           = other.isNew_;
    isObsolete_      = other.isObsolete_;
    isUnchanged_     = other.isUnchanged_;
  }

  virtual ~TenantUsage(void)
  {}

  bool operator==(const TenantUsage& other) const
  {
   //  Check for pathological case of X = X.
   if ( this == &other )
      return true;

   return ( ( tenantID_ == other.tenantID_ ) &&
            ( tenantUsageID_  == other.tenantUsageID_  ) &&
            ( tenantUsageType_ == other.tenantUsageType_ ) );
  }

  static TenantUsageType getUsageTypeAsEnum(NAString usageType);
  static NAString        getUsageTypeAsString(TenantUsage::TenantUsageType usageType);
  static Int32           getTenantUsages(
    const Int32 tenantID,
    NAList<TenantUsage> *& usageList,
    CollHeap *heap);

  const Int32           getTenantID()      { return tenantID_; }
  const Int64           getUsageID()       { return tenantUsageID_; }
  const TenantUsageType getUsageType()     { return tenantUsageType_; }
  const Int64           getFlags()         { return flags_; }

  bool isNew()         {return isNew_; }
  bool isObsolete()    {return isObsolete_; }
  bool isGroupUsage()  {return tenantUsageType_ == TENANT_USAGE_GROUP; }
  bool isRGroupUsage() {return tenantUsageType_ == TENANT_USAGE_RESOURCE; }
  bool isSchemaUsage() {return tenantUsageType_ == TENANT_USAGE_SCHEMA; }  
  bool isUnchanged()   {return isUnchanged_; }

  const void setFlags(Int64 flags)                    { flags_ = flags; }
  const void setIsNew(bool isNew)                     { isNew_ = isNew; }
  const void setIsObsolete(bool isObsolete)           { isObsolete_ = isObsolete; }
  const void setTenantID(Int32 tenantID)              { tenantID_ = tenantID; }
  const void setUnchanged(bool isUnchanged)           { isUnchanged_ = isUnchanged; }
  const void setUsageID(Int64 usageID)                { tenantUsageID_ = usageID; }
  const void setUsageType (TenantUsageType usageType) { tenantUsageType_ = usageType; }

  private:

  Int32                  tenantID_;
  Int64                  tenantUsageID_;
  TenantUsageType        tenantUsageType_;
  Int64                  flags_;
  bool                   isNew_;
  bool                   isObsolete_;
  bool                   isUnchanged_;

};

// *****************************************************************************
// Class TenantUsageList
//   List of TenantUsage
// *****************************************************************************
class TenantUsageList : public LIST(TenantUsage)
{
  public:

  // constructor
  TenantUsageList( NAHeap *heap = NULL)
   : LIST(TenantUsage)(heap),
     heap_(heap)
  {}

  // virtual destructor
  virtual ~TenantUsageList(){};

  const void setHeap  (NAHeap *heap)           { heap_ = heap; }
  inline NAHeap * getHeap() { return heap_; }


  NABoolean findTenantUsage (
    const Int64 tenantUsageID,
    TenantUsage &tenantUsage) const;

  Int32 getNewSchemaUsages();

  Int32 getNumberUsages(
    const TenantUsage::TenantUsageType &usageType,
    bool skipObsolete) const;

  bool hasSchemaUsages() const
  { 
    return (getNumberUsages(TenantUsage::TENANT_USAGE_SCHEMA, false) > 0);
  }

  void obsoleteTenantRGroupUsages();

  Int64 possibleDefSchUID() const;

  void setTenantUsageObsolete (
    const Int64 tenantUsageID,
    const bool obsolete = true);

  Int32 deleteUsages(const char * whereClause);
  Int32 insertUsages();
  Int32 selectUsages(
    const Int32 tenantID,
    NAHeap *heap);


  private:

  NAHeap *heap_;

}; // class TenantUsageList


// *****************************************************************************
// Class TenantInfo
//   Interface to the TENANTS table
// *****************************************************************************
class TenantInfo
{
  public:

  TenantInfo(NAHeap *heap)
  : tenantID_ (NA_UserIdDefault),
    adminRoleID_ (NA_UserIdDefault),
    defaultSchemaUID_ (NA_UserIdDefault),
    usageList_(NULL),
    tenantDetails_(NULL),
    flags_(0),
    assignedNodes_(NULL),
    origAssignedNodes_(NULL),
    heap_(heap)
  {}
 
  TenantInfo( const Int32 tenantID,
              const Int32 adminRoleID,
              const Int64 defaultSchemaUID,
              TenantUsageList *usageList,
              NAWNodeSet *tenantNodes,
              NAString tenantName,
              NAHeap *heap);

  virtual ~TenantInfo(void)
  { 
    if (tenantDetails_)
      NADELETE(tenantDetails_, NAString, heap_);
    // even though these are NABasicObjects, use NADELETE here,
    // since the regular delete doesn't work if the object was
    // allocated from the system heap via standard operator new
    if (usageList_)
      NADELETE(usageList_, TenantUsageList, heap_);
    usageList_ = NULL;
    if (assignedNodes_)
      NADELETE(assignedNodes_, NAWNodeSet, heap_);
    if (origAssignedNodes_)
      NADELETE(origAssignedNodes_, NAWNodeSet, heap_);
  }

  const Int32 getTenantID()                   { return tenantID_; }
  const NAString getTenantName()              { return tenantName_; }
  const Int32 getAdminRoleID()                { return adminRoleID_; }
  const Int64 getDefaultSchemaUID()           { return defaultSchemaUID_; }
  const TenantUsageList *getUsageList()       { return usageList_; }
  const NAWNodeSet *getTenantNodes()          { return assignedNodes_; }
  const NAWNodeSet *getOrigTenantNodes()      { return origAssignedNodes_; }
  const Int32 getAffinity();
  const Int32 getClusterSize();
  const Int32 getTenantSize();
  const NAString *getTenantDetails()          { return tenantDetails_; }

  const void setTenantID(Int32 tenantID)    { tenantID_ = tenantID; }
  const void setAdminRoleID(Int32 roleID)   { adminRoleID_ = roleID; }
  const void setFlags (Int64 flags)         { flags_ = flags; }
  const void setDefaultSchemaUID(Int64 schemaUID){ defaultSchemaUID_ = schemaUID; }
  const void setTenantUsages(TenantUsageList *usageList) { usageList_ = usageList; }
  const void setTenantDetails(const char *tenantDetails);
  const void setTenantNodes(NAWNodeSet *assignedNodes);
  const void setOrigTenantNodes (NAWNodeSet *assignedNodes);
  const void setTenantName(const char * tenantName) { tenantName_ = tenantName; }

  // Future - save the schema name in class to I/O's if needed multiple times
  NAString getDefaultSchemaName() 
  { 
    NAString schemaName = CmpSeabaseDDLtenant::getSchemaName(defaultSchemaUID_);
    if (schemaName == "?")
      return NAString("");
    return schemaName;
  }

  void updateDefaultSchema(NAString &schema)
  {
    ActiveSchemaDB()->getDefaults().validateAndInsert("SCHEMA", schema, TRUE);
  }

  Int32 getNodeList(TenantResourceUsageList *& nodeList);
  static Int32 getTenantInfo(
    const Int32 authID,
    TenantInfo &tenantInfo);

  Int32 getNumberGroupUsages()
  {
    if (usageList_ && usageList_->entries() > 0)
      return usageList_->getNumberUsages(TenantUsage::TENANT_USAGE_GROUP, false);
    return 0;
  }

  Int32 getNumberRGroupUsages(bool skipObsolete = false) 
  {
    if (usageList_ && usageList_->entries() > 0)
      return usageList_->getNumberUsages(TenantUsage::TENANT_USAGE_RESOURCE, skipObsolete);
    return 0;
  }
 
  Int32 getNumberSchemaUsages()
  {
    if (usageList_ && usageList_->entries() > 0)
      return usageList_->getNumberUsages(TenantUsage::TENANT_USAGE_SCHEMA, false);
    return 0;
  }

  Int32 addTenantInfo(const Int32 tenantSize = -1);
  Int32 deleteUsages(const char * whereClause)
  {
    if (usageList_ && usageList_->entries() > 0)
      return usageList_->deleteUsages(whereClause);
    return 0;
  }
  Int32 dropTenantInfo();

  Int32 dropTenantInfo(
    const bool updateTenant,
    const TenantUsageList *usageList); 

  Int32 modifyTenantInfo(
    const bool updateTenant,
    const TenantUsageList *usageList);

  const NABoolean findTenantUsage (
    const Int64 tenantUsageID,
    TenantUsage &tenantUsage) 
  { return usageList_->findTenantUsage(tenantUsageID, tenantUsage); }

  void obsoleteTenantRGroupUsages()
  {
    if (usageList_ && usageList_->entries() > 0)
      usageList_->obsoleteTenantRGroupUsages();
  }
  void setTenantUsageObsolete (
    const Int64 tenantUsageID,
    const bool obsolete = true)
  {
    if (usageList_ && usageList_->entries() > 0)
      usageList_->setTenantUsageObsolete(tenantUsageID, obsolete);
  }

  Int32 updateTenantInfo(
    const bool updateTenant,
    TenantUsageList *usageList,
    const Int32 tenantSize = -1); 

  bool validateTenantSize();

  void allocTenantNode (
    const NABoolean AStenant,
    const Int32 affinity,
    const Int32 clusterSize,
    const Int32 units,
    TenantNodeInfoList *nodeList,
    NAWNodeSet *&tenantNodes,
    bool skipInvalidWeights = false);
    
  void balanceTenantNodeAlloc(
    const NAString &tenantDefaultSchema,
    const Int32 sessionLimit,
    bool isBackup,
    NAWNodeSet *origTenantNodeSet,
    NAWNodeSet *&tenantNodeSet,
    TenantResourceUsageList *&tenantRUsageList,
    bool &registrationComplete);

  private:

  Int32 selectRow(const std::string &whereClause);
  Int32 deleteRow();
  Int32 insertRow(const Int32 tenantSize = -1);
  Int32 updateRow(const Int32 tenantSize = -1);

  Int32                 tenantID_;
  Int32                 adminRoleID_;
  Int64                 defaultSchemaUID_;
  NAString             *tenantDetails_;
  Int64                 flags_;
  TenantUsageList      *usageList_;
  NAWNodeSet           *assignedNodes_;
  NAWNodeSet           *origAssignedNodes_;
  NAString              tenantName_;
  NAHeap               *heap_;
};


// *****************************************************************************
// Class TenantSchemaInfo
//   Describes tenant schema relationships
// *****************************************************************************
// Class that describes the schema tenant relationship
class TenantSchemaInfo
{
  public:

  TenantSchemaInfo()
  : tenantID_ (NA_UserIdDefault),
    schemaUID_ (0),
    defSch_(FALSE)
  {}

  TenantSchemaInfo(Int32 tenantID, Int64 schemaUID, NABoolean defSch)
  : tenantID_ (tenantID),
    schemaUID_ (schemaUID),
    defSch_(defSch)
  {}

  virtual ~TenantSchemaInfo(void)
  {
  }

  NABoolean operator==(const TenantSchemaInfo* other)
  {
    // tenantID_ and schemaUID_ uniquely define an entry
    // no need to check defSch_
    return ( ( tenantID_ == other->tenantID_ &&
               schemaUID_ == other->schemaUID_) );
  }
    
  const Int32 getTenantID()        { return tenantID_; }
  const Int64 getSchemaUID()       { return schemaUID_; }
  const NABoolean isDefSch()       { return defSch_; }

  Int32 removeSchemaUsage();

  private:

  Int32 tenantID_;
  Int64 schemaUID_;
  NABoolean defSch_;
};


// *****************************************************************************
// Class TenantSchemaInfoList
//   List of TenantSchemaInfo classes
// *****************************************************************************
class TenantSchemaInfoList : public LIST(TenantSchemaInfo *)
{
  public:

  // constructor
  TenantSchemaInfoList(NAHeap *heap)
   : LIST(TenantSchemaInfo *)(heap)
  { }

  // virtual destructor
  virtual ~TenantSchemaInfoList();

  TenantSchemaInfo * find(Int64 schemaUID);
  void getSchemaList(
    Int32 tenantID, 
    NAList<Int64> &schemaList);
  Int32 getTenantID(Int64 schemaUID);

}; // class TenantSchemaInfoList

// *****************************************************************************
// Class: TenantGroupInfo
//   describes tenant user group relationships
// *****************************************************************************
class TenantGroupInfo
{
  public:

  TenantGroupInfo()
  : config_((Int16)DBUserAuth::DefaultConfiguration),
    groupID_ (NA_UserIdDefault),
    tenantID_ (NA_UserIdDefault)
  {}

  TenantGroupInfo(
    Int32 tenantID, 
    const NAString &groupName,
    Int32 groupID,
    Int32 config)
  : config_(config),
    groupID_(groupID),
    groupName_(groupName),
    tenantID_ (tenantID)
  {}

  virtual ~TenantGroupInfo(void) {}

  NABoolean operator==(const TenantGroupInfo* other)
  {
    // tenantID_ and groupID_ uniquely define an entry
    return ( ( tenantID_ == other->tenantID_ &&
               groupID_ == other->groupID_) );
  }

  const Int16    getConfig()       { return config_; }
  const Int32    getGroupID()      { return groupID_; }
  const NAString getGroupName()    { return groupName_; }
  const Int32    getTenantID()     { return tenantID_; }

  private:

  Int16 config_;
  Int32 groupID_;
  NAString groupName_;
  Int32 tenantID_;
};


// *****************************************************************************
// Class:  TenantGroupInfoList
//   describes a list of TenantGroup
// *****************************************************************************
class TenantGroupInfoList : public LIST(TenantGroupInfo *)
{
  public:

  // constructor
  TenantGroupInfoList(NAHeap *heap)
   : LIST(TenantGroupInfo *)(heap),
     heap_(heap)
  {}

  // virtual destructor
  virtual ~TenantGroupInfoList()
  {
    for (CollIndex i = 0; i < entries(); i++)
      NADELETE(operator[](i), TenantGroupInfo, heap_);
    clear();
  }

  inline NAHeap * getHeap() { return heap_; }

  private:
 
  NAHeap *heap_;

}; // class TenantGroupInfoList


// *****************************************************************************
// Class TenantNodeInfo
//    Describes details of nodes for a tenant
// *****************************************************************************
class TenantNodeInfo
{
  public:

  // constructor
  TenantNodeInfo()
  : logicalNodeID_ (-1),
    physicalNodeID_(-1),
    mdNodeUID_(-1),
    nodeWeight_(-1),
    heap_(NULL) {} 

  TenantNodeInfo( const NAString nodeName,
                  const Int64 logicalNodeID,
                  const Int64 physicalNodeID,
                  const Int64 mdNodeUID,
                  const Int32 nodeWeight,
                  NAHeap *heap = NULL)
  : nodeName_(nodeName),
    logicalNodeID_ (logicalNodeID),
    physicalNodeID_(physicalNodeID),
    mdNodeUID_(mdNodeUID),
    nodeWeight_(nodeWeight),
    heap_(heap) {}

  TenantNodeInfo(const TenantNodeInfo *other)
  {
    nodeName_        = other->nodeName_;
    logicalNodeID_   = other->logicalNodeID_;
    physicalNodeID_  = other->physicalNodeID_;
    mdNodeUID_       = other->mdNodeUID_;
    nodeWeight_      = other->nodeWeight_;
  }

  virtual ~TenantNodeInfo() {}

  const NAString   getNodeName() { return nodeName_; }
  const Int64      getLogicalNodeID() { return logicalNodeID_; }
  const Int64      getPhysicalNodeID() { return physicalNodeID_; }
  const Int64      getMetadataNodeID() { return mdNodeUID_; }
  const Int32      getNodeWeight() { return nodeWeight_; }


  private:

  NAString      nodeName_;
  Int64         logicalNodeID_;
  Int64         physicalNodeID_;
  Int64         mdNodeUID_;
  Int32         nodeWeight_;
  NAHeap       *heap_;
};
  
// *****************************************************************************
// Class:  TenantNodeInfoList
//   contains a list of nodes that are assigned to a tenant
// *****************************************************************************
class TenantNodeInfoList : public LIST(TenantNodeInfo *)
{
  public:

  // constructor
  TenantNodeInfoList(NAHeap *heap)
   : LIST(TenantNodeInfo *)(heap),
     heap_(heap)
  {}

  // virtual destructor
  virtual ~TenantNodeInfoList()
  {
    for (CollIndex i = 0; i < entries(); i++)
      NADELETE(operator[](i), TenantNodeInfo, heap_);
    clear();
  }

  inline NAHeap * getHeap() { return heap_; }

  NABoolean contains (const NAString &nodeName);
  Int64 getNodeID(const NAString &nodeName);
  NAString getNodeName(const Int64 logicalNodeID);
  NAString listOfNodes();
  void orderedInsert(TenantNodeInfo *nodeInfo);
  void removeIfExists (const NAString &nodeName);


  private:

  NAHeap *heap_;

}; // class TenantNodeInfoList



#endif
