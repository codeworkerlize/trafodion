/* -*-C++-*-

 ****************************************************************************
 *
 * File:         StmtDDLTenant.cpp
 * Description:  Methods for classes representing DDL tenant statements
 *
 * Language:     C++
 *
 *****************************************************************************
 */

#include "parser/StmtDDLTenant.h"

#include "ElemDDLTenantSchema.h"
#include "parser/StmtDDLRegisterUser.h"

// -----------------------------------------------------------------------
// methods for class StmtDDLTenant
// -----------------------------------------------------------------------

//
// constructor
//
// constructor used for REGISTER TENANT
StmtDDLTenant::StmtDDLTenant(const NAString &tenantName, ElemDDLNode *optionList, ElemDDLNode *schemaList,
                             CollHeap *heap)
    : StmtDDLNode(DDL_REGISTER_TENANT),
      tenantAlterType_(NOT_ALTER),
      tenantName_(tenantName, heap),
      dropDependencies_(FALSE),
      defaultSchema_(NULL),
      roleName_(NULL),
      affinity_(-1),
      balance_(FALSE),
      clusterSize_(0),
      sessionLimit_(-2),
      sessionLimitSpecified_(FALSE),
      tenantAffinitySizing_(FALSE),
      tenantSize_(0),
      tenantSizeSpecified_(FALSE),
      addGroupList_(FALSE),
      groupList_(NULL),
      addRGroupList_(FALSE),
      dropRGroupList_(FALSE),
      replaceRGroupList_(FALSE),
      rgroupList_(NULL),
      addNodeList_(FALSE),
      nodeList_(NULL),
      addSchemaList_(FALSE),
      schemaList_(NULL),
      defaultSchemaSpecified_(FALSE),
      heap_(heap) {
  ElemDDLNode *tempSchemaList = schemaList;
  bool tenantNodeSizing = false;
  bool defaultSpecified = false;
  if (optionList) {
    for (CollIndex i = 0; i < optionList->entries(); i++) {
      ElemDDLTenantOption *option = (*optionList)[i]->castToElemDDLTenantOption();
      assert(option) switch (option->getOptionEnum()) {
        case ElemDDLTenantOption::TENANT_OPT_ADMIN_ROLE:
          roleName_ = new (heap) NAString(option->getOptionValue(), heap);
          break;
        case ElemDDLTenantOption::TENANT_OPT_AFFINITY:
          affinity_ = atoInt64(option->getOptionValue());
          tenantAffinitySizing_ = TRUE;
          break;
        case ElemDDLTenantOption::TENANT_OPT_CLUSTER_SIZE:
          clusterSize_ = atoInt64(option->getOptionValue());
          tenantAffinitySizing_ = true;
          break;
        case ElemDDLTenantOption::TENANT_OPT_BALANCE:
          balance_ = TRUE;
          break;
        case ElemDDLTenantOption::TENANT_OPT_DEFAULT_SCHEMA:
          if (defaultSpecified)
            *SqlParser_Diags << DgSqlCode(-3103) << DgString0("DEFAULT");
          else {
            defaultSchema_ = (SchemaName *)option->getSchema();
            defaultSchemaSpecified_ = TRUE;
            defaultSpecified = true;
          }
          break;
        case ElemDDLTenantOption::TENANT_OPT_SESSIONS:
          sessionLimit_ = atoInt64(option->getOptionValue());
          if (sessionLimitSpecified_)
            *SqlParser_Diags << DgSqlCode(-3103) << DgString0("SESSIONS");
          else {
            sessionLimitSpecified_ = TRUE;
            if (sessionLimit_ < -2) *SqlParser_Diags << DgSqlCode(-3001) << DgString0("SESSIONS");
          }
          break;
        case ElemDDLTenantOption::TENANT_OPT_TENANT_SIZE:
          tenantSize_ = atoInt64(option->getOptionValue());
          if (tenantSizeSpecified_)
            *SqlParser_Diags << DgSqlCode(-3103) << DgString0("TENANT SIZE");
          else
            tenantSizeSpecified_ = TRUE;
          break;
        case ElemDDLTenantOption::TENANT_OPT_ADD_GROUP_LIST:
          groupList_ = (ElemDDLNode *)option->getGroupList();
          addGroupList_ = TRUE;
          break;
        case ElemDDLTenantOption::TENANT_OPT_ADD_SCHEMA_LIST:
          tempSchemaList = (ElemDDLNode *)option->getSchemaList();
          addSchemaList_ = TRUE;
          break;
        case ElemDDLTenantOption::TENANT_OPT_ADD_RGROUP_LIST:
          rgroupList_ = (ElemDDLNode *)option->getRGroupList();
          addRGroupList_ = TRUE;
          tenantNodeSizing = true;
          break;
        case ElemDDLTenantOption::TENANT_OPT_ADD_NODE_LIST:
          nodeList_ = (ConstStringList *)option->getNodeList();
          addNodeList_ = TRUE;
          tenantNodeSizing = true;
          break;
        case ElemDDLTenantOption::TENANT_OPT_ADD_RGROUP_NODES:
          rgroupList_ = (ElemDDLNode *)option->getRGroupList();
          nodeList_ = (ConstStringList *)option->getNodeList();
          tenantNodeSizing = true;
          break;

        case ElemDDLTenantOption::TENANT_OPT_DROP_GROUP_LIST:
        case ElemDDLTenantOption::TENANT_OPT_DROP_SCHEMA_LIST:
        case ElemDDLTenantOption::TENANT_OPT_DROP_RGROUP_LIST:
        case ElemDDLTenantOption::TENANT_OPT_DROP_NODE_LIST:
        case ElemDDLTenantOption::TENANT_OPT_DROP_RGROUP_NODES:
        case ElemDDLTenantOption::TENANT_OPT_REPLACE_RGROUP_LIST:
          *SqlParser_Diags << DgSqlCode(-3001) << DgString0("DROP");
          break;

        case ElemDDLTenantOption::TENANT_OPT_UNKNOWN:
        default:
          break;
      }
    }
  }

  if (tenantAffinitySizing_ && tenantNodeSizing) {
    *SqlParser_Diags << DgSqlCode(-3290);
    return;
  }

  // If no sizing information specified, assume affinity sizing
  if (!tenantAffinitySizing_ && !tenantNodeSizing) tenantAffinitySizing_ = TRUE;

  if (tempSchemaList) {
    schemaList_ = new (heap) LIST(SchemaName *)(heap);
    for (CollIndex i = 0; i < tempSchemaList->entries(); i++) {
      ElemDDLTenantSchema *schema = (*tempSchemaList)[i]->castToElemDDLTenantSchema();
      assert(schema);
      schemaList_->insert((SchemaName *)schema->getSchemaName());
      if (schema->isDefaultSchema()) {
        if (defaultSpecified) {
          *SqlParser_Diags << DgSqlCode(-3103) << DgString0("DEFAULT");
          return;
        } else {
          defaultSpecified = true;
          defaultSchema_ = (SchemaName *)schema->getSchemaName();
        }
      }
    }
  }
}

// constructor used for ALTER TENANT
StmtDDLTenant::StmtDDLTenant(const int alterType, const NAString &tenantName, ElemDDLNode *optionList,
                             SchemaName *tenantSchema, ElemDDLNode *schemaList, CollHeap *heap)
    : StmtDDLNode(DDL_ALTER_TENANT),
      tenantAlterType_((TenantAlterType)alterType),
      dropDependencies_(FALSE),
      tenantName_(tenantName, heap),
      defaultSchema_(NULL),
      roleName_(NULL),
      affinity_(-1),
      balance_(FALSE),
      clusterSize_(-1),
      sessionLimit_(-2),
      sessionLimitSpecified_(FALSE),
      tenantAffinitySizing_(FALSE),
      tenantSize_(-1),
      tenantSizeSpecified_(FALSE),
      addGroupList_(FALSE),
      groupList_(NULL),
      addRGroupList_(FALSE),
      dropRGroupList_(FALSE),
      replaceRGroupList_(FALSE),
      rgroupList_(NULL),
      addNodeList_(FALSE),
      nodeList_(NULL),
      addSchemaList_(FALSE),
      schemaList_(NULL),
      defaultSchemaSpecified_(FALSE),
      heap_(heap) {
  ElemDDLNode *tempSchemaList = schemaList;
  bool defaultSpecified = false;
  switch (tenantAlterType_) {
    case TenantAlterType::ALTER_ADD_SCHEMA:
    case TenantAlterType::ALTER_DROP_SCHEMA: {
      assert(schemaList && schemaList->entries() > 0);
      schemaList_ = new (heap) LIST(SchemaName *)(heap);
      for (CollIndex i = 0; i < schemaList->entries(); i++) {
        ElemDDLTenantSchema *schema = (*schemaList)[i]->castToElemDDLTenantSchema();
        assert(schema);
        if (schema->isDefaultSchema()) {
          if (defaultSpecified)
            *SqlParser_Diags << DgSqlCode(-3103) << DgString0("DEFAULT");
          else {
            defaultSpecified = true;
            defaultSchema_ = (SchemaName *)schema->getSchemaName();
          }
        }
        schemaList_->insert((SchemaName *)schema->getSchemaName());
      }
      break;
    }

    case TenantAlterType::ALTER_TENANT_OPTIONS: {
      NABoolean addList = false;
      NABoolean dropList = false;
      NABoolean modifyList = false;
      for (CollIndex i = 0; i < optionList->entries(); i++) {
        ElemDDLTenantOption *option = (*optionList)[i]->castToElemDDLTenantOption();
        assert(option) switch (option->getOptionEnum()) {
          case ElemDDLTenantOption::TENANT_OPT_ADMIN_ROLE:
            *SqlParser_Diags << DgSqlCode(-3001) << DgString0("ADMIN ROLE");
            break;
          case ElemDDLTenantOption::TENANT_OPT_AFFINITY:
            affinity_ = atoInt64(option->getOptionValue());
            tenantAffinitySizing_ = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_BALANCE:
            balance_ = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_CLUSTER_SIZE:
            clusterSize_ = atoInt64(option->getOptionValue());
            tenantAffinitySizing_ = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_DEFAULT_SCHEMA:
            if (defaultSpecified)
              *SqlParser_Diags << DgSqlCode(-3103) << DgString0("DEFAULT");
            else {
              defaultSchema_ = (SchemaName *)option->getSchema();
              defaultSpecified = true;
              defaultSchemaSpecified_ = TRUE;
            }
            break;
          case ElemDDLTenantOption::TENANT_OPT_SESSIONS:
            sessionLimit_ = atoInt64(option->getOptionValue());
            if (sessionLimitSpecified_)
              *SqlParser_Diags << DgSqlCode(-3103) << DgString0("SESSIONS");
            else {
              sessionLimitSpecified_ = TRUE;
              if (sessionLimit_ < -2) *SqlParser_Diags << DgSqlCode(-3001) << DgString0("SESSIONS");
            }
            break;
          case ElemDDLTenantOption::TENANT_OPT_TENANT_SIZE:
            tenantSize_ = atoInt64(option->getOptionValue());
            if (tenantSizeSpecified_)
              *SqlParser_Diags << DgSqlCode(-3103) << DgString0("SESSIONS");
            else
              tenantSizeSpecified_ = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_ADD_GROUP_LIST:
            groupList_ = (ElemDDLNode *)option->getGroupList();
            addGroupList_ = TRUE;
            addList = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_DROP_GROUP_LIST:
            groupList_ = (ElemDDLNode *)option->getGroupList();
            dropList = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_ADD_RGROUP_LIST:
            rgroupList_ = (ElemDDLNode *)option->getRGroupList();
            addRGroupList_ = TRUE;
            addList = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_DROP_RGROUP_LIST:
            rgroupList_ = (ElemDDLNode *)option->getRGroupList();
            dropRGroupList_ = TRUE;
            dropList = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_REPLACE_RGROUP_LIST:
            rgroupList_ = (ElemDDLNode *)option->getRGroupList();
            replaceRGroupList_ = TRUE;
            modifyList = TRUE;
            break;
          case ElemDDLTenantOption::TENANT_OPT_ADD_SCHEMA_LIST:
          case ElemDDLTenantOption::TENANT_OPT_DROP_SCHEMA_LIST: {
            tempSchemaList = (ElemDDLNode *)option->getSchemaList();
            assert(tempSchemaList && tempSchemaList->entries() > 0);
            schemaList_ = new (heap) LIST(SchemaName *)(heap);
            for (CollIndex i = 0; i < tempSchemaList->entries(); i++) {
              ElemDDLTenantSchema *schema = (*tempSchemaList)[i]->castToElemDDLTenantSchema();
              assert(schema);
              if (schema->isDefaultSchema()) {
                if (defaultSpecified)
                  *SqlParser_Diags << DgSqlCode(-3103) << DgString0("DEFAULT");
                else {
                  defaultSpecified = TRUE;
                  defaultSchema_ = (SchemaName *)schema->getSchemaName();
                }
              }
              schemaList_->insert((SchemaName *)schema->getSchemaName());
            }
            if (option->getOptionEnum() == ElemDDLTenantOption::TENANT_OPT_ADD_SCHEMA_LIST) {
              addSchemaList_ = true;
              addList = TRUE;
            } else
              dropList = TRUE;
            break;
          }
          case ElemDDLTenantOption::TENANT_OPT_UNKNOWN:
          default:
            break;
        }
        if (addList && dropList || addList && modifyList || dropList && modifyList)
          *SqlParser_Diags << DgSqlCode(-3009);
      }
      break;
    }

    default:
      schemaList_ = NULL;
  }
}

// constructor used for UNREGISTER TENANT
StmtDDLTenant::StmtDDLTenant(const NAString &tenantName, const NABoolean dropDependencies, CollHeap *heap)
    : StmtDDLNode(DDL_UNREGISTER_TENANT),
      tenantAlterType_(NOT_ALTER),
      tenantName_(tenantName, heap),
      dropDependencies_(dropDependencies),
      defaultSchema_(NULL),
      roleName_(NULL),
      affinity_(-1),
      balance_(FALSE),
      clusterSize_(0),
      sessionLimit_(-2),
      sessionLimitSpecified_(FALSE),
      tenantSize_(0),
      addGroupList_(FALSE),
      groupList_(NULL),
      addRGroupList_(FALSE),
      rgroupList_(NULL),
      addNodeList_(FALSE),
      nodeList_(NULL),
      addSchemaList_(FALSE),
      schemaList_(NULL),
      tenantAffinitySizing_(FALSE),
      defaultSchemaSpecified_(FALSE),
      heap_(heap) {}  // StmtDDLTenant::StmtDDLTenant()

//
// virtual destructor
//
StmtDDLTenant::~StmtDDLTenant() {
  // if (roleName_)
  // NADELETE (roleName_, heap_);
  // if (schemaList_)
  // NADELETE (schemaList_, heap_);
}

//
// cast
//
StmtDDLTenant *StmtDDLTenant::castToStmtDDLTenant() { return this; }

//
// End  f File
//
