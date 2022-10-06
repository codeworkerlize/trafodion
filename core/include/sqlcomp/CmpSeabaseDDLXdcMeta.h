

/*
 ********************************************************************************
 *
 * File:         CmpSeabaseDDLXdcMeta.h
 * Description:  This file describes XDC metadata tables used by Trafodion.
 *               Currently includes sequence and DDL table
 *
 * *****************************************************************************
 */

#ifndef _CMP_SEABASE_XDC_META_H_
#define _CMP_SEABASE_XDC_META_H_
#include "sqlcomp/CmpSeabaseDDLupgrade.h"

///////////////////////////////////////////////////////////////////////////////
// *** Current Definition ***
//
// Current XDC Metadata tables definition for Metadata Version 2.1
//  (Major version = 2, Minor version = 1)
///////////////////////////////////////////////////////////////////////////////

#define DDL_TEXT_LEN 10000
//----------------------------------------------------------------
//-- XDC_SEQUENCE_TABLE
//----------------------------------------------------------------
static const QString createXDCSequenceTable[] = {
    {" create table %s.\"%s\"." XDC_SEQUENCE_TABLE " "},
    {" ( "},
    {" catalog_name                           varchar( 256 bytes ) character set utf8 not null not serialized, "},
    {" schema_name                            varchar( 256 bytes ) character set utf8 not null not serialized, "},
    {" seq_name                               varchar( 256 bytes ) character set utf8 not null not serialized, "},
    {" next_value               			   largeint not null not serialized "},
    {" ) "},
    {" primary key ( catalog_name, schema_name, seq_name ) "},
    {" attribute incremental backup hbase format storage hbase "},
    {" ; "}};

//----------------------------------------------------------------
//-- XDC_DDL_TABLE
//----------------------------------------------------------------
static const QString createXDCDDLTable[] = {
    {" create table %s.\"%s\"." XDC_DDL_TABLE " "},
    {" ( "},
    {" catalog_name                           varchar( 256 bytes ) character set utf8 not null not serialized, "},
    {" schema_name                            varchar( 256 bytes ) character set utf8 not null not serialized, "},
    {" object_name                            varchar( 256 bytes ) character set utf8 not null not serialized, "},
    {" object_type                            char( 2 ) character set iso88591 not null not serialized, "},
    {" seq_num                                int not null not serialized, "},
    {" ddl_text        			   varchar( 10000 bytes) character set utf8 not null not serialized "},
    {" ) "},
    {" primary key ( catalog_name, schema_name, object_name, object_type, seq_num) "},
    {" attribute incremental backup hbase format storage hbase "},
    {" ; "}};

#define XDC_SEQUENCE_OLD_TABLE XDC_SEQUENCE_TABLE "_OLD_XDC_MD"
#define XDC_DDL_OLD_TABLE      XDC_DDL_TABLE "_OLD_XDC_MD"

static const MDUpgradeInfo allXDCMDUpgradeInfo[] = {
    // XDC_SEQUENCE_TABLE
    {XDC_SEQUENCE_TABLE, XDC_SEQUENCE_OLD_TABLE, createXDCSequenceTable, sizeof(createXDCSequenceTable), NULL, 0, NULL,
     0, FALSE, NULL, NULL, NULL, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE},

    {XDC_DDL_TABLE, XDC_DDL_OLD_TABLE, createXDCDDLTable, sizeof(createXDCDDLTable), NULL, 0, NULL, 0, FALSE, NULL,
     NULL, NULL, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE}

};

#endif
