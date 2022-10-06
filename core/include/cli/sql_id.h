
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         sql_id.h
 * RCS:          $Id:
 * Description:  The C-style ADT for manipulating CLI module/object identifiers
 *
 *
 * Created:      7/8/98
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef SQL_ID_H
#define SQL_ID_H

#include "cli/SQLCLIdev.h"
#include "common/BaseTypes.h"
#include "common/NAWinNT.h"
#include "common/Platform.h"
#include "common/str.h"

#define getModNameLen(sqlmodule_id_ptr) (sqlmodule_id_ptr)->module_name_len

#define getIdLen(sqlcli_obj_ptr) (sqlcli_obj_ptr)->identifier_len

#define getModCharSet(sqlmodule_id_ptr) \
  (((sqlmodule_id_ptr)->version < SQLCLI_CURRENT_VERSION) ? SQLCHARSETSTRING_ISO88591 : (sqlmodule_id_ptr)->charset)

#define getIdCharSet(sqlcli_obj_ptr) \
  (((sqlcli_obj_ptr)->version < SQLCLI_CURRENT_VERSION) ? SQLCHARSETSTRING_ISO88591 : (sqlcli_obj_ptr)->charset)
#if 0
SQLMODULE_ID* new_SQLMODULE_ID(
	int version = SQLCLI_CURRENT_VERSION, 
	const char* module_name = 0,
	int creation_timestamp = 0,
	const char* charset = SQLCHARSETSTRING_ISO88591,
	int module_name_len = 0
	);
#endif

void init_SQLMODULE_ID(SQLMODULE_ID *m, int version = SQLCLI_CURRENT_VERSION, const char *module_name = 0,
                       int timestamp = 0, const char *charset = SQLCHARSETSTRING_ISO88591, int name_len = 0);
#if 0
SQLCLI_OBJ_ID* new_SQLCLI_OBJ_ID( int version = SQLCLI_CURRENT_VERSION, 
	enum SQLOBJ_ID_NAME_MODE mode = stmt_name, 
	const SQLMODULE_ID* module = 0, 
	const char* id = 0, 
	void* handle_ = 0,
	const char* charset = SQLCHARSETSTRING_ISO88591,
        int id_len = 0, int tag = 0
	);

#define new_SQLSTMT_ID new_SQLCLI_OBJ_ID
#define new_SQLDESC_ID new_SQLCLI_OBJ_ID

#endif
void init_SQLCLI_OBJ_ID(SQLCLI_OBJ_ID *x, int version = SQLCLI_CURRENT_VERSION,
                        enum SQLOBJ_ID_NAME_MODE mode = stmt_name, const SQLMODULE_ID *module = 0, const char *id = 0,
                        void *handle_ = 0, const char *charset = SQLCHARSETSTRING_ISO88591, int id_len = 0,
                        int tag = 0);

#define init_SQLSTMT_ID init_SQLCLI_OBJ_ID
#define init_SQLDESC_ID init_SQLCLI_OBJ_ID

int isEqualByName(SQLCLI_OBJ_ID *x, SQLCLI_OBJ_ID *y);

int isEqualByName(const SQLMODULE_ID *x, const SQLMODULE_ID *y);

void setNameForId(SQLCLI_OBJ_ID *x, const char *name, int len, const char *charset = SQLCHARSETSTRING_ISO88591);

void setNameForModule(SQLMODULE_ID *x, const char *name, int len, const char *charset = SQLCHARSETSTRING_ISO88591);
#endif
