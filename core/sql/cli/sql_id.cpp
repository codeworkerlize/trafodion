

#include "cli/sql_id.h"

#include "cli/SQLCLIdev.h"
#include "common/BaseTypes.h"
#include "common/NLSConversion.h"
#include "common/charinfo.h"
#include "common/str.h"

// intialize a SQL module id.
void init_SQLMODULE_ID(SQLMODULE_ID *m, int version, const char *name, int timestamp, const char *charset,
                       int name_len) {
  m->version = version;
  m->creation_timestamp = timestamp;

  setNameForModule(m, name, name_len, charset);
}

// initialize a SQL object id.
void init_SQLCLI_OBJ_ID(SQLCLI_OBJ_ID *x, int version, enum SQLOBJ_ID_NAME_MODE mode, const SQLMODULE_ID *module,
                        const char *id, void *handle_, const char *charset, int id_len, int tag) {
  x->version = version;
  x->name_mode = mode;
  x->module = module;
  x->handle = handle_;
  x->tag = tag;
  setNameForId(x, id, id_len, charset);
}

////////////////////////////////////////////
// compare two SQL object ids for equality.
////////////////////////////////////////////

int isEqualByName(SQLCLI_OBJ_ID *x, SQLCLI_OBJ_ID *y) {
  // 8/6/98: Unicode based comparison is not enabled yet
  // as it is still questionable whether a module will be
  // coded in Unicode.

  return ((x->identifier && y->identifier && getIdLen(x) == getIdLen(y) &&
           str_cmp(x->identifier, y->identifier, getIdLen(x)) == 0) ||
          (x->identifier == 0 && y->identifier == 0));
}

////////////////////////////////////////////
// compare two SQL module ids for equality.
////////////////////////////////////////////

int isEqualByName(const SQLMODULE_ID *x, const SQLMODULE_ID *y) {
  // 8/6/98: Unicode based comparison is not enabled yet
  // as it is still questionable whether a module will be
  // coded in Unicode.

  return ((x->module_name && y->module_name && getModNameLen(x) == getModNameLen(y) &&
           str_cmp(x->module_name, y->module_name, getModNameLen(x)) == 0) ||
          (x->module_name == 0 && y->module_name == 0));
}

////////////////////////////////////////////
// set the name for a SQL object id.
////////////////////////////////////////////
void setNameForId(SQLCLI_OBJ_ID *x, const char *name, int len, const char *charset) {
  x->identifier = name;

  if (x->version == SQLCLI_CURRENT_VERSION) {
    x->identifier_len = len;
    x->charset = charset;
  }
}

////////////////////////////////////////////
// set the name for a SQL module id.
////////////////////////////////////////////
void setNameForModule(SQLMODULE_ID *m, const char *name, int len, const char *charset) {
  m->module_name = name;

  if (m->version == SQLCLI_CURRENT_VERSION) {
    m->module_name_len = len;
    m->charset = charset;
  }
}

#if 0
////////////////////////////////////////////
// get the name of a SQL module id in 
// Unicode.
////////////////////////////////////////////
NAWchar* getModNameInWchar(const SQLMODULE_ID* m)
{
   static NAWcharBuf * buf = new NAWcharBuf(MAX_CHAR_SET_STRING_LENGTH+1);

   if ( m == 0 ) return 0;

   const char* charset = getModCharSet(m);
   NAWcharBuf* res = 0;

   if ( charset == SQLCHARSETSTRING_UNICODE || str_cmp(charset, SQLCHARSETSTRING_UNICODE,str_len(charset)) == 0 ) {
     return (NAWchar*)(m -> module_name);
   }
   if ( charset == SQLCHARSETSTRING_ISO88591 || str_cmp(charset, SQLCHARSETSTRING_ISO88591,str_len(charset)) == 0 ) {
     res = ISO88591ToUnicode(
	charBuf((unsigned char*)(m->module_name), getModNameLen(m)), 
	(CollHeap *)0,
	buf
		      );
     return (res) ? (res -> data()) : 0;
   }
   return 0;
}

////////////////////////////////////////////
// get the name of a SQL object id in 
// Unicode.
////////////////////////////////////////////
NAWchar* getIdInWchar(SQLCLI_OBJ_ID* x)
{
   static NAWcharBuf * buf = new NAWcharBuf(MAX_CHAR_SET_STRING_LENGTH+1);

   if ( x == 0 ) return 0;

   const char* charset = getIdCharSet(x);
   NAWcharBuf* res = 0;

   if ( charset == SQLCHARSETSTRING_UNICODE || str_cmp(charset, SQLCHARSETSTRING_UNICODE,str_len(charset)) == 0 ) {
     return (NAWchar*)(x -> identifier);
   }
   if ( charset == SQLCHARSETSTRING_ISO88591 || str_cmp(charset, SQLCHARSETSTRING_ISO88591,str_len(charset)) == 0 ) {
     res = ISO88591ToUnicode(
	charBuf((unsigned char*)(x->identifier), getIdLen(x)), 
	(CollHeap *)0,
	buf
		      );
     return (res) ? (res -> data()) : 0;
   }
   return 0;
}

////////////////////////////////////////////
// get the name of a SQL module id in locale.
////////////////////////////////////////////
char* getModNameInLocale(const SQLMODULE_ID* m)
{
   if ( m == 0 ) return 0;

   const char* charset = getModCharSet(m);

   if ( charset == CharInfo::getLocaleCharSetAsString() )
     return (char *)m -> module_name;

   NAWchar* nameInWchar = getModNameInWchar(m);

   if (nameInWchar == 0)
     return 0;

   int wcNameLen = NAWstrlen(nameInWchar);

   static char* nameInLocale = new char[MAX_CHAR_SET_STRING_LENGTH+1];

   int x = UnicodeStringToLocale(CharInfo::getCharSetEnum(charset), 
                         nameInWchar, wcNameLen,
                         nameInLocale, MAX_CHAR_SET_STRING_LENGTH+1);
  
   if ( x == 0 )
      return 0;

   return nameInLocale;
}

////////////////////////////////////////////
// get the name of a SQL object id in locale.
////////////////////////////////////////////
char* getIdInLocale(SQLCLI_OBJ_ID* x)
{
   if ( x == 0 ) return 0;

   const char* charset = getIdCharSet(x);

   if ( charset == CharInfo::getLocaleCharSetAsString() )
     return (char*) x -> identifier;

   NAWchar* nameInWchar = getIdInWchar(x);

   if (nameInWchar == 0)
     return 0;

   int wcNameLen = NAWstrlen(nameInWchar);

   static char* nameInLocale = new char[MAX_CHAR_SET_STRING_LENGTH+1];

   int nameInLocaleLen = UnicodeStringToLocale(CharInfo::getCharSetEnum(charset),
                         nameInWchar, wcNameLen,
                         nameInLocale, MAX_CHAR_SET_STRING_LENGTH+1);

   if ( nameInLocaleLen == 0 )
     return 0;

   return nameInLocale;
}
#endif
