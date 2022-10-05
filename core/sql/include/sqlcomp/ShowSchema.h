#ifndef SHOWSCHEMA_H
#define SHOWSCHEMA_H
/* -*-C++-*-
******************************************************************************
*
* File:         ShowSchema.h
*
* Description:  A lightweight means for a requester (SQLCI's ENV command)
*		to get arkcmp's current default catalog and schema
*		(in Ansi format).
*		A similar means for ExSqlComp to get arkcmp's initial defaults,
*		to be reissued if arkcmp crashes, to ensure identical context
*		of the restarted arkcmp.
*
*

*
*       Nomina si nescis perit et cognitio rerum.
*	[If you do not know the names, the knowledge of things is also lost.]
*	        -- Carolus Linnaeus
*
******************************************************************************
*/

#include "common/Platform.h"
#include "common/BaseTypes.h"
#include "exp/ExpError.h"
#include "export/NAStringDef.h"

class ShowSchema {
 public:
  static const char *ShowControlDefaultSchemaMagic() { return "**cat.sch**"; }  // ident internal-fmt

  static const char *ShowSchemaStmt() { return "SHOWCONTROL DEFAULT \"**cat.sch**\";"; }  // ident delimited

  static int DiagSqlCode() { return ABS(EXE_INFO_DEFAULT_CAT_SCH); }

  static NABoolean getDefaultCatAndSch(NAString &cat, NAString &sch);


};

class GetControlDefaults {  // Genesis 10-981211-5986
 public:
  static const char *GetExternalizedDefaultsMagic() { return "**extlzd.deflts**"; }  // ident internal-fmt

  static const char *GetExternalizedDefaultsStmt() {
    return "SHOWCONTROL DEFAULT \"**extlzd.deflts**\";";
  }  // ident delimited

  static int DiagSqlCode() { return ABS(EXE_INFO_CQD_NAME_VALUE_PAIRS); }

  // Handled like the preceding class.
  // Used by ExSqlComp.
};

#endif  // SHOWSCHEMA_H
