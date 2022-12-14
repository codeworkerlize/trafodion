
#ifndef DESCRIPTOR_H
#define DESCRIPTOR_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         Descriptor.h
 * Description:  SQL Descriptors, used to interface between host programs
 *               and SQL. A descriptor describes one or more host variables
 *               (their data type, precision, character set, etc).
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
//#include <sys/types.h>
//#include <stdarg.h>

#include "cli/CliDefs.h"
#include "comexe/ComTdbRoot.h"
#include "common/ComSysUtils.h"
#include "executor/ex_god.h"
// -----------------------------------------------------------------------
// Forward references
// -----------------------------------------------------------------------

class ContextCli;
class Attributes;
class Statement;

// -----------------------------------------------------------------------
// BulkMoveInfo
// -----------------------------------------------------------------------

class BulkMoveInfo {
 public:
  friend class Descriptor;

  int maxEntries() { return maxEntries_; };
  int usedEntries() { return usedEntries_; };

  // returns info about the i'th entry. Entry num is 0 based.
  NABoolean isExeDataPtr(int i) { return bmiArray_[i].isExePtr(); };
  int getLength(int i) { return bmiArray_[i].length_; };
  char *getDescDataPtr(int i) { return bmiArray_[i].descDataPtr_; };
  short getExeAtpIndex(int i) { return bmiArray_[i].exeAtpIndex_; };
  Long getExeOffset(int i) { return bmiArray_[i].exeOffset_; };
  char *getExeDataPtr(int i) { return bmiArray_[i].exeDataPtr_; };
  short getFirstEntryNum(int i) { return bmiArray_[i].firstEntryNum_; };
  short getLastEntryNum(int i) { return bmiArray_[i].lastEntryNum_; };
  NABoolean isVarchar(int i) { return bmiArray_[i].isVarchar(); };
  NABoolean isNullable(int i) { return bmiArray_[i].isNullable(); };

  void addEntry(int length, char *descDataPtr, short exeAtpIndex, NABoolean exeIsPtr, Long exeOffset,
                short firstEntryNum, short lastEntryNum, NABoolean isVarchar, NABoolean isNullable);

 private:
  struct BMInfoStruct {
    enum { IS_EXE_PTR = 0x0001, IS_VARCHAR = 0x0002, IS_NULLABLE = 0x0004 };

    NABoolean isExePtr() { return (bmiFlags_ & IS_EXE_PTR) != 0; }
    void setIsExePtr(NABoolean v) { (v ? bmiFlags_ |= IS_EXE_PTR : bmiFlags_ &= ~IS_EXE_PTR); }

    NABoolean isVarchar() { return (bmiFlags_ & IS_VARCHAR) != 0; }
    void setIsVarchar(NABoolean v) { (v ? bmiFlags_ |= IS_VARCHAR : bmiFlags_ &= ~IS_VARCHAR); }

    NABoolean isNullable() { return (bmiFlags_ & IS_NULLABLE) != 0; }
    void setIsNullable(NABoolean v) { (v ? bmiFlags_ |= IS_NULLABLE : bmiFlags_ &= ~IS_NULLABLE); }

    int length_;
    char *descDataPtr_;
    short exeAtpIndex_;
    short bmiFlags_;
    union {
      Long exeOffset_;
      char *exeDataPtr_;
    };
    short firstEntryNum_;
    short lastEntryNum_;
  };

  int flags_;
  int maxEntries_;
  int usedEntries_;

  BMInfoStruct bmiArray_[1];
};

// -----------------------------------------------------------------------
// Descriptor
// -----------------------------------------------------------------------
class Descriptor : public ExGod {
  enum Flags {
    /*************************************************************

       Rowwise Rowset Versions

         There two versions of rowsets, version 1 and version 2. The
         original version (version 1) of rowwise rowsets does not
         support all SQL/MX data types and does not support nullable
         columns. If the input/output params/columns contained any of
         the following attributes:

           Varchar type
           Intervals type
           Datetime type
           Nullable
           Character set was not single byte

         Rowwise rowsets and/or bulk move was disallowed.

         Version 2 of rowwise rowsets removes most of the restrictions
         imposed by version 1. All data types are supported and nullable
         columns are allowed. In certain very rare cases bulk move may
         be disallowed (e.g. interval types) for a given param/column,
         but this will not disallow bulk move for all other
         params/columns. Rowwise rowsets will be returned in all cases,
         except if the column is special. A column is special if it is
         an "added column" (note: I'm not entirely sure what a
         special column is. As best I can tell from the code, it is as
         stated, a column that was added after the table was created).

       Rowset Types

         A Descriptor contains information about values used as input or
         output. Each descriptor has a rowset type associated with it.
         Typically, the rowset type is set by the user. The executor will
         format data based on the rowset type.

         There are 4 rowset types as defined in SQLCLIdev.h.

           ROWSET_NOT_SPECIFIED
           ROWSET_COLUMNWISE
           ROWSET_ROWWISE
           ROWSET_ROWWISE_V1
           ROWSET_ROWWISE_V2

         ROWSET_NOT_SPECIFIED is the default rowset type, and indicates that
         rowsets are not being used.

         ROWSET_COLUMNWISE instructs the executor to use columnwise format
         for processing data.

         ROWSET_ROWWISE and ROWSET_ROWWISE_V1 are the same value. If this
         value is set, then the executor will use the version 1 rowwise rowset
         logic for processing.

         ROWSET_ROWWISE_V2 instructs the executor to use version 2 rowwise
         rowset logic for processing.

       Internal Flags

         A "Descriptor" object contains information about values used as input
         or output. The Descriptor has contains an array of "desc_struct"
         structures, where each element of the array contains information about
         one of the values. The desc_struct data member is named "desc".

         Both the Descriptor object and desc_struct structure contain a data
         member than is a flag bit map. The Descriptor object flag bit map is
         named "flags_". The desc_struct structure bit map data member is named
         "desc_flag". Possible values for both the Descriptor flags_ and
         desc_struct desc_flags bit maps are:

           BULK_MOVE_DISABLED
           BULK_MOVE_SETUP
           DESC_TYPE_WIDE
           DO_SLOW_MOVE
           ROWWISE_ROWSET
           ROWWISE_ROWSET_V1
           ROWWISE_ROWSET_DISABLED
           ROWWISE_ROWSET_V2

         All flags are mutually exclusive. That is, setting one flag will not
         implicitly change the value of another flag.

         The BULK_MOVE_DISABLED flag is used to disable bulk move for all of
         the Descriptor's entries. If this flag is set, then none of the
         descriptor's entries are bulk moved.

         The BULK_MOVE_SETUP flag is used to indicate that the appropriate
         actions have been taken to set up for bulk move of some or all of
         the entries in a Descriptor (i.e. InputOutputExpr::setupBulkMoveInfo
         has been called).

         The DESC_TYPE_WIDE flag is used to indicate that the Descriptor is a
         wide descriptor.

         The bulk move logic will disable bulk move (i.e. set the BULK_MOVE_DISABLED flag)
         if the DESC_TYPE_WIDE flag is set.

         The DO_SLOW_MOVE flag is used by the desc_struct to indicate that the value
         represented by that desc_struct is not bulk moved (i.e. it is slow moved by
         convDoIt). It is also used by the Descriptor to indicate there is a slow
         move value in its list of values. That is, the DO_SLOW_MOVE flag is set in
         the Descriptor if any of the Descriptor's values has the DO_SLOW_MOVE flag
         set.

         The ROWWISE_ROWSET and ROWWISE_ROWSET_V1 flags are the same value. This flag
         is used by the Descriptor to indicate that the rowset type is version 1. It
         is set by the user, via the SQL_EXEC_SetDescItem method. It indicates that
         the user wants the executor to use the original rowwise rowset and bulk move
         logic for processing rowwise rowsets and bulk move.

         The ROWWISE_ROWSET_V2 flag is used by the Descriptor to indicate that the
         rowset type is version 2. It is set by the user, via the SQL_EXEC_SetDescItem
         method. It indicates that the user wants the executor to use the new rowwise
         rowset and bulk move logic for processing rowwise rowsets and bulk move.

    **************************************************************/

    BULK_MOVE_DISABLED = 0x0001,
    BULK_MOVE_SETUP = 0x0002,
    DESC_TYPE_WIDE = 0x0004,
    DO_SLOW_MOVE = 0x0008,
    ROWWISE_ROWSET = 0x0010,
    ROWWISE_ROWSET_V1 = ROWWISE_ROWSET,
    ROWWISE_ROWSET_DISABLED = 0x0020,
    ROWWISE_ROWSET_V2 = 0x0040

  };

  SQLDESC_ID descriptor_id;
  SQLMODULE_ID module;
  short dyn_alloc_flag;

  int rowsetSize;          // Number of rows in rowset array.
  void *rowsetStatusPtr;   // Optional. Specifies the status array. It
                           // contains the row status values for each row
                           // after a RowSet fetch.
                           // Must be size of RowsetSize
                           // Application is responsable for this ptr.
  int rowsetNumProcessed;  // Number of rows processed in the rowset
  int rowsetHandle;        // A handle/cursor to current row in rowset
                           // A value of "-1" indicates that a rowset
                           // has not been defined.
  int max_entries;         // Max entries of desc_struct
  int used_entries;        // Num active entries of desc_struct

  int flags_;

  int compoundStmtsInfo_;  // contains information needed for compound statements

  int rowwiseRowsetSize;      // Number of rows in rowset array they
                              // are being passed in stacked rowwise.
  int rowwiseRowsetRowLen;    // length of each row.
  Long rowwiseRowsetPtr;      // ptr to the start of rowset buffer.
  int rowwiseRowsetPartnNum;  // partition number where this rwrs
                              // buffer need to go to.
  char filler_[16];

  struct desc_struct {
    enum {
      IS_NUMERIC = 0x0001,
      IS_NUMERIC_UNSIGNED = 0x0002,
      DO_ITEM_SLOW_MOVE = 0x0004,
      IS_CASE_INSENSITIVE = 0x0008
    };

    int rowsetVarLayoutSize; /* Three cases:
                              * ZERO: when entry is not participating
                              *       in a rowset or the same value is
                              *       used in the rowset (i.e., without
                              *       an array)
                              * COLUMN_WISE: Same value as length. There
                              *       is an array of values of that length
                              * Row-WISE: value is the size of the structure
                              *       where this item is contained. There
                              *       are several item in the structure,
                              *       even items outside the rowset.
                              */
    int rowsetIndLayoutSize; /* Three cases:
                              * ZERO: when entry is not participating
                              *       in a rowset or the same value is
                              *       used in the rowset (i.e., without
                              *       an array)
                              * COLUMN_WISE: Same value as ind_length. There
                              *       is an array of values of that length
                              * Row-WISE: value is the size of the structure
                              *       where this item is contained. There
                              *       are several item in the structure,
                              *       even items outside the rowset.
                              */
    int datatype;
    int datetime_code;
    int length;
    short nullable;
    int charset;            //
    char *charset_schema;   // Internally charset and collation
    char *charset_catalog;  // is stored as a long.
    char *charset_name;     // All other charset and collation related

    int coll_seq;  // desc items are stored as character strings.
    char *coll_schema;
    char *coll_catalog;
    char *coll_name;

    int scale;
    int precision;
    int int_leading_precision;
    char *output_name;
    short generated_output_name;
    char *heading;
    short string_format;
    char *table_name;
    char *schema_name;
    char *catalog_name;
    Long var_ptr;    // For Bound. Application allocate memory
                     // If Rowset is equal to 1,
                     // then var_ptr is a pointer to single var.
                     // else var_ptr is a pointer to ptr array of
                     // variables.
    char *var_data;  // For Unbound. Rowsets are not used
    Long ind_ptr;    // Same convention as var_ptr
    int ind_data;    // For Unbound. Rowsets are not used
    int ind_datatype;
    int ind_length;
    int vc_ind_length;
    int aligned_length;  // length + ind_length + vc_ind_length + fillers

    // offsets to values in the actual sql row.
    // Used by caller to align their data so bulkmove could be done.
    int data_offset;
    int null_ind_offset;

    int desc_flags;

    // Vicz: three new descriptor items to support call stmt description
    short parameterMode;
    short ordinalPosition;
    short parameterIndex;

    char *text_format;

#ifdef _DEBUG
    int rowwise_var_offset;  // testing logic
    int rowwise_ind_offset;  // testing logic
#endif

    int lobInlinedDataMaxLen;
    int lobChunkMaxLen;
  };

  desc_struct *desc;

  BulkMoveInfo *bmInfo_;

  // the statement for which bulk move was done from/to this descriptor.
  Statement *bulkMoveStmt_;

  ContextCli *context_;  // the context that contains this descriptor

  NABoolean lockedForNoWaitOp_;  // true if this descriptor is in use
                                 // by a pending no-wait operation

  static void DescItemDefaultsForDatetimeCode(desc_struct &descItem, int datetime_interval);
  static void DescItemDefaultsForType(desc_struct &descItem, int datatype, NAHeap &heap);
  static int DefaultPrecision(int datatype);
  static int DefaultScale(int datatype);

  static int setIndLength(desc_struct &descItem);
  static int setVarLength(desc_struct &descItem);

  char *getVarItem(desc_struct &descItem, int idxrow);
  char *getIndItem(desc_struct &descItem, int idxrow);

  RETCODE processNumericDatatype(desc_struct &descItem, int numeric_value);

  RETCODE processNumericDatatypeWithPrecision(desc_struct &descItem, ComDiagsArea &diags);

  void set_text_format(desc_struct &descItem);

 public:
  Descriptor(SQLDESC_ID *descriptor_id_, int max_entries_, ContextCli *context);
  ~Descriptor();

  NABoolean operator==(Descriptor &other);

  RETCODE alloc(int used_entries_);

  RETCODE dealloc();

  RETCODE addEntry(int entry);

  char *getVarData(int entry);
  char *getVarData(int entry, int idxrow);
  int getVarDataLength(int entry);
  int getVarIndicatorLength(int entry);
  int getVarDataType(int entry);
  const char *getVarDataCharSet(int entry);
  char *getIndData(int entry);
  char *getIndData(int entry, int idxrow);
  int getIndLength(int entry);
  RETCODE getDescItemMainVarInfo(int entry, short &var_isnullable, int &var_datatype, int &var_length, void **var_ptr,
                                 int &ind_datatype, int &ind_length, void **ind_ptr);

  RETCODE getDescItem(int entry, int what_to_get, void *numeric_value, char *string_value, int max_string_len,
                      int *returned_len, int start_from_offset, Descriptor *desc_to_get_more_info = 0,
                      int entry_in_desc_to_get_more_info = 0);

  RETCODE getDescItemPtr(int entry, int what_to_get, char **string_ptr, int *returned_len);

  RETCODE setDescItem(int entry, int what_to_set, Long numeric_value, char *string_value,
                      Descriptor *desc_to_get_more_info = 0, int entry_in_desc_to_get_more_info = 0);

  RETCODE setDescItemInternal(int entry, int what_to_set, int numeric_value, char *string_value);

  //
  // GetNameViaDesc - retrieve the name of a statement,cursor, or descriptor
  //   from a given descriptor id.  The name is returned in a OBJ_ID
  // dynamically
  //   allocated from the executor heap.
  //
  static SQLCLI_OBJ_ID *GetNameViaDesc(SQLDESC_ID *desc_id, ContextCli *context, NAHeap &heap);

  inline void setMaxEntryCount(int max_entries_);
  inline int getMaxEntryCount();

  int getUsedEntryCount();

  void setUsedEntryCount(int used_entries_);

  int getRowsetSize() { return rowsetSize; }

  int getRowwiseRowsetSize() { return rowwiseRowsetSize; }
  int getRowwiseRowsetRowLen() { return rowwiseRowsetRowLen; }
  long getRowwiseRowsetPtr() { return rowwiseRowsetPtr; }

  int getRowsetNumProcessed() { return rowsetNumProcessed; }

  int getCurrRowOffsetInRowwiseRowset() { return (rowwiseRowsetRowLen * rowsetNumProcessed); }

  Long getCurrRowPtrInRowwiseRowset() { return (rowwiseRowsetPtr + rowwiseRowsetRowLen * rowsetNumProcessed); }

  int getCompoundStmtsInfo() const;

  void setCompoundStmtsInfo(int info);

  inline NABoolean thereIsACompoundStatement() const {
    return ((compoundStmtsInfo_ & ComTdbRoot::COMPOUND_STATEMENT_IN_QUERY) != 0);
  }

  inline void setCompoundStatement() { compoundStmtsInfo_ |= ComTdbRoot::COMPOUND_STATEMENT_IN_QUERY; }

  inline void *getDescHandle();

  inline SQLDESC_ID *getDescName();

  const SQLMODULE_ID *getModuleId() { return descriptor_id.module; };
  /*
    inline char *getModuleName();
    inline long getModuleNameLen()
      { return GET_SQL_MODULE_NAME_LEN_PTR(descriptor_id.module); };
  */
  inline int getDescFlags();
  inline void setDescFlags(int f);
  // static helper functions to deal with FS types
  static int ansiTypeFromFSType(int datatype);
  static const char *ansiTypeStrFromFSType(int datatype);
  static int datetimeIntCodeFromTypePrec(int datatype, int precision);
  static short isIntervalFSType(int datatype);
  static short isCharacterFSType(int datatype);
  static short isIntegralFSType(int datatype);
  static short isFloatFSType(int datatype);
  static short isNumericFSType(int datatype);
  static short isDatetimeFSType(int datatype);
  static short isBitFSType(int datatype);

  inline short dynAllocated();
  ContextCli *getContext() { return context_; }

  // This function gets char string content from an ASCII CHAR host variable.
  static char *getCharDataFromCharHostVar(ComDiagsArea &diags, NAHeap &heap, char *host_var_string_value,
                                          int host_var_string_value_length, const char *the_SQLDESC_option,
                                          Descriptor *info_desc = 0, int info_desc_index = 0, short target_type = -1);

  //////////////////////////////////////////////////
  // Methods to do Bulk Move
  //////////////////////////////////////////////////
  RETCODE allocBulkMoveInfo();
  RETCODE deallocBulkMoveInfo();

  BulkMoveInfo *bulkMoveInfo() { return bmInfo_; };

  NABoolean bulkMoveDisabled();
  void setBulkMoveDisabled(NABoolean v) { (v ? flags_ |= BULK_MOVE_DISABLED : flags_ &= ~BULK_MOVE_DISABLED); }
  NABoolean bulkMoveSetup();
  void setBulkMoveSetup(NABoolean v) { (v ? flags_ |= BULK_MOVE_SETUP : flags_ &= ~BULK_MOVE_SETUP); }

  void reComputeBulkMoveInfo() {
    // set the flags so bulk move info could be recomputed.
    setBulkMoveDisabled(FALSE);  // bulk move is no longer disabled
    setBulkMoveSetup(FALSE);     // but needs to be set up.
  }

  enum BulkMoveStatus {
    // bulk  move is disallowed for this case. Use convDoIt
    // to move data(slow move) for this entry.
    BULK_MOVE_DISALLOWED,

    // bulk move is turned off for all entries.
    // Use convDoIt for everything.
    BULK_MOVE_OFF,

    // bulk move is allowed.
    BULK_MOVE_OK
  };

  //
  // This method will return the proper location in the
  // descriptor's data buffer.
  //
  Long getRowsetVarPtr(short entry);

  //
  // This method will call the appropriate checkBulkMoveStatus method
  // depending on the rowwise rowset version currently in use.
  //
  // The methods called from this method also have the side affect of
  // setting the varPtr parameter to the proper location in the
  //  descriptor's data buffer for this items bulk move start address.
  //
  // isInputDesc: TRUE, if bulk move is being done for input.
  //              FALSE, if bulkd move is being done for output.
  //
  inline BulkMoveStatus checkBulkMoveStatus(short entry, Attributes *op, Long &varPtr, NABoolean isInputDesc,
                                            NABoolean isOdbc, NABoolean isRWRS, NABoolean isInternalIntervalFormat) {
    if (rowwiseRowsetV2() == TRUE)
      return checkBulkMoveStatusV2(entry, op, varPtr, isInputDesc, isOdbc, isRWRS, isInternalIntervalFormat);
    else
      return checkBulkMoveStatusV1(entry, op, varPtr, isInputDesc, isRWRS, isInternalIntervalFormat);
  };  // end Descriptor::checkBulkMoveStatus

  BulkMoveStatus checkBulkMoveStatusV1(short entry, Attributes *op, Long &varPtr, NABoolean isInputDesc,
                                       NABoolean isRWRS, NABoolean isInternalIntervalFormat);

  BulkMoveStatus checkBulkMoveStatusV2(short entry, Attributes *op, Long &varPtr, NABoolean isInputDesc,
                                       NABoolean isOdbc, NABoolean isRWRS, NABoolean isInternalIntervalFormat);

  // this flag indicates that the entry number 'entry' is to be
  // be moved using 'slow' move method (convDoIt).
  NABoolean doSlowMove(short entry) {
    desc_struct &descItem = desc[entry - 1];  // Zero base

    return (descItem.desc_flags & descItem.DO_ITEM_SLOW_MOVE) != 0;
  }
  void setDoSlowMove(short entry, NABoolean v) {
    desc_struct &descItem = desc[entry - 1];  // Zero base

    (v ? descItem.desc_flags |= descItem.DO_ITEM_SLOW_MOVE : descItem.desc_flags &= ~descItem.DO_ITEM_SLOW_MOVE);
  }

  NABoolean doSlowMove();

  void setDoSlowMove(NABoolean v) { (v ? flags_ |= DO_SLOW_MOVE : flags_ &= ~DO_SLOW_MOVE); }

  //
  // Common Rowwise rowset methods.
  //

  // Set rowwise rowset disabled flag
  void setRowwiseRowsetDisabled(NABoolean v) {
    (v ? flags_ |= ROWWISE_ROWSET_DISABLED : flags_ &= ~ROWWISE_ROWSET_DISABLED);
  }

  // True if rowwise rowset disabled flag is on.
  NABoolean rowwiseRowsetDisabled() { return (flags_ & ROWWISE_ROWSET_DISABLED) != 0; }

  // True if any type of rowwise rowset is on.
  NABoolean rowwiseRowset() { return ((flags_ & ROWWISE_ROWSET_V1) || (flags_ & ROWWISE_ROWSET_V2)) != 0; }

  // True if any type of rowwise rowset is on and rowwise rowsets are not disabled.
  NABoolean rowwiseRowsetEnabled() { return ((rowwiseRowset()) && (NOT rowwiseRowsetDisabled())); }

  //
  // Rowwise rowset version 1 methods.
  //

  NABoolean rowwiseRowsetV1() { return (flags_ & ROWWISE_ROWSET_V1) != 0; }
  void setRowwiseRowsetV1(NABoolean v) { (v ? flags_ |= ROWWISE_ROWSET_V1 : flags_ &= ~ROWWISE_ROWSET_V1); }

  NABoolean rowwiseRowsetV1Enabled() { return ((rowwiseRowsetV1()) && (NOT rowwiseRowsetDisabled())); }

  //
  // Rowwise rowset version 2 methods.
  //

  NABoolean rowwiseRowsetV2() { return (flags_ & ROWWISE_ROWSET_V2) != 0; }
  void setRowwiseRowsetV2(NABoolean v) { (v ? flags_ |= ROWWISE_ROWSET_V2 : flags_ &= ~ROWWISE_ROWSET_V2); }

  NABoolean rowwiseRowsetV2Enabled() { return ((rowwiseRowsetV2()) && (NOT rowwiseRowsetDisabled())); }

  Statement *&bulkMoveStmt() { return bulkMoveStmt_; }

  NABoolean isDescTypeWide();
  void setDescTypeWide(NABoolean v) { (v ? flags_ |= DESC_TYPE_WIDE : flags_ &= ~DESC_TYPE_WIDE); }

  // Methods to mark and unmark Descriptor as in use for a pending
  // no-wait operation

  // returns SUCCESS if Descriptor is not already locked; ERROR
  // otherwise (caller should generate diagnostic if appropriate)
  RETCODE lockForNoWaitOp(void);

  // returns SUCCESS if Descriptor is locked; ERROR otherwise
  // (caller should generate diagnostic if appropriate)
  RETCODE unlockForNoWaitOp(void);

  // returns true if Descriptor is locked, false if unlocked;
  // does not change lock state
  NABoolean lockedForNoWaitOp(void) { return lockedForNoWaitOp_; };
};

inline void *Descriptor::getDescHandle() { return descriptor_id.handle; }

inline SQLDESC_ID *Descriptor::getDescName() { return &descriptor_id; }

/*
inline char *Descriptor::getModuleName()
{
  return descriptor_id.module->module_name;
}
*/

/* returns -1 if this descriptor was allocated by a call to AllocDesc */
inline short Descriptor::dynAllocated() { return dyn_alloc_flag; }

inline int Descriptor::getMaxEntryCount() { return max_entries; }

inline void Descriptor::setMaxEntryCount(int max_entries_) { max_entries = max_entries_; }
inline int Descriptor::getDescFlags() { return flags_; }
inline void Descriptor::setDescFlags(int f) { flags_ = f; }

void stripBlanks(char *buf, int &len);

void upperCase(char *buf);

#endif
