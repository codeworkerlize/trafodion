
#ifndef NAFILESET_H
#define NAFILESET_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NAFileSet.h
 * Description:  A description of a file set, i.e., a set of files
 *               that implement a table, an index, a view, etc.
 * Created:      12/20/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "common/Collections.h"
#include "optimizer/NAColumn.h"
#include "optimizer/ObjectNames.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class NAFileSet;
class NAFileSetList;

// -----------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------
class PartitioningFunction;
class RangePartitioningFunction;
class TrafDesc;
class HbaseCreateOption;

// -----------------------------------------------------------------------
// An enumerated type that describes how data is organized in EVERY
// file that belongs to this file set.
// A key sequenced file is implemented by a B+ tree.
// A hash file contains records whose key columns compute to the
// same hash value when the hash function that is used for
// distributing the data is applied to them.
// -----------------------------------------------------------------------
enum FileOrganizationEnum { KEY_SEQUENCED_FILE, HASH_FILE };

// -----------------------------------------------------------------------
// A NAFileSet object describes common attributes of a set of files
// that are used for implementing a base table, a single-table or a
// multi-table index, a materialized view or any other SQL object.
// -----------------------------------------------------------------------
class NAFileSet : public NABasicObject {
  friend class NATable;

 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  NAFileSet(const QualifiedName &fileSetName, const QualifiedName &extFileSetObj, const NAString &extFileSetName,
            enum FileOrganizationEnum org, NABoolean isSystemTable, int countOfFiles,
            Cardinality estimatedNumberOfRecords, int recordLength, int blockSize, int indexLevels,
            const NAColumnArray &allColumns, const NAColumnArray &indexKeyColumns,
            const NAColumnArray &horizontalPartKeyColumns, const NAColumnArray &hiveSortKeyColumns,
            PartitioningFunction *forHorizontalPartitioning, short keytag, long redefTime, NABoolean audited,
            NABoolean auditCompressed, NABoolean compressed, ComCompressionType dcompressed, NABoolean icompressed,
            NABoolean buffered, NABoolean clearOnPurge, NABoolean packedRows, NABoolean hasRemotePartition,
            NABoolean isUniqueSecondaryIndex, NABoolean isNgramIndex, NABoolean isPartLocalBaseIndex,
            NABoolean isPartLocalIndex, NABoolean isPartGlobalIndex, NABoolean isDecoupledRangePartitioned,
            int fileCode, NABoolean isVolatile, NABoolean inMemObjectDefn, long indexUID, TrafDesc *keysDesc,
            int numSaltPartns, int numInitialSaltRegions, Int16 numTrafReplicas,
            NAList<HbaseCreateOption *> *hbaseCreateOptions, CollHeap *h = 0);

  // copy ctor
  NAFileSet(const NAFileSet &orig, CollHeap *h = 0);

  virtual ~NAFileSet();

  NABoolean operator==(const NAFileSet &other) const;

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const QualifiedName &getFileSetName() const { return fileSetName_; }
  const NAString &getExtFileSetName() const { return extFileSetName_; }
  const QualifiedName &getExtFileSetObj() const { return extFileSetObj_; }
  const QualifiedName &getRandomPartition() const;

  const NAColumnArray &getAllColumns() const { return allColumns_; }

  // Method to get counts for various types of columns.
  // - excludeNonUserSpecifiedAlternateIndexColumns returns the number
  //   of columns in an index that are specified directly by the
  //   user (salt,replica included unless exludeSystemColumns is also set).
  //   Returns 0 when called on the clusteringindex.
  // - Other options should be self-explanatory.
  int getCountOfColumns(NABoolean excludeNonKeyColumns = FALSE,
                        NABoolean excludeNonUserSpecifiedAlternateIndexColumns = FALSE,
                        NABoolean excludeSystemColumns = TRUE,
                        NABoolean excludeAlwaysComputedSystemColumns = FALSE) const;

  const NAColumnArray &getIndexKeyColumns() const { return indexKeyColumns_; }

  const TrafDesc *getKeysDesc() const { return keysDesc_; }
  TrafDesc *getKeysDesc() { return keysDesc_; }

  int getCountOfFiles() const { return countOfFiles_; }

  Cardinality getEstimatedNumberOfRecords() const { return estimatedNumberOfRecords_; }
  int getRecordLength() const { return recordLength_; }
  int getLockLength() const { return lockLength_; }
  int getKeyLength();
  int getEncodedKeyLength();
  int getBlockSize() const { return blockSize_; }

  int getIndexLevels() const { return indexLevels_; }

  int getPackingScheme() const { return packingScheme_; }
  int getPackingFactor() const { return packingFactor_; }

  int getFileCode() const { return fileCode_; }

  const long &getIndexUID() const { return indexUID_; }
  long &getIndexUID() { return indexUID_; }

  int numSaltPartns() const { return numSaltPartns_; }
  int numInitialSaltRegions() const { return numInitialSaltRegions_; }
  Int16 numTrafReplicas() const { return numTrafReplicas_; }
  NAList<HbaseCreateOption *> *hbaseCreateOptions() const { return hbaseCreateOptions_; }
  int numHivePartCols() const;

  int numMaxVersions() const { return numMaxVersions_; }

  // ---------------------------------------------------------------------
  // Mutator functions
  // ---------------------------------------------------------------------

  void setCountOfFiles(int count) { countOfFiles_ = count; }
  void setIndexLevels(int numLevels) { indexLevels_ = numLevels; }
  void setHasRemotePartitions(NABoolean flag) { hasRemotePartition_ = flag; }
  void setPartitioningFunction(PartitioningFunction *pFunc) { partFunc_ = pFunc; }
  void setFileSetName(QualifiedName &rhs) { fileSetName_ = rhs; }
  void setEstimatedNumberOfRecords(Cardinality newEstimate) { estimatedNumberOfRecords_ = newEstimate; }
  void markAsHivePartitioningColumn(int colNum);

  // ---------------------------------------------------------------------
  // Query the file organization.
  // ---------------------------------------------------------------------
  NABoolean isKeySequenced() const { return (fileOrganization_ == KEY_SEQUENCED_FILE); }
  NABoolean isHashed() const { return (fileOrganization_ == HASH_FILE); }
  NABoolean isSyskeyLeading() const;
  int getSysKeyPosition() const;
  NABoolean hasSyskey() const;
  NABoolean hasOnlySyskey() const;
  NABoolean hasSingleColVarcharKey() const;

  NABoolean isDecoupledRangePartitioned() const { return isDecoupledRangePartitioned_; }

  // ---------------------------------------------------------------------
  // Query miscellaneous flags.
  // ---------------------------------------------------------------------
  NABoolean isAudited() const { return audited_; }
  NABoolean isAuditCompressed() const { return auditCompressed_; }
  NABoolean isCompressed() const { return compressed_; }
  ComCompressionType getDCompressed() const { return dcompressed_; }
  NABoolean isICompressed() const { return icompressed_; }
  NABoolean isBuffered() const { return buffered_; }
  NABoolean isClearOnPurge() const { return clearOnPurge_; }
  NABoolean isSystemTable() const { return isSystemTable_; }
  NABoolean isRemoteIndexGone() const { return thisRemoteIndexGone_; }
  void setRemoteIndexGone() { thisRemoteIndexGone_ = TRUE; }

  short getKeytag() const { return keytag_; }

  const long &getRedefTime() const { return redefTime_; }

  // ---------------------------------------------------------------------
  // Query the partitioning function.
  // ---------------------------------------------------------------------
  NABoolean isPartitioned() const;

  int getCountOfPartitions() const;

  NABoolean containsPartition(const NAString &partitionName) const;

  const NAColumnArray &getPartitioningKeyColumns() const { return partitioningKeyColumns_; }

  NAString getBestPartitioningKeyColumns(char separator) const;

  const NAColumnArray &getHiveSortKeyColumns() const { return hiveSortKeyColumns_; }

  PartitioningFunction *getPartitioningFunction() const { return partFunc_; }

  NABoolean isPacked() const { return packedRows_; };

  NABoolean hasRemotePartitions() const { return hasRemotePartition_; };

  // Get and Set methods for the bitFlags_.
  NABoolean uniqueIndex() const { return (bitFlags_ & UNIQUE_INDEX) != 0; }
  void setUniqueIndex(NABoolean v) { (v ? bitFlags_ |= UNIQUE_INDEX : bitFlags_ &= ~UNIQUE_INDEX); }
  // for ngram index, Get and Set methods for the bitFlags_.
  NABoolean ngramIndex() const { return (bitFlags_ & NGRAM_INDEX) != 0; }
  void setNgramIndex(NABoolean v) { (v ? bitFlags_ |= NGRAM_INDEX : bitFlags_ &= ~NGRAM_INDEX); }

  NABoolean partLocalBaseIndex() const { return (bitFlags_ & PARTITION_LOCAL_BASE_INDEX) != 0; }
  void setPartLocalBaseIndex(NABoolean v) {
    (v ? bitFlags_ |= PARTITION_LOCAL_BASE_INDEX : bitFlags_ &= ~PARTITION_LOCAL_BASE_INDEX);
  }

  NABoolean partLocalIndex() const { return (bitFlags_ & PARTITION_LOCAL_INDEX) != 0; }
  void setPartLocalIndex(NABoolean v) {
    (v ? bitFlags_ |= PARTITION_LOCAL_INDEX : bitFlags_ &= ~PARTITION_LOCAL_INDEX);
  }

  NABoolean partGlobalIndex() const { return (bitFlags_ & PARTITION_GLOBAL_INDEX) != 0; }
  void setPartGlobalIndex(NABoolean v) {
    (v ? bitFlags_ |= PARTITION_GLOBAL_INDEX : bitFlags_ &= ~PARTITION_GLOBAL_INDEX);
  }

  NABoolean notAvailable() const { return (bitFlags_ & NOT_AVAILABLE) != 0; }
  void setNotAvailable(NABoolean v) { (v ? bitFlags_ |= NOT_AVAILABLE : bitFlags_ &= ~NOT_AVAILABLE); }

  NABoolean isVolatile() const { return (bitFlags_ & IS_VOLATILE) != 0; }
  void setIsVolatile(NABoolean v) { (v ? bitFlags_ |= IS_VOLATILE : bitFlags_ &= ~IS_VOLATILE); }

  const NABoolean isInMemoryObjectDefn() const { return (bitFlags_ & IN_MEMORY_OBJECT_DEFN) != 0; }
  void setInMemoryObjectDefn(NABoolean v) {
    (v ? bitFlags_ |= IN_MEMORY_OBJECT_DEFN : bitFlags_ &= ~IN_MEMORY_OBJECT_DEFN);
  }

  NABoolean isCreatedExplicitly() const { return (bitFlags_ & IS_EXPLICIT_INDEX) != 0; }
  void setIsCreatedExplicitly(NABoolean v) { (v ? bitFlags_ |= IS_EXPLICIT_INDEX : bitFlags_ &= ~IS_EXPLICIT_INDEX); }

  NABoolean isNullablePkey() const { return (bitFlags_ & NULLABLE_PKEY) != 0; }
  void setNullablePkey(NABoolean v) { (v ? bitFlags_ |= NULLABLE_PKEY : bitFlags_ &= ~NULLABLE_PKEY); }

  // For NATable caching, clear up the statement specific stuff
  // so that this can be used for following statements
  void resetAfterStatement();

  // For NATable caching, prepare for use by a statement.
  void setupForStatement();

  NABoolean isSqlmxAlignedRowFormat() const { return rowFormat_ == COM_ALIGNED_FORMAT_TYPE; }

  void setRowFormat(ComRowFormat rowFormat) { rowFormat_ = rowFormat; }

 private:
  // ---------------------------------------------------------------------
  // The fully qualified PHYSICAL name for the file set,
  // e.g. { "\NSK.$VOL", ZSDNGUOF, T5AB0000 }.
  // ---------------------------------------------------------------------
  QualifiedName fileSetName_;

  // ---------------------------------------------------------------------
  // The EXTERNAL name of fileset, as specified by the user.
  // For ARK tables, this is the ANSI name of the index/basetable specified
  // when the index/basetable was created (the preceding field, fileSetName_,
  // thus represents the actual physical name of the index).
  //
  // For SQL/MP or simulator tables, both fileSetName_ and extFileSetName_
  // fields are nearly the same, just formatted differently (one without
  // double-quote delimiters, one with).
  // ---------------------------------------------------------------------
  NAString extFileSetName_;
  QualifiedName extFileSetObj_;

  // ---------------------------------------------------------------------
  // File organization.
  // The following enum describes how records are organized in each
  // file of this file set.
  // ---------------------------------------------------------------------
  FileOrganizationEnum fileOrganization_;

  // ---------------------------------------------------------------------
  // The number of files that belong to this file set.
  // (May need a set of file names/NAFile also/instead someday.)
  // ---------------------------------------------------------------------
  int countOfFiles_;

  // ---------------------------------------------------------------------
  // The number of records can either be a value that is computed by
  // UPDATE STATISTICS or may be estimated by examining the EOFs of
  // each file in this file set and accumulating an estimate.
  // ---------------------------------------------------------------------
  Cardinality estimatedNumberOfRecords_;

  // ---------------------------------------------------------------------
  // Record length in bytes.
  // ---------------------------------------------------------------------
  int recordLength_;

  // ---------------------------------------------------------------------
  // Key length in bytes.
  //----------------------------------------------------------------------
  int keyLength_;

  // ---------------------------------------------------------------------
  // Encoded key length in bytes.
  //----------------------------------------------------------------------
  int encodedKeyLength_;

  // ---------------------------------------------------------------------
  // Lock length in bytes.
  //----------------------------------------------------------------------
  int lockLength_;

  // ---------------------------------------------------------------------
  // Filecode.
  // ---------------------------------------------------------------------
  int fileCode_;

  // ---------------------------------------------------------------------
  // Size of a page (block) that is a contant for every file that
  // belongs to this file set. It is expressed in bytes.
  // ---------------------------------------------------------------------
  int blockSize_;

  // ----------------------------------------------------------------------
  // Packing information. Packing scheme: describes version of packed
  // record format used in this file set's packed records.
  // Packing factor: number of unpacked (logical) rows per packed (physical) record.
  // ----------------------------------------------------------------------
  int packingScheme_;
  int packingFactor_;

  // ---------------------------------------------------------------------
  // since the index levels could be different for every file that
  // belongs to this file set, we probably should save index levels
  // per file. But, for now, we save the maximum of the index levels
  // of the files that belong to this fileset.
  // ---------------------------------------------------------------------
  int indexLevels_;

  // ---------------------------------------------------------------------
  // Array of all the columns that appear in each record in every file
  // that belongs to this file set.
  // ---------------------------------------------------------------------
  NAColumnArray allColumns_;

  // ---------------------------------------------------------------------
  // Array of key columns that implement a B+ tree or hash index per
  // file that belongs to this file set.
  // ---------------------------------------------------------------------
  NAColumnArray indexKeyColumns_;

  // uid for index
  long indexUID_;

  TrafDesc *keysDesc_;  // needed for parallel label operations.

  // ---------------------------------------------------------------------
  // Horizontal partitioning:
  // The partitioning function describes how data is distributed
  // amongst the files contained in this file set. Note that the scheme
  // for distributing the data is orthogonal to the scheme for
  // organizing records within a file.
  // ---------------------------------------------------------------------

  // --------------------------------------------------------------------
  // For persistent range or hash partitioned data, the partitioning
  // key is expressed as a list of NAColumns. The list represents the
  // sequence in which the key columns appear in the declaration of
  // the partitioning scheme such as the SQL CREATE TABLE statement.
  // It does not imply an ordering on the key columns. If an ordering
  // exists, it is implemented by a partitioning function.
  //
  // A NAFileSet need not contain a partitioning key.
  // For Hive tables, the bucketing key columns are stored in the
  // partitioning key, since this comes closest to the HASH2
  // partitioning of other (salted) tables. Optional Hive sort
  // columns are stored in another column array here.
  // --------------------------------------------------------------------
  NAColumnArray partitioningKeyColumns_;
  NAColumnArray hiveSortKeyColumns_;

  PartitioningFunction *partFunc_;

  // ---------------------------------------------------------------------
  // Some day we may support arbitrary queries, then an additional
  // data member could hold the relational expression that defines
  // the content of the file set. The file set columns would then refer
  // to the result of this expression.
  // ---------------------------------------------------------------------
  // RelExpr *fileSetQuery_; // unbound fileSet definition (no value ids)

  // ---------------------------------------------------------------------
  // This member is poorly named.  For all base tables (MP or MX),
  // i.e. for all primary index NAFilesets, this is zero.
  // For all secondary indexes (MP or MX), this is nonzero.
  // ---------------------------------------------------------------------
  short keytag_;

  // ---------------------------------------------------------------------
  // Catalog timestamp for this fileset. Each index gets its own timestamp.
  // ---------------------------------------------------------------------
  long redefTime_;

  // ---------------------------------------------------------------------
  // Miscellaneous flags
  // ---------------------------------------------------------------------
  NABoolean audited_;
  NABoolean auditCompressed_;
  NABoolean compressed_;
  ComCompressionType dcompressed_;
  NABoolean icompressed_;
  NABoolean buffered_;
  NABoolean clearOnPurge_;
  NABoolean isSystemTable_;
  NABoolean packedRows_;
  NABoolean hasRemotePartition_;
  NABoolean setupForStatement_;
  NABoolean resetAfterStatement_;
  NABoolean isDecoupledRangePartitioned_;

  // ---------------------------------------------------------------------
  // Miscellaneous Bit flags. It reduces compiler memory consumption +
  // no need to recompile the world on adding a new flag.
  // ---------------------------------------------------------------------
  enum BitFlags {
    // Set, if this is a unique secondary index.
    UNIQUE_INDEX = 0x0001,

    // Set, if this index is not available.
    // Could be due to no populate, or if an mv has not been
    // refreshed, or others...
    NOT_AVAILABLE = 0x0002,

    // if this is a volatile index
    IS_VOLATILE = 0x0004,

    // if set, indicates that this object is created in mxcmp/catman
    // memory and not available in metadata or disk.
    // Used to test out plans on tables/indexes without actually
    // creating them.
    IN_MEMORY_OBJECT_DEFN = 0x0008,

    // if this index was explicitly created by user and not internally
    // created to implement a unique constraint.
    IS_EXPLICIT_INDEX = 0x00010,

    // set, if this is an ngram index
    NGRAM_INDEX = 0X00020,

    // During create time, nullable pkey columns are automatically made
    // non-nullable unless 'PRIMARY KEY NULLABLE' attribute is specified.
    // If it is, column nullability is not changed.
    // If at least one pkey column is nullable, then it becomes a nullable
    // primary key.
    NULLABLE_PKEY = 0x00040,

    // partition local index for partition base table.
    PARTITION_LOCAL_BASE_INDEX = 0x00080,

    // partition local index for each partition table.
    PARTITION_LOCAL_INDEX = 0x00100,

    PARTITION_GLOBAL_INDEX = 0x00200,

  };
  int bitFlags_;

  NABoolean thisRemoteIndexGone_;

  // number of salt buckets and initial regions specified at
  // table create time in the SALT clause
  int numSaltPartns_;
  int numInitialSaltRegions_;
  Int16 numTrafReplicas_;

  // if table was created with max versions greater than 1 using
  // hbase_options clause.
  int numMaxVersions_;

  NAList<HbaseCreateOption *> *hbaseCreateOptions_;

  ComRowFormat rowFormat_;

  // Indicate whether some data member is possiblly
  // shallow copied. Set to TRUE when this is copy
  // constructed.
  NABoolean shallowCopied_;
};

class NAFileSetList : public LIST(NAFileSet *) {
 public:
  NAFileSetList(CollHeap *h) : LIST(NAFileSet *)(h) {}

  // copy cstr
  NAFileSetList(const NAFileSetList &other, CollHeap *h) : LIST(NAFileSet *)(other, h) {}
};

#endif /* NAFILESET_H */
