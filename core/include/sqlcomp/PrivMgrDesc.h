//*****************************************************************************

//*****************************************************************************

#ifndef PRIVMGR_DESC_H
#define PRIVMGR_DESC_H

#include <bitset>
#include <string>

#include "common/ComSmallDefs.h"
#include "sqlcomp/PrivMgrDefs.h"

class ComDiags;

// *****************************************************************************
// Contents of file
// *****************************************************************************
class PrivMgrCoreDesc;
class PrivMgrDesc;

// *****************************************************************************
// *
// * The privilege manager component stores privileges as bitmaps.
// * Classes and methods exist to create (grant), drop (revoke), and
// * peruse (select) privilege information for objects and user/roles.
// *
// * The PrivMgrCoreDesc class is the basic building block for privilege
// * support.  It encapsulates the list of privileges assigned to a
// * object and user/role (grantee) assigned by the grantor into a bitmap.
// * It also encapsulates which privileges are grantable (WGO) by the grantor.
// *    member: priv_ is the list of privileges
// *    member: wgo_ is the corresponding grantable attribute
// *
// * The PrivMgrDesc class contains a list of priv_/wgo_ combinations.
// * Privileges can be granted at the schema, object, and column level.
// * The PrivMgrDesc class returns information about privileges on objects
// * for grantees across all level of privileges.
// *
// ****************************************************************************

// *****************************************************************************
// *
// * Class:        PrivMgrCoreDesc
// * Description:  This class defines the basic representation of privilege and
// *               WGO.  It can be used to carry the privilege settings for
// *               some (unspecified) user on some (unspecified) object, either
// *               as granted by some grantor, or summarized over all grantors.
// *
// * When used to represent privileges held, the valid combinations are:
// *
// *         priv_    wgo_
// *         -----    ----
// *         F        F     priv not held
// *         T        F     priv held, not grantable
// *         T        T     priv held, grantable.
// *
// * So the wgo_ value can be regarded as an option (which it is) on the
// * priv_ value, having meaning only if priv_ = T.
// *
// * When used to represent privileges to be granted, the valid combinations
// * are the same as the above, with the following interpretations:
// *         priv_    wgo_
// *         -----    ----
// *         F        F     no change to privilege
// *         T        F     priv to be granted, but not wgo
// *         T        T     priv to be granted, wgo.
// *
// * When used to represent privileges which have been successfully granted,
// * all four possible combinations are valid:
// *         priv_    wgo_
// *         -----    ----
// *         F        F     no change to privilege
// *         F        T     only wgo was granted (priv was already held)
// *         T        F     priv was granted, but not wgo
// *         T        T     priv was granted wgo.
// *
// * When used to represent privileges to be revoked, the valid combinations
// * are:
// *         priv_    wgo_
// *         -----    ----
// *         F        F    nothing to be revoked
// *         F        T    revoke grant option only
// *         T        T    revoke priv and grant option.
// *
// * Here the priv_ can be regarded as an option on the wgo_ value.  We always
// * revoke the wgo (if we revoke anything at all), and optionally revoke the
// * entire privilege.
// *   "Revoke SELECT on.." is represented as T T.
// *   "Revoke grant option for SELECT on.." is represented as F T.
// *
// * When used to represent privileges which have been successfully revoked,
// * all four possible combinations are valid:
// *         priv_    wgo_
// *         -----    ----
// *         F        F    nothing was revoked
// *         F        T    only the grant option was revoked (priv remains)
// *         T        F    only priv was revoked (wgo was not currently held)
// *         T        T    priv and grant option were revoked.
// *
// *****************************************************************************

class PrivMgrCoreDesc {
 public:
  // PrivResult indicates the result of a Grant/Revoke operation.
  //   NONE means none of the specified privs were actually
  //       granted/revoked.
  //   SOME means some but not all ..
  //   ALL means all ..
  //   NEUTRAL means no privs were specified could be changed.
  enum PrivResult { NONE, SOME, ALL, NEUTRAL };

  // PrivChanges indicates the result of a replacement operation.
  //   NO_CHANGE means the privilege settings are unchanged.
  //   DECREASE means some privileges were lost.
  //   INCREASE means some privileges were added.
  //   AMBIGUOUS means some privileges were lost, others added.
  enum PrivChanges { NO_CHANGE, DECREASE, INCREASE, AMBIGUOUS };

  PrivMgrCoreDesc() {
    priv_.reset();
    wgo_.reset();
    columnOrdinal_ = -1;
  }

  PrivMgrCoreDesc(PrivMgrBitmap privBits, PrivMgrBitmap wgoBits) : priv_(privBits), wgo_(wgoBits), columnOrdinal_(-1) {}

  PrivMgrCoreDesc(PrivMgrBitmap privBits, PrivMgrBitmap wgoBits, int32_t columnOrdinal)
      : priv_(privBits), wgo_(wgoBits), columnOrdinal_(columnOrdinal) {}

  PrivMgrCoreDesc(const PrivMgrCoreDesc &other)  // copy constructor
  {
    priv_ = other.priv_;
    wgo_ = other.wgo_;
    columnOrdinal_ = other.columnOrdinal_;
  }

  virtual ~PrivMgrCoreDesc()  // destructor
  {}

  // assignment operator
  PrivMgrCoreDesc &operator=(const PrivMgrCoreDesc &other) {
    //  Check for pathological case of X == X.
    if (this == &other) return *this;

    priv_ = other.priv_;
    wgo_ = other.wgo_;
    columnOrdinal_ = other.columnOrdinal_;

    return *this;
  }

  // comparison operator
  bool operator==(const PrivMgrCoreDesc &other) const;

  // isNull - returns True iff no privs or wgos are set
  bool isNull() const { return ((priv_.none()) && (wgo_.none())); }

  // isNullWgo - returns True iff no wgos are set
  bool isNullWgo() const { return (wgo_.none()); }

  // union -- modify the privilege settings by ORing values
  //    of another PrivMgrCoreDesc with this one.
  void unionOfPrivs(const PrivMgrCoreDesc &other) {
    priv_ |= other.priv_;
    wgo_ |= other.wgo_;
  }

  // intersection -- modify the privilege settings by ANDing values
  //    of another PrivMgrCoreDesc with this one.
  void intersectionOfPrivs(const PrivMgrCoreDesc &other) {
    priv_ &= other.priv_;
    wgo_ &= other.wgo_;
  }

  // complement -- replace the privilege settings with their complement.
  void complementPrivs() {
    priv_.flip();
    wgo_.flip();
  }

  // AndNot -- sets "this" to "this" AND NOT "other"
  void AndNot(const PrivMgrCoreDesc &other);

  // grant -- modify the privilege settings of target PrivMgrCoreDesc
  //    by ORing values of this one.  Change values of this
  //    to carry only the privileges actually granted.
  //      (Refer to the .cpp file for the algorithm.)
  PrivResult grantPrivs(PrivMgrCoreDesc &other);

  // revoke -- modify the privilege settings of target PrivMgrCoreDesc
  //    by ANDing values of this one. Change values of this
  //    to carry only the privileges actually revoked.
  //      (Refer to the .cpp file for the algorithm.)
  PrivResult revokePrivs(PrivMgrCoreDesc &other);

  // replace -- replace the privilege settings of this PrivMgrCoreDesc
  //    with the values of the specified one.  Return value
  //    indicates whether privs were decreased/increased.
  //    When privs are lost, return those values in lostPrivs.
  PrivChanges replacePrivs(const PrivMgrCoreDesc &other, PrivMgrCoreDesc &lostPrivs);

  // Remove from this descriptor any privileges which
  // are not held WGO in other.
  // Return True if any privileges were removed; else False.
  bool limitToGrantable(const PrivMgrCoreDesc &other);

  // Remove from this descriptor any priv/wgo settings
  // which also appear in other.
  // Return True if any privileges were removed; else False.
  bool suppressDuplicatedPrivs(const PrivMgrCoreDesc &other);

  // Return True iff any priv/wgo flag set in this is not set in other.
  // (Evaluate whether this && !other is null.)
  bool anyNotSet(const PrivMgrCoreDesc &other) const;

  // For any priv/wgo flag set in this is not set in other,
  // unset the value in this.
  // Return True if any change was made.
  bool cascadeAnyNotSet(const PrivMgrCoreDesc &other);

  // Returns true if the bit was set in the bitmap.  It returns
  // false is the bit was already set.  If false is returned,
  // then there are probably duplicate privileges specified in
  // the privilege list
  inline bool testAndSetBit(PrivType i, bool isWGOSpecified, bool isGOFSpecified);

  // -------------------------------------------------------------------
  // Accessors:
  // -------------------------------------------------------------------
  PrivMgrBitmap getPrivBitmap(void) const { return priv_; }
  bool getPriv(const PrivType which) const { return priv_.test(which); }

  PrivMgrBitmap getWgoBitmap(void) const { return wgo_; }
  bool getWgo(const PrivType which) const { return wgo_.test(which); }

  int32_t getColumnOrdinal(void) const { return columnOrdinal_; }

  // -------------------------------------------------------------------
  // Mutators:
  // -------------------------------------------------------------------
  void setPriv(const PrivType which, const bool value);
  void setWgo(const PrivType which, const bool value);
  void setAllPrivAndWgo(const bool val);

  void setAllObjectPrivileges(const ComObjectType objectType, const bool priv, const bool wgo, const bool all_dml,
                              const bool all_ddl);

  inline void setAllLibraryGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    if (all_dml) {
      setPriv(UPDATE_PRIV, priv);
      setWgo(UPDATE_PRIV, wgo);
      setPriv(USAGE_PRIV, priv);
      setWgo(USAGE_PRIV, wgo);
    }
    if (all_ddl) {
      setPriv(ALTER_PRIV, priv);
      setWgo(ALTER_PRIV, wgo);
      setPriv(DROP_PRIV, priv);
      setWgo(DROP_PRIV, wgo);
    }
  }

  inline void setAllTableGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    if (all_dml) {
      setPriv(SELECT_PRIV, priv);
      setWgo(SELECT_PRIV, wgo);
      setPriv(INSERT_PRIV, priv);
      setWgo(INSERT_PRIV, wgo);
      setPriv(DELETE_PRIV, priv);
      setWgo(DELETE_PRIV, wgo);
      setPriv(UPDATE_PRIV, priv);
      setWgo(UPDATE_PRIV, wgo);
      setPriv(REFERENCES_PRIV, priv);
      setWgo(REFERENCES_PRIV, wgo);
    }
    if (all_ddl) {
      setPriv(ALTER_PRIV, priv);
      setWgo(ALTER_PRIV, wgo);
      setPriv(DROP_PRIV, priv);
      setWgo(DROP_PRIV, wgo);
    }
  }

  inline void setAllSchemaGrantPrivileges(const bool wgo, const bool all_dml, const bool all_ddl) {
    if (all_dml) {
      setPriv(SELECT_PRIV, TRUE);
      setWgo(SELECT_PRIV, wgo);
      setPriv(INSERT_PRIV, TRUE);
      setWgo(INSERT_PRIV, wgo);
      setPriv(DELETE_PRIV, TRUE);
      setWgo(DELETE_PRIV, wgo);
      setPriv(UPDATE_PRIV, TRUE);
      setWgo(UPDATE_PRIV, wgo);
      setPriv(USAGE_PRIV, TRUE);
      setWgo(USAGE_PRIV, wgo);
      setPriv(REFERENCES_PRIV, TRUE);
      setWgo(REFERENCES_PRIV, wgo);
      setPriv(EXECUTE_PRIV, TRUE);
      setWgo(EXECUTE_PRIV, wgo);
    }
    if (all_ddl) {
      setPriv(CREATE_PRIV, TRUE);
      setWgo(CREATE_PRIV, wgo);
      setPriv(ALTER_PRIV, TRUE);
      setWgo(ALTER_PRIV, wgo);
      setPriv(DROP_PRIV, TRUE);
      setWgo(DROP_PRIV, wgo);
    }
  }

  inline void setAllSequenceGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    if (all_dml) {
      setPriv(USAGE_PRIV, priv);
      setWgo(USAGE_PRIV, wgo);
    }
    if (all_ddl) {
      setPriv(ALTER_PRIV, priv);
      setWgo(ALTER_PRIV, wgo);
      setPriv(DROP_PRIV, priv);
      setWgo(DROP_PRIV, wgo);
    }
  }

  inline void setAllUdrGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    if (all_dml) {
      setPriv(EXECUTE_PRIV, priv);
      setWgo(EXECUTE_PRIV, wgo);
    }
    if (all_ddl) {
      setPriv(ALTER_PRIV, priv);
      setWgo(ALTER_PRIV, wgo);
      setPriv(DROP_PRIV, priv);
      setWgo(DROP_PRIV, wgo);
    }
  }

  inline void setColumnOrdinal(const int32_t columnOrdinal) { columnOrdinal_ = columnOrdinal; }
  inline void setPrivBitmap(PrivMgrBitmap priv) { priv_ = priv; }
  inline void setWgoBitmap(PrivMgrBitmap wgo) { wgo_ = wgo; }

  NAString print() {
    std::string priv = priv_.to_string<char, std::string::traits_type, std::string::allocator_type>();
    return NAString(priv);
  }

 private:
  PrivMgrBitmap priv_;  // Bit == True if the privilege is held.

  PrivMgrBitmap wgo_;  // == True if the priv is held grantable.

  int32_t columnOrdinal_;

  // Private helper function to interpret changes to specified privs.
  void interpretChanges(const bool before,    // in
                        const bool after,     // in
                        PrivResult &result);  // in/out

};  // class PrivMgrCoreDesc

inline bool PrivMgrCoreDesc::testAndSetBit(PrivType i, bool isWGOSpecified, bool isGOFSpecified) {
  if (isGOFSpecified) {
    if (wgo_.test(i)) return false;
    wgo_.set(i, true);
  } else {
    if (priv_.test(i)) return false;
    priv_.set(i, true);
    wgo_.set(i, isWGOSpecified);
  }
  return true;
}

/* *******************************************************************
 * Class PrivMgrDesc -- Representation of all privs on an object
 * for a specified Grantee and an unspecified Grantor (or over all Grantors).
 * ****************************************************************** */
class PrivMgrDesc {
 public:
  PrivMgrDesc(const PrivMgrDesc &other)  // copy constructor
      : tableLevel_(other.tableLevel_),
        columnLevel_(other.columnLevel_),
        schemaLevel_(other.schemaLevel_),
        grantee_(other.grantee_),
        schemaUID_(other.schemaUID_),
        hasPublicPriv_(other.hasPublicPriv_),
        schemaObject_(other.schemaObject_) {}

  PrivMgrDesc(const int32_t grantee, const bool schemaObject = false)
      : tableLevel_(),
        columnLevel_(NULL),
        schemaLevel_(),
        grantee_(grantee),
        schemaUID_(0),
        schemaObject_(schemaObject),
        hasPublicPriv_(false) {}

  PrivMgrDesc(const PrivMgrDesc &privs,  // preset constructor
              const int32_t grantee)
      : tableLevel_(privs.tableLevel_),
        columnLevel_(privs.columnLevel_, NULL),
        schemaLevel_(privs.schemaLevel_),
        grantee_(privs.grantee_),
        schemaUID_(privs.schemaUID_),
        hasPublicPriv_(privs.hasPublicPriv_),
        schemaObject_(privs.schemaObject_) {}

  PrivMgrDesc(void)
      : tableLevel_(),
        columnLevel_(NULL),
        schemaLevel_(),
        grantee_(0),
        schemaUID_(0),
        hasPublicPriv_(false),
        schemaObject_(false) {}

  virtual ~PrivMgrDesc()  // destructor
  {}

  // assignment operator
  PrivMgrDesc &operator=(const PrivMgrDesc &other) {
    //  Check for pathological case of X == X.
    if (this == &other) return *this;

    tableLevel_ = other.tableLevel_;
    columnLevel_ = other.columnLevel_;
    schemaLevel_ = other.schemaLevel_;
    grantee_ = other.grantee_;
    schemaUID_ = other.schemaUID_;
    hasPublicPriv_ = other.hasPublicPriv_;
    schemaObject_ = other.schemaObject_;

    return *this;
  }

  // comparison operator
  bool operator==(const PrivMgrDesc &other) const {
    //  Check for pathological case of X == X.
    if (this == &other) return TRUE;

    // Not checking all members, should be okay
    return ((columnLevel_ == other.columnLevel_) && (tableLevel_ == other.tableLevel_) &&
            (schemaLevel_ == other.schemaLevel_) && (grantee_ == other.grantee_));
  }

  // Union -- OR current privilege settings in this PrivMgrDesc with other
  void unionOfPrivs(const PrivMgrDesc &other) {
    if (schemaObject_)
      schemaLevel_.unionOfPrivs(other.schemaLevel_);
    else
      tableLevel_.unionOfPrivs(other.tableLevel_);
  }
  void unionOfPrivs(const PrivMgrCoreDesc &other) {
    if (schemaObject_)
      schemaLevel_.unionOfPrivs(other);
    else
      tableLevel_.unionOfPrivs(other);
  }

  // Intersection -- AND current privilege settings in this PrivMgrDesc with other
  void intersectionOfPrivs(const PrivMgrDesc &other) { tableLevel_.intersectionOfPrivs(other.tableLevel_); }
  void intersectionOfPrivs(const PrivMgrCoreDesc &other) { tableLevel_.intersectionOfPrivs(other); }

  // Complement -- take the complement of each priv setting in this.
  void complement() { tableLevel_.complementPrivs(); }

  // AndNot -- sets "this" to "this" AND NOT "other"
  void AndNot(const PrivMgrDesc &other);

  // isNull - returns True iff no privs or wgos
  bool isNull() const {
    if (tableLevel_.isNull() && schemaLevel_.isNull()) return isColumnLevelNull();
    return false;
  }

  bool isColumnLevelNull() const {
    for (int i = 0; i < columnLevel_.entries(); i++)
      if (!columnLevel_[i].isNull()) return false;
    return true;
  }

  // isNullWgo - returns True iff no wgos are set
  bool isNullWgo() const;

  // Remove from this descriptor any privileges
  // (both priv and WGO) which are not held WGO in other.
  // Return True if any privileges were removed; else False.
  bool limitToGrantable(const PrivMgrDesc &other);

  // Remove from this descriptor any priv/wgo settings
  // which also appear in other.
  // Return True if any privileges were removed; else False.
  bool suppressDuplicatedPrivs(const PrivMgrDesc &other);

  // Accessors

  PrivMgrCoreDesc getTablePrivs() const { return tableLevel_; }
  NAList<PrivMgrCoreDesc> getColumnPrivs() const { return columnLevel_; }
  PrivMgrCoreDesc getSchemaPrivs() const { return schemaLevel_; }

  // Get the PrivMgrCoreDesc based on the columnOrdinal (column number)
  int getColumnPriv(int32_t columnOrdinal) const {
    for (int i = 0; i < columnLevel_.entries(); i++)
      if (columnLevel_[i].getColumnOrdinal() == columnOrdinal) return i;
    return -1;
  }

  int32_t getGrantee() const { return grantee_; }
  int64_t getSchemaUID() const { return schemaUID_; }
  PrivMgrCoreDesc &fetchTablePrivs();
  bool getOneTablePriv(const PrivType which) const;
  bool getOneTableWgo(const PrivType which) const;

  // Mutators

  void setGrantee(const int32_t &grantee) { grantee_ = grantee; }
  void setSchemaUID(const int64_t &schemaUID) { schemaUID_ = schemaUID; }
  void setTablePrivs(const PrivMgrCoreDesc &privs) { tableLevel_ = privs; }
  void resetTablePrivs() { tableLevel_.setAllPrivAndWgo(0); }
  void setColumnPrivs(const NAList<PrivMgrCoreDesc> &privs) { columnLevel_ = privs; }
  void setSchemaPrivs(const PrivMgrCoreDesc &privs) { schemaLevel_ = privs; }
  void resetSchemaPrivs() { schemaLevel_.setAllPrivAndWgo(0); }

  void setAllObjectPrivileges(const ComObjectType objectType, const bool priv, const bool wgo, const bool all_dml,
                              const bool all_ddl) {
    PrivMgrCoreDesc objectCorePrivs;
    objectCorePrivs.setAllObjectPrivileges(objectType, priv, wgo, all_dml, all_ddl);
    setTablePrivs(objectCorePrivs);
  }

  void setAllTableGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    PrivMgrCoreDesc tableCorePrivs;
    tableCorePrivs.setAllTableGrantPrivileges(priv, wgo, all_dml, all_ddl);
    setTablePrivs(tableCorePrivs);
  }

  void setAllSchemaGrantPrivileges(const bool wgo, const bool all_dml, const bool all_ddl) {
    PrivMgrCoreDesc schemaCorePrivs;

    schemaCorePrivs.setAllSchemaGrantPrivileges(wgo, all_dml, all_ddl);

    setSchemaPrivs(schemaCorePrivs);
  }

  void setAllLibraryGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    PrivMgrCoreDesc tableCorePrivs;
    tableCorePrivs.setAllLibraryGrantPrivileges(priv, wgo, all_dml, all_ddl);
    setTablePrivs(tableCorePrivs);
  }

  void setAllUdrGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    PrivMgrCoreDesc tableCorePrivs;
    tableCorePrivs.setAllUdrGrantPrivileges(priv, wgo, all_dml, all_ddl);
    setTablePrivs(tableCorePrivs);
  }

  void setAllSequenceGrantPrivileges(const bool priv, const bool wgo, const bool all_dml, const bool all_ddl) {
    PrivMgrCoreDesc corePrivs;
    corePrivs.setAllSequenceGrantPrivileges(priv, wgo, all_dml, all_ddl);
    setTablePrivs(corePrivs);
  }

  bool getHasPublicPriv() { return hasPublicPriv_; }
  void setHasPublicPriv(bool hasPublicPriv) { hasPublicPriv_ = hasPublicPriv; }

  bool isSchemaObject() { return schemaObject_; }
  void setSchemaObject(bool isSchemaObject) { schemaObject_ = isSchemaObject; }
  PrivMgrCoreDesc::PrivResult grantTablePrivs(PrivMgrCoreDesc &priv) { return tableLevel_.grantPrivs(priv); }

  PrivMgrCoreDesc::PrivResult revokeTablePrivs(PrivMgrCoreDesc &priv) { return tableLevel_.revokePrivs(priv); }

  PrivMgrCoreDesc::PrivResult grantSchemaPrivs(PrivMgrCoreDesc &priv) { return schemaLevel_.grantPrivs(priv); }

  PrivMgrCoreDesc::PrivResult revokeSchemaPrivs(PrivMgrCoreDesc &priv) { return schemaLevel_.revokePrivs(priv); }

  NAString print();  // Debug trace

  // Data members
 private:
  PrivMgrCoreDesc tableLevel_;
  NAList<PrivMgrCoreDesc> columnLevel_;
  PrivMgrCoreDesc schemaLevel_;
  int32_t grantee_;
  int64_t schemaUID_;
  bool hasPublicPriv_;
  bool schemaObject_;
};

/* *******************************************************************
 * Class PrivMgrDescList -- A list of PrivMgrDesc pointers
 * ****************************************************************** */

class PrivMgrDescList : public LIST(PrivMgrDesc *) {
 public:
  // constructor
  PrivMgrDescList(CollHeap *heap) : LIST(PrivMgrDesc *)(heap), heap_(heap) {}

  // copy constructor
  PrivMgrDescList(const PrivMgrDescList &other, CollHeap *heap) : LIST(PrivMgrDesc *)(other, heap), heap_(heap) {}

  // virtual destructor
  virtual ~PrivMgrDescList() {
    for (CollIndex i = 0; i < entries(); i++) NADELETE(operator[](i), PrivMgrDesc, heap_);
    clear();
  }

  CollHeap *getHeap() { return heap_; }

  void print() {
    for (CollIndex i = 0; i < entries(); i++) {
      PrivMgrDesc *desc = operator[](i);
      cout << desc->print().data() << endl;
    }
  }

 private:
  CollHeap *heap_;

};  // class PrivMgrDescList

#endif  // PRIVMGR_DESC_H
