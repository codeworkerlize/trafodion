

#ifndef LMCONNECTION_H
#define LMCONNECTION_H


#include "ComSmallDefs.h"
#include "LmLangManagerJava.h"
#include "Collections.h"
#include "LmJavaExceptionReporter.h"
#include "jni.h"

class LmRoutineJava;

class SQLLM_LIB_FUNC LmConnection : public NABasicObject
{
friend class LmRoutineJava;
friend class LmResultSetJava;

public:
  enum LmConnectionType {
    DEFAULT_CONN,      // JDBC/MX connection created using url: 
                       // "jdbc:default:connection"
    NON_DEFAULT_CONN,  // JDBC/MX connection created using url:
                       // "jdbc:sqlmx:"
  };

  // Accessor methods
  NABoolean readyToClose() { return (refCnt_ > 0 ? FALSE : TRUE); }
  jobject getConnRef() const { return jdbcConnRef_; }
  LmConnectionType connType() { return type_; }

private:

  // The constructor and destructor are defined as private 
  // since the object management of this class can only be 
  // done by the LmRoutineJava and LmResultSetJava classes.

  // Constructor
  LmConnection( LmLanguageManagerJava *lm,
                jobject connRef,
                LmConnectionType connType,
                ComRoutineTransactionAttributes transactionAttrs);

  // Destructor: Should be called only from within the
  // close() method
  ~LmConnection();

  // A reference counting mechanism is used to determine when
  // to destruct an object of this class. The 'refCnt_' field 
  // of this class is:
  // - incremented by '1' whenever a LmResultSetJava object is 
  //   instantiated for a java.sql.ResultSet object that is 
  //    associated with this connection.
  // - decremented by '1' when the LmResultSetJava object 
  //   created above is destroyed.

  // Decrements 'refCnt_' by '1' and when it becomes '0'
  // calls the close() method of this class
  // Returns a boolean value to indicate if the close()
  // method was called
  NABoolean decrRefCnt( ComDiagsArea *diags = NULL );

  // Increments 'refCnt_' by '1'
  void incrRefCnt() { refCnt_++; }

  // Calls the close() method on 'jdbcConnRef_'
  // Removes this object from the 'lmConnList_'
  // Calls the destructor of this object
  void close( ComDiagsArea *diags = NULL );

  // Do not implement default constructors or an assignment operator
  LmConnection();
  LmConnection(const LmConnection &);
  LmConnection &operator=(const LmConnection &);

private:
  LmLanguageManagerJava *lmj_;
  LmConnectionType type_;       // Connection type - Default or Non-Default
  ComSInt32 refCnt_;            // Reference count to track the number of result
                                // sets that are associated with this connection
  jobject	jdbcConnRef_;         // The java.sql.Connection object reference
  ComRoutineTransactionAttributes transactionAttrs_; //COM_UNKNOWN_ROUTINE_TRANSACTION_ATTRIBUTE or COM_NO_TRANSACTION_REQUIRED or COM_TRANSACTION_REQUIRED

};

#endif
