#ifndef LMLANGMANAGERJAVA_H
#define LMLANGMANAGERJAVA_H
/* -*-C++-*-
**********************************************************************
*
* File:         LmLangManagerJava.h
* Description:  Language Manager for Java definitions
*
* Created:      07/01/1999
* Language:     C++
*

**********************************************************************/

#include "LmLangManager.h"
#include "LmParameter.h"

#define JDBC_SPJRS_VERSION 1

#if defined(CONVERT_INPUT_TO_IEEE) || defined(CONVERT_RETURN_FROM_IEEE)
extern "C" {
float Tandem2IEEE_float(float org);
float IEEE2Tandem_float(float org);
double Tandem2IEEE_double(double org);
double IEEE2Tandem_double(double org);
}
#endif

//////////////////////////////////////////////////////////////////////
//
// Forward Reference Classes
//
//////////////////////////////////////////////////////////////////////
class ComDiagsArea;
class LmJavaOptions;
class LmJavaExceptionReporter;
class LmRoutineJava;
class LmContainerManager;
class LmResultSetJava;
class LmResultSet;
class LmConnection;

//////////////////////////////////////////////////////////////////////
//
// Contents
//
//////////////////////////////////////////////////////////////////////
class LmLanguageManagerJava;

//////////////////////////////////////////////////////////////////////
//
// LmLanguageManagerJava
//
// The LmLanguageManagerJava is a concrete class implementing the LM
// for Java (LMJ). The LMJ interfaces to the Java Virtual Machine (JVM)
// using the Java Native Interface (JNI) API library.
//
// The JNI allows for one JVM per process, but it allows for multiple
// threads to attach to the single JVM. These semantics are provided
// for in the LMJ in that the first LMJ created allocates the JVM or
// attaches to an already existig JVM, created outside LMJ. Subsequent
// LMJ instances are only attached to the JVM (assuming they are
// different threads). Therefore, most of the LMJ's constructor
// parameters are only applicable to the first LMJ instance, and only
// if the JVM does not already exists. In a threaded application, the
// same LMJ instance created by a thread T must be used by T when it
// accesses services of the LMJ.
//
// The LMJ constructor has the following parameters:
//   result:    Constructor result, LM_OK is returned upon success.
//   maxLMJava: Max # of LMJ's per process [1]. This is required so that
//              each LMJ's Container Manager can have an equal portion
//              of the JVM's heap.
//   userOpts:  A set of JVM startup options [NULL]
//   diagsArea: Diagnostics area [NULL]
//
// NOTE: This LMJ header file intentionally does NOT include the JNI
// header file so that no other part of SQL/MX is dependent on it.
//////////////////////////////////////////////////////////////////////
class SQLLM_LIB_FUNC LmLanguageManagerJava : public LmLanguageManager {
  friend class LmRoutineJava;
  friend class LmResultSetJava;
  friend class LmRoutineJavaObj;
  friend class LmConnection;

 public:
  LmLanguageManagerJava(LmResult &result, NABoolean commandLineMode = FALSE, ComUInt32 maxLMJava = 1,
                        LmJavaOptions *userOptions = NULL, ComDiagsArea *diagsArea = NULL);

  ~LmLanguageManagerJava();

  virtual ComRoutineLanguage getLanguage() const { return COM_LANGUAGE_JAVA; }

  // This function will shut down the JVM. Currently the only value we
  // are aware of in doing this is that it flushes JVM output buffers
  // such as those holding data generated by the -verbose or
  // -Xrunhprof options.
  static void destroyVM();

  // init Java Classes
  virtual LmResult initJavaClasses();

  // LM service methods. All service methods take optional ComDiagsArea
  virtual LmResult validateRoutine(ComUInt32 numParam, ComFSDataType paramType[], ComUInt32 paramSubType[],
                                   ComColumnDirection direction[], const char *routineName, const char *containerName,
                                   const char *externalPath, char *sigBuf, ComUInt32 sigLen,
                                   ComFSDataType resultType = COM_UNKNOWN_FSDT, ComUInt32 resultSubType = 0,
                                   ComUInt32 numResultSets = 0, const char *metaContainerName = NULL,
                                   const char *optionalSig = NULL, ComDiagsArea *diagsArea = NULL);

  virtual LmResult getRoutine(ComUInt32 numParam, LmParameter parameters[], ComUInt32 numTableInfo,
                              LmTableInfo tableInfo[], LmParameter *returnValue, ComRoutineParamStyle paramStyle,
                              ComRoutineTransactionAttributes transactionAttrs, ComRoutineSQLAccess sqlAccessMode,
                              const char *parentQid, const char *clientInfo, ComUInt32 inputRowLen,
                              ComUInt32 outputRowLen, const char *sqlName, const char *externalName,
                              const char *routineSig, const char *containerName, const char *externalPath,
                              const char *librarySqlName, const char *currentUserName, const char *sessionUserName,
                              ComRoutineExternalSecurity externalSecurity, int routineOwnerId, LmRoutine **handle,
                              LmHandle getNextRowPtr, LmHandle emitRowPtr, ComUInt32 maxResultSets = 0,
                              ComDiagsArea *diagsArea = NULL);

  virtual LmResult getObjRoutine(const char *serializedInvocationInfo, int invocationInfoLen,
                                 const char *serializedPlanInfo, int planInfoLen, ComRoutineLanguage language,
                                 ComRoutineParamStyle paramStyle, const char *externalName, const char *containerName,
                                 const char *externalPath, const char *librarySqlName, LmRoutine **handle,
                                 ComDiagsArea *diagsArea);

  virtual LmResult putRoutine(LmRoutine *handle, ComDiagsArea *diagsArea = NULL);

  virtual LmResult invokeRoutine(LmRoutine *handle, void *inputRow, void *outputRow, ComDiagsArea *diagsArea = NULL);

  virtual LmResult getSystemProperty(const char *key, char *value, ComUInt32 bufferLen, ComBoolean &propertyIsSet,
                                     ComDiagsArea *diagsArea = NULL);

  virtual LmResult setSystemProperty(const char *key, const char *value, ComDiagsArea *diagsArea = NULL);

  virtual LmResult clearSystemProperty(const char *key, ComDiagsArea *diagsArea = NULL);

  // Container Manager support methods.
  virtual LmHandle createLoader(const char *externalPath, ComDiagsArea *diags);

  virtual void deleteLoader(LmHandle extLoader);

  virtual LmHandle loadContainer(const char *containerName, const char *externalPath, LmHandle extLoader,
                                 ComUInt32 *containerSize, ComDiagsArea *diagsArea);

  virtual void unloadContainer(LmHandle cont);

  virtual const char *containerExtension() { return "class"; }

  NABoolean jdbcSupportsRS() const { return jdbcSupportsRS_; }
  NABoolean jdbcRSVerMismatch() const { return jdbcSPJRSVer_ > JDBC_SPJRS_VERSION; }

  LmHandle getUdrQueueStateField() const { return udrQueueStateField_; }
  LmHandle getReturnInfoSQLStateField() const { return returnInfoSQLStateField_; }
  LmHandle getReturnInfoMessageField() const { return returnInfoMessageField_; }

 private:
  // Helper function that does all the work of the constructor. Should
  // only be called by the constructor.
  void initialize(LmResult &result, ComUInt32 maxLMJava, LmJavaOptions *userOptions, ComDiagsArea *diagsArea);

  // Runtime parameter processing methods.
  LmResult processInParameters(LmRoutineJava *handle, LmParameter params[], void *inputRow, ComDiagsArea *diagsArea);

  LmResult processOutParameters(LmRoutineJava *handle, LmParameter params[], void *outputRow, NABoolean uncaughtExp,
                                ComDiagsArea *diagsArea);

  void processParametersDone(LmRoutineJava *handle, LmParameter params[]);

  // Data conversion methods
  //
  // convertTo methods: convert a SQL value into a Java value
  //
  // convertFrom methods: convert a Java value into a SQL value.
  //
  // These methods contain no special processing for NULL values and
  // should NOT be called to convert NULL values
  //
  // The 2nd argument is a pointer to the beginning of a data row. The
  // LmParameter contains offsets and lengths for an individual value.
  //
  // NOTE: convertFrom methods take a Java object reference as their
  // 3rd argument and do NOT release the reference on that
  // object. Callers are responsible for releasing the reference at an
  // appropriate time.

  LmResult convertToString(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromString(LmParameter *, void *, LmHandle, ComDiagsArea *);

  LmResult convertToBigdec(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromBigdec(LmParameter *, void *, LmHandle, ComBoolean resultSet_decimal = FALSE,
                             ComBoolean copyBinary = FALSE, ComDiagsArea *da = NULL);

  LmResult convertToDate(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromDate(LmParameter *, void *, LmHandle, ComDiagsArea *da = NULL);

  LmResult convertToTime(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromTime(LmParameter *, void *, LmHandle, ComDiagsArea *da = NULL);

  LmResult convertToTimestamp(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromTimestamp(LmParameter *, void *, LmHandle, ComDiagsArea *da = NULL);

  LmResult convertToInteger(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromInteger(LmParameter *, void *, LmHandle);

  LmResult convertToLong(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromLong(LmParameter *, void *, LmHandle);

  LmResult convertToFloat(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromFloat(LmParameter *, void *, LmHandle, ComDiagsArea *);

  LmResult convertToDouble(LmParameter *, void *, LmHandle *, ComDiagsArea *);
  LmResult convertFromDouble(LmParameter *, void *, LmHandle, ComDiagsArea *);

  LmResult convertFromInterval(LmParameter *, void *, LmHandle, ComDiagsArea *);

  // Utility methods.
  LmHandle loadSysClass(const char *clName, const char *name, ComDiagsArea *da);

  void unloadSysClass(LmHandle obj);

  LmResult startService(ComDiagsArea *da);

  ComSInt32 threadId();

  void processJavaOptions(const LmJavaOptions &userOptions, LmJavaOptions &jvmOptions);

  LmContainerManager *contManager_;             // LMJ's CM.
  LmHandle jniEnv_;                             // JNI handle.
  static ComUInt32 maxLMJava_;                  // Max LMJs per process.
  static ComUInt32 numLMJava_;                  // Current # of LMJs.
  ComSInt32 threadId_;                          // Creating thread's ID.
  ComDiagsArea *diagsArea_;                     // Diagnostics Area passed from client
  LmJavaExceptionReporter *exceptionReporter_;  // Exception reporting
                                                // mechanism

  char *sysCatalog_;                // JDBC/MX system property "jdbcmx.catalog"/"catalog"
  char *sysSchema_;                 // JDBC/MX system property "jdbcmx.schema"/"schema"
  NABoolean setDefaultCatSchFlag_;  // Flag indicating whether to set
                                    // UDR cat&sch

  NABoolean enableType2Conn_;  // Type 2 conns are enabled
                               // They are enabled only when env variable
                               // ENABLE_TYPE2_CONN is set.

  NABoolean mapDefaultConnToType2Conn_;  // Default conns are mapped to Type 2
                                         // True if MAP_DEFAULT_CONN_2_TYPE2_CONN
                                         // is set. enableType2Conn_ will also
                                         // be set to true.

  char *userName_;  // Authentication information for JDBC Type 4 driver
  char *userPassword_;
  char *datasourceName_;  // Datasource Name for Type 4 connections

  NABoolean jdbcSupportsRS_;  // If JDBC Type 2 supports RS?
  long jdbcSPJRSVer_;        // JDBC Type2 RS version

  // The following data members are used to hold references to Java
  // classes that are used through out the life of the LMJ. Most
  // classes are used for data conversions. The approach taken is that
  // these classes are pre-loaded at LMJ construction so that
  // they do not require look-up during routine invocation. In addition
  // to the classes, the method IDs for the methods used during
  // processing are also recorded.

  LmHandle loaderClass_;  // LmClassLoader
  LmHandle loadClassId_;  // loadClass

  LmHandle utilityClass_;          // LmUtility
  LmHandle verifyMethodId_;        // verifyMethodSignature
  LmHandle createCLId_;            // createClassLoader
  LmHandle utilityInitId_;         // init method
  LmHandle classCacheSizeId_;      // static int classCacheSizeKB_
  LmHandle classCacheEnforceId_;   // static int classCacheEnforceLimit_
  LmHandle utilityInitRSId_;       // init method for T2 result sets
  LmHandle utilityGetRSInfoId_;    // getRSInfo() method  (for T2 RS)
  LmHandle utilityGetConnTypeId_;  // getConnectionType()
  LmHandle utilityInitT4RSId_;     // initT4RS(), init method for T4 RS
  LmHandle utilityGetT4RSInfoId_;  // getT4RSInfo()

  LmHandle lmObjMethodInvokeClass_;   // LmUDRObjMethodInvoke class
  LmHandle makeNewObjId_;             // make a new LmUDRObjMethodInvoke object
  LmHandle setRuntimeInfoId_;         // set run-time info in UDRInvocationInfo
  LmHandle routineMethodInvokeId_;    // invoke method of tmudr::UDR object
  LmHandle routineReturnInfoClass_;   // nested class for return info
  LmHandle returnInfoStatusField_;    // return status field id
  LmHandle returnInfoSQLStateField_;  // SQLSTATE from thrown UDRException, if any
  LmHandle returnInfoMessageField_;   // message from thrown UDRException, if any
  LmHandle returnInfoRIIField_;       // returned invocation info field id
  LmHandle returnInfoRPIField_;       // returned plan info field id
  LmHandle udrClass_;                 // org.trafodion.sql.udr.UDR class
  LmHandle udrQueueStateField_;       // UDR$QueueStateInfo.queueState_

  LmHandle lmCharsetClass_;
  LmHandle bytesToUnicodeId_;  // conversion going into JVM
  LmHandle unicodeToBytesId_;  // conversion coming out from JVM

  LmHandle stringClass_;        // java.lang.String
  LmHandle stringSubstringId_;  // substring method

  LmHandle systemClass_;        // java.lang.System
  LmHandle systemGetPropId_;    // getProperty method
  LmHandle systemSetPropId_;    // setProperty method
  LmHandle systemClearPropId_;  // clearProperty method

  LmHandle bigdecClass_;      // java.math.BigDecimal
  LmHandle bigdecCtorId_;     // Constructor
  LmHandle bigdecStrId_;      // toString
  LmHandle bigdecUnscaleId_;  // returns unscaled value of BigDecimal

  LmHandle bigintClass_;        // java.math.BigInteger
  LmHandle bigintIntValueId_;   // to int
  LmHandle bigintLongValueId_;  // to long

  LmHandle dateClass_;  // java.sql.Date
  LmHandle dateStrId_;  // toString
  LmHandle dateValId_;  // valueOf

  LmHandle timeClass_;  // java.sql.Time
  LmHandle timeStrId_;  // toString
  LmHandle timeValId_;  // valueOf

  LmHandle stampClass_;  // java.sql.Timestamp
  LmHandle stampStrId_;  // toString
  LmHandle stampValId_;  // valueOf

  LmHandle intClass_;   // java.lang.Integer
  LmHandle intCtorId_;  // Constructor
  LmHandle intValId_;   // intValue

  LmHandle longClass_;   // java.lang.Long
  LmHandle longCtorId_;  // Constructor
  LmHandle longValId_;   // longValue

  LmHandle floatClass_;   // java.lang.Float
  LmHandle floatCtorId_;  // Constructor
  LmHandle floatValId_;   // floatValue

  LmHandle doubleClass_;   // java.lang.Double
  LmHandle doubleCtorId_;  // Constructor
  LmHandle doubleValId_;   // doubleValue

  LmHandle resultSetClass_;     // java.sql.ResultSet
  LmHandle rsCloseId_;          // close() method of java.sql.ResultSet
  LmHandle rsBeforeFirstId_;    // beforefirst() method of java.sql.ResultSet
  LmHandle rsNextId_;           // next() method of java.sql.ResultSet
  LmHandle rsWasNullId_;        // wasNull() method of java.sql.ResultSet
  LmHandle rsGetWarningsId_;    // getWarnings() method of java.sql.ResultSet
  LmHandle rsGetShortId_;       // getShort() method of java.sql.ResultSet
  LmHandle rsGetIntId_;         // getInt() method of java.sql.ResultSet
  LmHandle rsGetLongId_;        // getLong() method of java.sql.ResultSet
  LmHandle rsGetFloatId_;       // getFloat() method of java.sql.ResultSet
  LmHandle rsGetDoubleId_;      // getDouble() method of java.sql.ResultSet
  LmHandle rsGetStringId_;      // getString() method of java.sql.ResultSet
  LmHandle rsGetObjectId_;      // getObject() method of java.sql.ResultSet
  LmHandle rsGetBigDecimalId_;  // getBigDecimal() method of java.sql.ResultSet
  LmHandle rsGetDateId_;        // getDate() method of java.sql.ResultSet
  LmHandle rsGetTimeId_;        // getTime() method of java.sql.ResultSet
  LmHandle rsGetTimestampId_;   // getTimestamp() method of java.sql.ResultSet

  LmHandle connCloseId_;          // close() method of java.sql.Connection
  LmHandle connIsClosedId_;       // isClosed() method of java.sql.Connection
  LmHandle hpT4ConnSuspUdrXnId_;  // suspendUdrTransaction method of com.hp.jdbc.HPT4Connection

  LmHandle jdbcMxT2Driver_;  // JDBC Type 2 Driver
  LmHandle jdbcMxT4Driver_;  // JDBC Type 4 Driver

  LmHandle lmDrvrClass_;   // com.tandem.sqlmx.LmSQLMXDriver
  LmHandle driverInitId_;  // init() method
  LmHandle connClass_;     // Java java.sql.Connection class
};

#endif
