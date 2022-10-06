
#ifndef EXP_DATETIME_H
#define EXP_DATETIME_H

#include "common/Int64.h"
#include "common/Platform.h"
#include "common/sqtypes.h"
#include "exp/ExpError.h"
#include "exp/exp_attrs.h"

UInt32 Date2Julian(int y, int m, int d);

class ExpDatetime : public SimpleType {
 public:
  // these enums must be in the same order as the datetimeFormat[] array
  // (defined in exp_datetime.cpp).
  enum DatetimeFormats {
    DATETIME_FORMAT_MIN = 0,
    DATETIME_FORMAT_MIN_DATE = DATETIME_FORMAT_MIN,
    DATETIME_FORMAT_DEFAULT = DATETIME_FORMAT_MIN_DATE,  // YYYY-MM-DD
    DATETIME_FORMAT_USA,                                 // MM/DD/YYYY AM|PM
    DATETIME_FORMAT_EUROPEAN,                            // DD.MM.YYYY
    DATETIME_FORMAT_DEFAULT2,                            // YYYY-MM
    DATETIME_FORMAT_USA2,                                // MM/DD/YYYY
    DATETIME_FORMAT_USA3,                                // YYYY/MM/DD
    DATETIME_FORMAT_USA4,                                // YYYYMMDD
    DATETIME_FORMAT_USA5,                                // YY/MM/DD
    DATETIME_FORMAT_USA6,                                // MM/DD/YY
    DATETIME_FORMAT_USA7,                                // MM-DD-YYYY
    DATETIME_FORMAT_USA8,                                // YYYYMM
    DATETIME_FORMAT_EUROPEAN2,                           // DD-MM-YYYY
    DATETIME_FORMAT_EUROPEAN3,                           // DD-MON-YYYY
    DATETIME_FORMAT_EUROPEAN4,                           // DDMONYYYY
    DATETIME_FORMAT_EUROPEAN5,                           // DD/MM/YYYY
    DATETIME_FORMAT_MAX_DATE = DATETIME_FORMAT_EUROPEAN5,

    DATETIME_FORMAT_MIN_TIME,
    DATETIME_FORMAT_TS4 = DATETIME_FORMAT_MIN_TIME,  // HH24:MI:SS
    DATETIME_FORMAT_TS13,                            // HH24:MI:SS.FF
    DATETIME_FORMAT_TS26,                            // HH24MISS
    DATETIME_FORMAT_TS27,                            // HHMISS
    DATETIME_FORMAT_TS28,                            // HH24MISSFF
    DATETIME_FORMAT_TS29,                            // HHMISSFF
    DATETIME_FORMAT_MAX_TIME = DATETIME_FORMAT_TS29,

    DATETIME_FORMAT_MIN_TS,
    DATETIME_FORMAT_TS1 = DATETIME_FORMAT_MIN_TS,  // YYYYMMDDHH24MISS
    DATETIME_FORMAT_TS2,                           // DD.MM.YYYY:HH24.MI.SS
    DATETIME_FORMAT_TS3,                           // YYYY-MM-DD HH24:MI:SS
    DATETIME_FORMAT_TS5,                           // YYYYMMDD:HH24:MI:SS
    DATETIME_FORMAT_TS6,                           // MMDDYYYY HH24:MI:SS
    DATETIME_FORMAT_TS7,                           // MM/DD/YYYY HH24:MI:SS
    DATETIME_FORMAT_TS8,                           // DD-MON-YYYY HH:MI:SS
    DATETIME_FORMAT_TS9,                           // MONTH DD, YYYY, HH:MI AM|PM
    DATETIME_FORMAT_TS10,                          // DD.MM.YYYY HH24.MI.SS
    DATETIME_FORMAT_TS11,                          // YYYY/MM/DD HH24:MI:SS
    DATETIME_FORMAT_TS12,                          // YYYY-MM-DD:HH24:MI:SS
    DATETIME_FORMAT_MIN_NANO,
    DATETIME_FORMAT_TS14 = DATETIME_FORMAT_MIN_NANO,  // YYYYMMDDHH24MISSFF
    DATETIME_FORMAT_TS15,                             // DD.MM.YYYY:HH24.MI.SS.FF
    DATETIME_FORMAT_TS16,                             // YYYY-MM-DD HH24:MI:SS.FF
    DATETIME_FORMAT_TS17,                             // YYYYMMDD:HH24:MI:SS.FF
    DATETIME_FORMAT_TS18,                             // MMDDYYYY HH24:MI:SS.FF
    DATETIME_FORMAT_TS19,                             // MM/DD/YYYY HH24:MI:SS.FF
    DATETIME_FORMAT_TS20,                             // DD-MON-YYYY HH:MI:SS.FF
    DATETIME_FORMAT_TS21,                             // DD.MM.YYYY HH24.MI.SS.FF
    DATETIME_FORMAT_TS22,                             // YYYY/MM/DD HH24:MI:SS.FF
    DATETIME_FORMAT_TS23,                             // YYYY-MM-DD:HH24:MI:SS.FF
    DATETIME_FORMAT_TS24,                             // DD-MM-YYYY HH24:MI:SS.FF
    DATETIME_FORMAT_TS30,                             // YYYYMMDD HH24:MI:SS.FF
    DATETIME_FORMAT_MAX_NANO = DATETIME_FORMAT_TS30,
    DATETIME_FORMAT_TS25,  // DD-MM-YYYY HH24:MI:SS
    DATETIME_FORMAT_TS31,  // YYYYMMDD HH24:MI:SS
    DATETIME_FORMAT_MAX_TS = DATETIME_FORMAT_TS31,

    DATETIME_FORMAT_MAX = DATETIME_FORMAT_MAX_TS,

    DATETIME_FORMAT_MIN_NUM = DATETIME_FORMAT_MAX,
    DATETIME_FORMAT_NUM1,  // 99:99:99:99
    DATETIME_FORMAT_NUM2,  // -99:99:99:99
    DATETIME_FORMAT_MAX_NUM = DATETIME_FORMAT_NUM2,

    DATETIME_FORMAT_EXTRA_MIN = DATETIME_FORMAT_MAX_NUM,
    DATETIME_FORMAT_EXTRA_HH,    // hour of day(00-23)
    DATETIME_FORMAT_EXTRA_HH12,  // hour of day(01-12)
    DATETIME_FORMAT_EXTRA_HH24,  // hour of day(00-23)
    DATETIME_FORMAT_EXTRA_MI,    // minute(00-59)
    DATETIME_FORMAT_EXTRA_SS,    // second(00-59)
    DATETIME_FORMAT_EXTRA_YYYY,  // year(4 digits)
    DATETIME_FORMAT_EXTRA_YYY,   // year(last 3 digits of year)
    DATETIME_FORMAT_EXTRA_YY,    // year(last 2 digits of year)
    DATETIME_FORMAT_EXTRA_Y,     // year(last digit of year)
    DATETIME_FORMAT_EXTRA_MON,   // month(3 chars in English)
    DATETIME_FORMAT_EXTRA_MM,    // month(01-12)
    DATETIME_FORMAT_EXTRA_DY,    // name of day(3 chars in English) exp. SUN
    DATETIME_FORMAT_EXTRA_DAY,   // name of day,padded with blanks to length of 9 characters. exp. SUNDAY
    DATETIME_FORMAT_EXTRA_CC,    // century
    DATETIME_FORMAT_EXTRA_D,     // day of week(Sunday(1) to Saturday(7))
    DATETIME_FORMAT_EXTRA_DD,    // day of month(01-31)
    DATETIME_FORMAT_EXTRA_DDD,   // day of year(1-366)
    DATETIME_FORMAT_EXTRA_W,     // week of month(1-5)
    DATETIME_FORMAT_EXTRA_WW,    // week number of year(1-53)
    DATETIME_FORMAT_EXTRA_J,     // number of days since January 1, 4713 BC
    DATETIME_FORMAT_EXTRA_Q,     // the quarter of year(1-4)
    DATETIME_FORMAT_EXTRA_FF,  // Fractional seconds,Use the numbers 1 to 9 after FF to specify the number of digits in
                               // the fractional second portion of the datetime value returned.
    DATETIME_FORMAT_EXTRA_MAX = DATETIME_FORMAT_EXTRA_FF,
    // the following are intended for binder time resolution based
    // on operand type to one of the formats above
    DATETIME_FORMAT_MIN_UNRESOLVED = DATETIME_FORMAT_EXTRA_MAX,
    DATETIME_FORMAT_UNSPECIFIED,  // Default format for TO_CHAR; resolved at bind time
                                  // based on the datatype of the operand
    DATETIME_FORMAT_MAX_UNRESOLVED = DATETIME_FORMAT_UNSPECIFIED,

    DATETIME_FORMAT_DATE_STR,  // format in str
    DATETIME_FORMAT_TIME_STR,  // format in str
    DATETIME_FORMAT_NONE,
    DATETIME_FORMAT_NULL,  // support null
    DATETIME_FORMAT_ERROR = -1
  };

  struct DatetimeFormatInfo {
    int format;       // defined by enum DatetimeFormats
    const char *str;  // string representation of datetime format
    int minLen;       // minimum length to hold this format
    int maxLen;
  };

  static const DatetimeFormatInfo datetimeFormat[];
  static const DatetimeFormatInfo altDatetimeFormat[];
  static const short altDatetimeFormatArraySize = 9;  // Update this count when you add a new alt datetime format

  enum { DATETIME_MAX_NUM_FIELDS = 7 };
  enum { MAX_DATETIME_SIZE = 11 };

  enum { MAX_DATETIME_MICROS_FRACT_PREC = 6 };
  enum { MAX_DATETIME_NANOS_FRACT_PREC = 9 };
  enum { MAX_DATETIME_FRACT_PREC = 9 };

  // MAX Length of Datetime string is 50 -
  // "DATE 'YYYY-MM-DD';"
  // "TIME 'HH:MM:SS';"
  // "TIMESTAMP 'YYYY-MM-DD:HH:MM:SS.MSSSSS';"
  // "DATETIME 'YYYY-MM-DD:HH:MM' YEAR TO MINUTE;"
  // "DATETIME 'MM-DD:HH:MM:SS.MSSSSS' MONTH TO SECOND;"
  // 012345678901234567890123456789012345678901234567890
  //
  enum { MAX_DATETIME_STRING_LEN = 50 };

  enum arithOps { DATETIME_ADD, DATETIME_SUB };

  ExpDatetime();
  ~ExpDatetime();

  static long getTotalDays(short year, short month, short day);

  static short getDatetimeFields(int datetimeCode, rec_datetime_field &startField, rec_datetime_field &endField);

  short computeLastDayOfMonth(rec_datetime_field startField, rec_datetime_field endField, char *inDatetimeOpData,
                              char *outDatetimeOpData);

  static NABoolean fractionStoredAsNanos(rec_datetime_field endField, short fractionPrecision);

  void convertDatetimeToInterval(rec_datetime_field datetimeStartField, rec_datetime_field datetimeEndField,
                                 short fractionPrecision, rec_datetime_field intervalEndField, char *datetimeOpData,
                                 long &interval, char *intervalBignum, NABoolean &isBignum) const;

  static short getYearMonthDay(long totalDays, short &year, char &month, char &day);

  short convertIntervalToDatetime(long interval, char *intervalBignum, rec_datetime_field startField,
                                  rec_datetime_field endField, short fractionPrecision, char *datetimeOpData) const;

  static short validateDate(rec_datetime_field startField, rec_datetime_field endField, char *datetimeOpData,
                            ExpDatetime *attr, short intervalFlag, NABoolean &LastDayPrevMonth);

  static short validateTime(const char *datetimeOpData);

  short compDatetimes(char *datetimeOpData1, char *datetimeOpData2);

  static short compDatetimes(const char *datetimeOpData1, const char *datetimeOpData2, rec_datetime_field startField,
                             rec_datetime_field endField, NABoolean hasFractionalPart);

  short arithDatetimeInterval(ExpDatetime::arithOps operation, ExpDatetime *datetimeOpType, Attributes *intervalOpType,
                              char *datetimeOpData, char *intervalOpData, char *resultData, CollHeap *heap,
                              ComDiagsArea **diagsArea);

  short subDatetimeDatetime(Attributes *datetimeOpType, Attributes *intervalOpType, char *datetimeOpData1,
                            char *datetimeOpData2, char *resultData, CollHeap *heap, ComDiagsArea **diagsArea) const;

  static short getDisplaySize(int datetimeCode, short fractionPrecision);

  static int getDatetimeFormatLen(int format, NABoolean to_date, rec_datetime_field startField,
                                  rec_datetime_field endField);

  Attributes *newCopy();

  Attributes *newCopy(CollHeap *);

  void copyAttrs(Attributes *source_);  // copy source attrs to this.

  // ---------------------------------------------------------------------
  // Perform type-safe pointer casts.
  // ---------------------------------------------------------------------
  virtual ExpDatetime *castToExpDatetime();

  short convDatetimeDatetime(char *srcData, rec_datetime_field dstStartField, rec_datetime_field dstEndField,
                             short dstFractPrec, char *dstData, int dstLen, short validateFlag,
                             NABoolean *roundedDownFlag = NULL);

  static short currentTimeStamp(char *dstData, rec_datetime_field startField, rec_datetime_field endField,
                                short fractPrec);

  short extractDatetime(rec_datetime_field srcStartField, rec_datetime_field srcEndField, short srcFractPrec,
                        char *srcData, char *dstData);

  static short convAsciiToDatetime(char *source, int sourceLen, char *target, int targetLen,
                                   rec_datetime_field dstStartField, rec_datetime_field dstEndField, int format,
                                   int &scale, CollHeap *heap, ComDiagsArea **diagsArea, int flags);

  short convAsciiToDatetime(char *source, int sourceLen, char *target, int targetLen, int format, CollHeap *heap,
                            ComDiagsArea **diagsArea, int flags);

  short convAsciiToDate(char *target, int targetLen, char *source, int sourceLen, int format, CollHeap *heap,
                        ComDiagsArea **diagsArea, int flags);

  int convDatetimeToASCII(char *srcData, char *dstData, int dstLen, int format, char *formatStr, CollHeap *heap,
                          ComDiagsArea **diagsArea, int caseSensitive = 0, int nsrcLen = -1);

  static int convNumericTimeToASCII(char *srcData, char *dstData, int dstLen, int format, char *formatStr,
                                    CollHeap *heap, ComDiagsArea **diagsArea);

  static short convAsciiDatetimeToASCII(char *srcData, int srcPrecision, int srcScale, int srcLen, char *dstData,
                                        int dstLen, int format, CollHeap *heap, ComDiagsArea **diagsArea);

  static short convAsciiDatetimeToUtcOrLocal(char *srcData, int srcLen, char *dstData, int dstLen, long gmtDiff,
                                             NABoolean toUTC, CollHeap *heap, ComDiagsArea **diagsArea);

  char *getDefaultStringValue(CollHeap *heap);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    SimpleType::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

  static const char *getDatetimeFormatStr(int frmt) { return datetimeFormat[frmt].str; }

  static const int getDatetimeFormat(const char *formatStr) {
    if (0 == strlen(formatStr)) return DATETIME_FORMAT_NULL;
    for (int i = DATETIME_FORMAT_MIN; i <= DATETIME_FORMAT_MAX; i++) {
      if (stricmp(formatStr, datetimeFormat[i].str) == 0) {
        if (datetimeFormat[i].format != i) return -1;

        return i;
      }
    }

    for (int i = DATETIME_FORMAT_MIN_NUM; i <= DATETIME_FORMAT_MAX_NUM; i++) {
      if (stricmp(formatStr, datetimeFormat[i].str) == 0) {
        if (datetimeFormat[i].format != i) return -1;

        return i;
      }
    }

    for (int i = DATETIME_FORMAT_EXTRA_MIN; i <= DATETIME_FORMAT_EXTRA_MAX; i++) {
      if (stricmp(formatStr, datetimeFormat[i].str) == 0) {
        if (datetimeFormat[i].format != i) return -1;

        return i;
      }
    }

    for (int i = DATETIME_FORMAT_MIN_UNRESOLVED; i <= DATETIME_FORMAT_MAX_UNRESOLVED; i++) {
      if (stricmp(formatStr, datetimeFormat[i].str) == 0) {
        if (datetimeFormat[i].format != i) return -1;

        return i;
      }
    }

    // check if alternate datetime format is specified
    int f = getAltDatetimeFormat(formatStr);
    if (f >= 0) return f;

    return -1;
  }

  // static const int getAltDatetimeFormat(const char * formatStr);
  static const int getAltDatetimeFormat(const char *formatStr) {
    for (int i = 0; i < altDatetimeFormatArraySize; i++) {
      if (stricmp(formatStr, altDatetimeFormat[i].str) == 0) {
        return altDatetimeFormat[i].format;
      }
    }

    return -1;
  }

  static NABoolean isDateTimeFormat(int frmt) {
    return ((frmt >= DATETIME_FORMAT_MIN) && (frmt <= DATETIME_FORMAT_MAX));
  }

  static NABoolean isDateFormat(int frmt) {
    return ((frmt >= DATETIME_FORMAT_MIN_DATE) && (frmt <= DATETIME_FORMAT_MAX_DATE));
  }

  static NABoolean isTimestampFormat(int frmt) {
    return ((frmt >= DATETIME_FORMAT_MIN_TS) && (frmt <= DATETIME_FORMAT_MAX_TS));
  }

  static NABoolean isTimeFormat(int frmt) {
    return ((frmt >= DATETIME_FORMAT_MIN_TIME) && (frmt <= DATETIME_FORMAT_MAX_TIME));
  }

  static NABoolean isExtraFormat(int frmt) {
    return ((frmt >= DATETIME_FORMAT_EXTRA_MIN) && (frmt <= DATETIME_FORMAT_EXTRA_MAX));
  }

  static NABoolean isNumericFormat(int frmt) {
    return ((frmt == DATETIME_FORMAT_NUM1) || (frmt == DATETIME_FORMAT_NUM2));
  }

  static int getDatetimeFormatLen(int frmt) { return datetimeFormat[frmt].minLen; }

  static int getDatetimeFormatMaxLen(int frmt) { return datetimeFormat[frmt].maxLen; }

 private:
};

#endif
