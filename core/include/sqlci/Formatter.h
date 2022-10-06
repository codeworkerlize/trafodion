#ifndef FORMATTER_H
#define FORMATTER_H

#include "common/BaseTypes.h"
#include "sqlci/SqlciEnv.h"

class Formatter {
 public:
  enum BufferIt { BLANK_SEP_WIDTH = 2 };
  enum ShowNonprinting { HEX_EXPANSION_ON = 9, HEX_BUFSIZ_MULTIPLIER = 3 };

  static int buffer_it(SqlciEnv *sqlci_env, char *data, int datatype, int length, int precision, int scale,
                       char *ind_data,
                       // display length is printed len in single-wide chars
                       int display_length,
                       // display buffer len may be longer for UTF-8
                       int display_buf_length, int null_flag, int vcIndLen, char *buf, int *curpos,
                       NABoolean separatorNeeded = FALSE, NABoolean checkShowNonPrinting = FALSE);

  static int display_length(int datatype, int length, int precision, int scale, int charsetEnum, int heading_len,
                            SqlciEnv *sqlci_env, int *output_buflen);

  static char getShowNonprintingReplacementChar(NABoolean reeval = FALSE);

  static size_t showNonprinting(char *s, size_t z, NABoolean varchar);

#define showNonprintingCHAR(f) showNonprinting(f, sizeof(f), FALSE)

#define showNonprintingVARCHAR(v, z) showNonprinting(v, size_t(z) + 1, TRUE)

#define showNonprintingCSTRING(c) showNonprinting(c, strlen(c) + 1, TRUE)

  static NABoolean replace8bit_;

 private:
};

#endif
