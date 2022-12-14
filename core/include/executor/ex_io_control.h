
#ifndef EX_IO_CONTROL_H
#define EX_IO_CONTROL_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "executor/ex_queue.h"

class ControlInfo {
 public:
  ControlInfo() : ciFlags_(HAS_NOTHING){};
  ~ControlInfo(){};

  inline up_state &getUpState();
  inline down_state &getDownState();

  inline int getBufferSequenceNumber();

  inline void setBufferSequenceNumber(int snum);
  inline NABoolean getIsDiagsAreaPresent() const;
  inline void setIsDiagsAreaPresent(NABoolean);
  inline NABoolean getIsDiagsAreaUnpacked() const;
  inline void setIsDiagsAreaUnpacked(NABoolean);
  // an external diags area is one that gets sent outside of the sql_buffer
  inline NABoolean getIsExtDiagsAreaPresent() const;
  inline void setIsExtDiagsAreaPresent(NABoolean);
  inline NABoolean getIsDataRowPresent() const;
  inline void setIsDataRowPresent(NABoolean);
  inline NABoolean getIsStatsAreaPresent() const;
  inline void setIsStatsAreaPresent(NABoolean);
  // an external stats area is one that gets sent outside of the sql_buffer
  inline NABoolean getIsExtStatsAreaPresent() const;
  inline void setIsExtStatsAreaPresent(NABoolean);
  inline void resetFlags();
  inline void copyFlags(const ControlInfo &other);

 private:
  union  // anonymous
  {
    up_state upState_;
    down_state downState_;
  };
  enum ciFlagsBits {
    HAS_NOTHING = 0x0,
    HAS_DATA = 0x1,
    HAS_DIAGSAREA = 0x02,
    HAS_STATSAREA = 0x04,
    HAS_EXT_DIAGSAREA = 0x08,
    HAS_EXT_STATSAREA = 0x10,
    IS_DIAGSAREA_UNPACKED = 0x20
  };
  int ciFlags_;
};

//
// In-line methods.
//

inline up_state &ControlInfo::getUpState() { return upState_; };

inline down_state &ControlInfo::getDownState() { return downState_; };

inline int ControlInfo::getBufferSequenceNumber() { return downState_.requestValue; }

inline void ControlInfo::setBufferSequenceNumber(int snum) { downState_.requestValue = (int)snum; }

inline NABoolean ControlInfo::getIsDiagsAreaPresent() const { return (ciFlags_ & HAS_DIAGSAREA); }

inline void ControlInfo::setIsDiagsAreaPresent(NABoolean val) {
  if (val)
    ciFlags_ |= HAS_DIAGSAREA;
  else
    ciFlags_ &= ~HAS_DIAGSAREA;
}

inline NABoolean ControlInfo::getIsDiagsAreaUnpacked() const { return ((ciFlags_ & IS_DIAGSAREA_UNPACKED) != 0); }

inline void ControlInfo::setIsDiagsAreaUnpacked(NABoolean val) {
  if (val)
    ciFlags_ |= IS_DIAGSAREA_UNPACKED;
  else
    ciFlags_ &= ~IS_DIAGSAREA_UNPACKED;
}

inline NABoolean ControlInfo::getIsExtDiagsAreaPresent() const { return (ciFlags_ & HAS_EXT_DIAGSAREA); }

inline void ControlInfo::setIsExtDiagsAreaPresent(NABoolean val) {
  if (val)
    ciFlags_ |= HAS_EXT_DIAGSAREA;
  else
    ciFlags_ &= ~HAS_EXT_DIAGSAREA;
}

inline NABoolean ControlInfo::getIsDataRowPresent() const { return (ciFlags_ & HAS_DATA); }

inline void ControlInfo::setIsDataRowPresent(NABoolean val) {
  if (val)
    ciFlags_ |= HAS_DATA;
  else
    ciFlags_ &= ~HAS_DATA;
}

inline NABoolean ControlInfo::getIsStatsAreaPresent() const { return (ciFlags_ & HAS_STATSAREA); }

inline void ControlInfo::setIsStatsAreaPresent(NABoolean val) {
  if (val)
    ciFlags_ |= HAS_STATSAREA;
  else
    ciFlags_ &= ~HAS_STATSAREA;
}

inline NABoolean ControlInfo::getIsExtStatsAreaPresent() const { return (ciFlags_ & HAS_EXT_STATSAREA); }

inline void ControlInfo::setIsExtStatsAreaPresent(NABoolean val) {
  if (val)
    ciFlags_ |= HAS_EXT_STATSAREA;
  else
    ciFlags_ &= ~HAS_EXT_STATSAREA;
}

inline void ControlInfo::resetFlags() { ciFlags_ = 0; }

inline void ControlInfo::copyFlags(const ControlInfo &other) { ciFlags_ = other.ciFlags_; }

#endif
