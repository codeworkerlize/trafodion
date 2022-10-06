//------------------------------------------------------------------
//

//
// Label-maps module
//
#ifndef __SB_LABELMAPS_H_
#define __SB_LABELMAPS_H_

#include "int/exp.h"

//
// Get label for errno
//
SB_Export const char *SB_get_label_errno(int value);

//
// Get label for MS_Mon_ConfigType (MS_Mon_ConfigType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_configtype(int value);

//
// Get (short) label for MS_Mon_ConfigType (MS_Mon_ConfigType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_configtype_short(int value);

//
// Get label for MS_MON_DEVICE_STATE (MS_Mon_State_<value>)
//
SB_Export const char *SB_get_label_ms_mon_device_state(int value);

//
// Get (short) label for MS_MON_DEVICE_STATE (MS_Mon_State_<value>)
//
SB_Export const char *SB_get_label_ms_mon_device_state_short(int value);

//
// Get label for MS_Mon_MSGTYPE (MS_MsgType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_msgtype(int value);

//
// Get (short) label for MS_Mon_MSGTYPE (MS_MsgType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_msgtype_short(int value);

//
// Get label for MS_MON_PROC_STATE (MS_Mon_State_<value)
//
SB_Export const char *SB_get_label_ms_mon_proc_state(int value);

//
// Get (short) label for MS_MON_PROC_STATE (MS_Mon_State_<value)
//
SB_Export const char *SB_get_label_ms_mon_proc_state_short(int value);

//
// Get label for MS_Mon_PROCESSTYPE (MS_ProcessType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_processtype(int value);

//
// Get (short) label for MS_Mon_PROCESSTYPE (MS_ProcessType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_processtype_short(int value);

//
// Get label for MS_Mon_REQTYPE (MS_ReqType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_reqtype(int value);

//
// Get (short) label for MS_Mon_REQTYPE (MS_ReqType_<value>)
//
SB_Export const char *SB_get_label_ms_mon_reqtype_short(int value);

//
// Get label for MS_MON_ShutdownLevel (MS_Mon_ShutdownLevel_<value>)
//
SB_Export const char *SB_get_label_ms_mon_shutdownlevel(int value);

//
// Get (short) label for MS_MON_ShutdownLevel (MS_Mon_ShutdownLevel_<value>)
//
SB_Export const char *SB_get_label_ms_mon_shutdownlevel_short(int value);

#endif  // !__SB_LABELMAPS_H_
