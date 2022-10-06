//------------------------------------------------------------------
//

//
// Message-system limits module
//
#ifndef __SB_MSLIMITS_H_
#define __SB_MSLIMITS_H_

// Note the MAX's and MS_Process_Info_Type are near clones from msgdef.h
enum { MS_MON_MAX_KEY_LIST = 32 };
enum { MS_MON_MAX_KEY_NAME = 64 };
enum { MS_MON_MAX_NODE_LIST = 64 };
enum { MS_MON_MAX_OPEN_LIST = 256 };
enum { MS_MON_MAX_PORT_NAME = 256 };
enum { MS_MON_MAX_PROC_LIST = 256 };
enum { MS_MON_MAX_PROCESS_NAME = 64 };
enum { MS_MON_MAX_PROCESS_PATH = 256 };
enum { MS_MON_MAX_PROCESSOR_NAME = 128 };
enum { MS_MON_MAX_REASON_TEXT = 256 };
enum { MS_MON_MAX_SYNC_DATA = 4096 };
enum { MS_MON_MAX_STFSD_DATA = 32767 };
enum { MS_MON_MAX_VALUE_SIZE = 512 };
enum { MS_MON_MAX_TM_SYNCS = 200 };

#endif  // !__SB_MSLIMITS_H_
