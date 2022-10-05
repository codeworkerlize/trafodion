

//-------------------------------------------------------------------
// For seamonster we register an exit handler to do the following:
//
//    Do nothing if the function was already called
//
//    Do nothing if SM was not initialized
//
//    If this is not the reader thread
//       Send the reader thread a SHUTDOWN message
//       Wait for reader thread to react
//
//    SM_cancel ID used internally by executor code
//
//    SM_finalize
//
// An older version of this file described a requirement which no
// longer exists: that no thread should call SM_finalize if another
// thread is inside a seamonster API call. This requirement has gone
// away.
//
// In addition, the old code relied on state variables in the main
// thread and reader thread to track SM initialization calls being
// made (an older API had several initialization calls such as
// SM_reader_connect). Now the goal is much simpler: if a process has
// called SM_init successfully, try to call SM_finalize before the
// process exits.
//-------------------------------------------------------------------

void ExSM_ExitHandler(void);
