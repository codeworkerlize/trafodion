//------------------------------------------------------------------
//

//
// File-system error module
//
#ifndef __SB_FSERR_H_
#define __SB_FSERR_H_

#if 0
enum { XZBASE               = 1000         }; // for testing
#else
enum { XZBASE = 0 };
#endif
enum { XZFIL_ERR_OK = XZBASE + 0 };
enum { XZFIL_ERR_INVALOP = XZBASE + 2 };
enum { XZFIL_ERR_SYSMESS = XZBASE + 6 };
enum { XZFIL_ERR_BADERR = XZBASE + 10 };
enum { XZFIL_ERR_NOTFOUND = XZBASE + 11 };
enum { XZFIL_ERR_BADNAME = XZBASE + 13 };
enum { XZFIL_ERR_NOSUCHDEV = XZBASE + 14 };
enum { XZFIL_ERR_NOTOPEN = XZBASE + 16 };
enum { XZFIL_ERR_BADCOUNT = XZBASE + 21 };
enum { XZFIL_ERR_BOUNDSERR = XZBASE + 22 };
enum { XZFIL_ERR_WAITFILE = XZBASE + 25 };
enum { XZFIL_ERR_NONEOUT = XZBASE + 26 };
enum { XZFIL_ERR_TOOMANY = XZBASE + 28 };
enum { XZFIL_ERR_NOLCB = XZBASE + 30 };
enum { XZFIL_ERR_NOBUFSPACE = XZBASE + 31 };
enum { XZFIL_ERR_TIMEDOUT = XZBASE + 40 };
enum { XZFIL_ERR_FSERR = XZBASE + 53 };  // internal prob w/ fs or i/o
enum { XZFIL_ERR_WRONGID = XZBASE + 60 };
enum { XZFIL_ERR_DEVDOWN = XZBASE + 66 };
enum { XZFIL_ERR_BADREPLY = XZBASE + 74 };
enum { XZFIL_ERR_OVERRUN = XZBASE + 121 };
enum { XZFIL_ERR_INVALIDSTATE = XZBASE + 160 };  // not inited
enum { XZFIL_ERR_DEVERR = XZBASE + 190 };
enum { XZFIL_ERR_PATHDOWN = XZBASE + 201 };
enum { XZFIL_ERR_BUFTOOSMALL = XZBASE + 563 };
enum { XZFIL_ERR_BADPARMVALUE = XZBASE + 590 };
enum { XZFIL_ERR_REQABANDONED = XZBASE + 593 };

#endif  // !__SB_FSERR_H_
