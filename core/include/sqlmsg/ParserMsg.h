
#ifndef PARSERMSG_H
#define PARSERMSG_H

/* -*-C++-*-
 *****************************************************************************
 * File:         ParserMsg.h
 * RCS:          $Id: ParserMsg.h,v 1.2 1998/08/10 15:36:19  Exp $
 *****************************************************************************
 */

#include "common/NAWinNT.h"

#include "common/charinfo.h"

class ComDiagsArea;

// Single-byte version
void StoreSyntaxError(const char *input_str, int input_pos, ComDiagsArea &diags, int dgStrNum = 0,
                      /*  input_str_cs: the charset of input_str */
                      CharInfo::CharSet input_str_cs = CharInfo::ISO88591,
                      /*  terminal_cs : the charset of terminal */
                      CharInfo::CharSet terminal_cs = CharInfo::ISO88591);

// Unicode version
void StoreSyntaxError(const NAWchar *input_str, int input_pos, ComDiagsArea &diags, int dgStrNum = 0,
                      /*  terminal_cs : the charset of terminal */
                      CharInfo::CharSet terminal_cs = CharInfo::ISO88591);

#endif /* PARSERMSG_H */
