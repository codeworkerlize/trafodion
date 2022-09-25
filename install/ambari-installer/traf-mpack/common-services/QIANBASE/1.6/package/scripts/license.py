#!/usr/bin/env python

import sys, os, pwd, signal, time
from resource_management import *
from tempfile import TemporaryFile

class License(object):
    def __init__(self,lpath):
        self.lfile = lpath
        self.decoder = self.cmd_output('source ~/.bashrc ; echo $TRAF_HOME/export/bin${SQ_MBTYPE}/decoder')
        if not os.path.exists(self.decoder):
            print 'Error: Missing decoder program'

    def cmd_output(self, cmd):
      import params
      scmd = cmd + '; exit 0' # make sure we ignore return code in Execute
      ofile = TemporaryFile()
      Execute(scmd,user=params.traf_user,stdout=ofile,logoutput=True)
      ofile.seek(0) # read from beginning
      outstr = ofile.read().rstrip()
      ofile.close()
      return outstr

    def gen_id(self):
        return self.cmd_output('%s -g' % (self.decoder))

    def lic_cmd(self, opt):
        return self.cmd_output('%s -%s -f %s' % (self.decoder, opt, self.lfile))

    def type(self):
        return self.lic_cmd('t')

    def vers(self):
        return int(self.lic_cmd('v'))

    def nodes(self):
        return int(self.lic_cmd('n'))

    def expired(self):
        result = self.cmd_output('%s -z -f %s' % (self.decoder, self.lfile))
        print result
        return ("EXPIRED" in result)

    def idmatch(self,idfile):
        result = self.cmd_output('%s -i -f %s -u %s' % (self.decoder, self.lfile, idfile))
        print result
        return ("matches" in result)

    def mt_enabled(self):
        return (int(self.lic_cmd('x')) & 4) != 0


def encode_pwd(s):
    def _to36(value):
        if value == 0:
            return '0'
        if value < 0:
            sign = '-'
            value = -value
        else:
            sign = ''
        result = []
        while value:
            value, mod = divmod(value, 36)
            result.append('0123456789abcdefghijklmnopqrstuvwxyz'[mod])
        return sign + ''.join(reversed(result))

    OBF_PREFIX = 'OBF:'
    o = OBF_PREFIX
    if isinstance(s, bytes):
        s = s.decode('utf-8')
    b = bytearray(s, 'utf-8')
    l = len(b)
    for i in range(0, l):
        b1, b2 = b[i], b[l - (i + 1)]
        if b1 < 0 or b2 < 0:
            i0 = (0xff & b1) * 256 + (0xff & b2)
            o += 'U0000'[0:5 - len(x)] + x
        else:
            i1, i2 = 127 + b1 + b2, 127 + b1 - b2
            i0 = i1 * 256 + i2
            x = _to36(i0)
            j0 = int(x, 36)
            j1, j2 = i0 / 256, i0 % 256
            o += '000'[0:4 - len(x)] + x
    return o
