#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

import re
import sys
import subprocess


def cmd_output(cmd):
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
    stdout, stderr = p.communicate()
    if stdout:
        return stdout
    else:
        return stderr


def err(msg):
    sys.stderr.write('\n\33[31m***[ERROR]: %s \33[0m\n' % msg)
    sys.exit(1)


def info(msg):
    print '\n\33[33m***[INFO]: %s \33[0m' % msg


def run():
    info('Fetching active queries list ...')
    active_queries_output = cmd_output('offender -s active |grep -E "^[0-9]+-[0-9]+"')
    outputs = active_queries_output.split('\n')
    if len(outputs) == 1:
        info('No active queries found at this moment ...')
        exit(0)

    try:
        results = [re.search(r'(\d+-\d+-\d+ \d+:\d+:\d+\.\d+)\s+(-?\d+)\s+(\w+)\s+(\w+)\s+"(.*)', output).groups() for output in outputs if output]
    except AttributeError:
        err('Failed to get active queries list ...')

    print 'NO.\tTIMESTAMP\t\t\t\t\tLAST_ACTIVITY_SECS\tEXECUTE_STATE\tSOURCE_TEXT'
    print '---\t------------\t\t\t\t------------------\t-------------\t--------------'

    for index, result in enumerate(results):
        print('%d\t%s\t\t%10s\t\t%12s\t%s' % (index+1, result[0], result[1], result[3], result[4][:63]))

    try:
        num = raw_input('Choose query No. for detail query explain: ')
    except KeyboardInterrupt:
        info('User quit ...')
        exit(0)

    if not num.isdigit():
        err('Invalid input, should be a number')

    num = int(num)
    if num > len(results) or num <= 0:
        err('Invalid number')

    qid = results[num-1][2]
    source_text = results[num-1][4][:63]

    info('Fetching query explain for query No.[%d], source text is: %s ...' % (num, source_text))
    explain_result = cmd_output("echo \"explain options 'f' for qid %s;\" | sqlci | grep -A 100 '>>'|grep -vE '(^$|SQL|>>)'" % qid)
    print explain_result


# main
if __name__ == '__main__':
    run()
