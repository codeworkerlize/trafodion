#!/usr/bin/env python
import re
import time
from scripts.reporter import format_time, Reporter
from scripts.common import format_output, err_m, ParseInI


def run():
    format_output('Trafodion Reporter ToolKit')

    time_now_sec = time.time()
    ini_data = ParseInI('./configs/reporter.ini', 'reporter').load()
    url = ini_data['prometheus_url']
    file_type = ini_data['report_type']

    if not ('http:' in url or 'https:' in url):
        url = 'http://' + url
    # set port
    if not re.search(r':\d+', url):
        url += ':9090'
    time_interval = ini_data['time_interval']

    time_num, time_unit = re.match(r'(\d+)(.*)', time_interval).groups()
    time_interval_sec, query_start_time, query_end_time = format_time(time_num, time_unit, time_now_sec)

    if time_interval_sec == 'Nothing':
        err_m('Invalid input')
    elif time_interval_sec > 1209600:
        err_m('Exceeding the maximum time limit')

    # generate report
    reporter = Reporter(url, time_interval_sec, time_now_sec, query_start_time, query_end_time)

    if 'csv' in file_type:
        reporter.gen_csv_report()
    elif 'excel' in file_type:
        reporter.gen_excel_report()

    end_process = time.time()
    print 'Total: %.2fs' % (end_process - time_now_sec)
    print 'Successfully generated report'


if __name__ == "__main__":
    run()
