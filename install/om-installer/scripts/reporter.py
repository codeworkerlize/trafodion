#!/usr/bin/env python
# -*- coding:UTF-8 -*-
import re
import os
import csv
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import xlwt
import time
import math
import json
import codecs
import httplib2
import numpy as np
from common import err_m


def format_time(time_num, time_unit, time_now_sec):
    """Returns the time interval in seconds, the start time and end time of the query"""
    time_num = int(time_num)
    switcher = {
        "m": (time_num * 60, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_now_sec - time_num * 60)),
              time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_now_sec))),
        "h": (time_num * 3600, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_now_sec - time_num * 3600)),
              time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_now_sec))),
        "d": (time_num * 24 * 3600, time.strftime('%Y-%m-%d', time.localtime(time_now_sec - time_num * 24 * 3600)),
              time.strftime('%Y-%m-%d', time.localtime(time_now_sec))),
        "w": (time_num * 7 * 24 * 3600, time.strftime('%Y-%m-%d', time.localtime(time_now_sec - time_num * 7 * 24 * 3600)),
              time.strftime('%Y-%m-%d', time.localtime(time_now_sec)))
    }
    return switcher.get(time_unit, "Nothing")


class Reporter(object):
    """Get metric data from Prometheus API and generate report for Esgyndb"""
    def __init__(self, url, time_interval, time_now, query_start_time, query_end_time):
        self.url = url
        self.start_time = query_start_time
        self.end_time = query_end_time
        self.step = math.ceil(time_interval / 11000.0)
        self.end_time_sec = time_now
        self.start_time_sec = self.end_time_sec - time_interval
        self.h = httplib2.Http()

    def _get_data(self, query):
        param = "?query=%s&time=%s" % (query, self.end_time_sec)
        url = self.url + "/api/v1/query" + param
        res, cont = self.h.request(url, 'GET')
        if res["status"] == '200':
            cont = json.loads(cont)
            return cont["data"]["result"]
        else:
            err_m('Query [%s] error\n%s\n%s' % (query, res, cont))

    def _get_range_date(self, query, step):
        param = "?query=%s&start=%s&end=%s&step=%s" % (query, self.start_time_sec, self.end_time_sec, step)
        url = self.url + "/api/v1/query_range" + param
        res, cont = self.h.request(url, 'GET')
        if res["status"] == '200':
            cont = json.loads(cont)
            return cont["data"]["result"]
        else:
            err_m('Query [%s] error\n%s\n%s' % (query, res, cont))

    def _set_query_step(self, frequency):
        if self.step < frequency:
            return '%ds' % frequency
        elif self.step >= frequency:
            return '%ds' % self.step

    def size_format(self, size, rate=False, suffix=True):
        size = float(size)
        if not suffix:
            return '%.2f' % size
        power = 2 ** 10
        n = 0
        size_unit = {0: 'MB', 1: 'GB', 2: 'TB'}
        rate_unit = {0: 'Bs', 1: "KBs", 2: 'MBs', 3: 'GBs'}
        while size > power:
            size /= power
            n += 1
        if not rate:
            return '%.2f%s' % (size, size_unit[n])
        else:
            return '%.2f%s' % (size, rate_unit[n])

    def cluster_usage_current(self, data_api, frequency, info, size=False, suffix=True):
        cluster_info = []
        query_step = self._set_query_step(frequency)

        usage_current = self._get_data(data_api)
        try:
            usage_current = float(usage_current[0]["value"][1])

            if not size and suffix:
                cluster_info.append(['集群%s' % info, '%.2f%%' % usage_current])
            else:
                usage_current = self.size_format(usage_current)
                cluster_info.append(['集群%s' % info, '%s' % usage_current])
        except IndexError:
            cluster_info.append(['集群%s' % info, 'N/A'])

        usage_data = self._get_range_date(data_api, query_step)
        try:
            usage_data = np.array(usage_data[0]["values"], float)
            usage_data_min = usage_data.min(axis=0)[1]
            usage_data_max = usage_data.max(axis=0)[1]

            if not size and suffix:
                cluster_info.append(['集群%s的最小值' % info, '%.2f%%' % usage_data_min])
                cluster_info.append(['集群%s的最大值' % info, '%.2f%%' % usage_data_max])
            else:
                usage_data_min = self.size_format(usage_data_min)
                usage_data_max = self.size_format(usage_data_max)
                cluster_info.append(['集群%s的最小值' % info, '%s' % usage_data_min])
                cluster_info.append(['集群%s的最大值' % info, '%s' % usage_data_max])
        except IndexError:
            cluster_info.append(['集群%s的最小值' % info, 'N/A'])
            cluster_info.append(['集群%s的最大值' % info, 'N/A'])

        return cluster_info

    def node_usage_current(self, data_api, frequency, info, size=False, suffix=True):
        node_info = []
        query_step = self._set_query_step(frequency)

        if 'esgyn_schema_usage' in data_api:
            key = 'schemaName'
        else:
            key = 'instance'

        usage_current = self._get_data(data_api)

        for node_usage_current in usage_current:
            try:
                node_name = re.match(r'(.*):.*', node_usage_current["metric"][key]).group(1)
            except AttributeError:
                node_name = node_usage_current["metric"][key]

            try:
                node_usage_current = float(node_usage_current["value"][1])
                if not size and suffix:
                    node_info.append(['%s %s' % (node_name, info), '%.2f%%' % node_usage_current])
                else:
                    node_usage_current = self.size_format(node_usage_current)
                    node_info.append(['%s %s' % (node_name, info), '%s' % node_usage_current])
            except IndexError:
                node_info.append(['%s %s' % (node_name, info), 'N/A'])

        usage_data = self._get_range_date(data_api, query_step)

        for node_usage_data in usage_data:
            try:
                node_name = re.match(r'(.*):.*', node_usage_data["metric"][key]).group(1)
            except AttributeError:
                node_name = node_usage_data["metric"][key]

            try:
                node_usage_data = np.array(node_usage_data["values"], float)
                node_data_min = node_usage_data.min(axis=0)[1]
                node_data_max = node_usage_data.max(axis=0)[1]

                if not size and suffix:
                    node_info.append(['%s %s的最小值' % (node_name, info), '%.2f%%' % node_data_min])
                    node_info.append(['%s %s的最大值' % (node_name, info), '%.2f%%' % node_data_max])
                else:
                    node_data_min = self.size_format(node_data_min)
                    node_data_max = self.size_format(node_data_max)
                    node_info.append(['%s %s的最小值' % (node_name, info), '%s' % node_data_min])
                    node_info.append(['%s %s的最大值' % (node_name, info), '%s' % node_data_max])
            except IndexError:
                node_info.append(['%s %s的最小值' % (node_name, info), 'N/A'])
                node_info.append(['%s %s的最大值' % (node_name, info), 'N/A'])

        return node_info

    def cluster_usage_avg(self, data_api, frequency, info, size=False, rate=False, suffix=True):
        cluster_info = []
        query_step = self._set_query_step(frequency)
        data = self._get_range_date(data_api, query_step)

        try:
            data = np.array(data[0]["values"], float)
            usage_avg = data.mean(axis=0)[1]
            usage_data_min = data.min(axis=0)[1]
            usage_data_max = data.max(axis=0)[1]

            if not size and suffix:
                cluster_info.append(['集群%s的平均值' % info, '%.2f%%' % usage_avg])
                cluster_info.append(['集群%s的最小值' % info, '%.2f%%' % usage_data_min])
                cluster_info.append(['集群%s的最大值' % info, '%.2f%%' % usage_data_max])
            else:
                usage_avg = self.size_format(usage_avg, rate, suffix)
                usage_data_min = self.size_format(usage_data_min, rate, suffix)
                usage_data_max = self.size_format(usage_data_max, rate, suffix)
                cluster_info.append(['集群%s的平均值' % info, '%s' % usage_avg])
                cluster_info.append(['集群%s的最小值' % info, '%s' % usage_data_min])
                cluster_info.append(['集群%s的最大值' % info, '%s' % usage_data_max])
        except IndexError:
            cluster_info.append(['集群%s的平均值' % info, 'N/A'])
            cluster_info.append(['集群%s的最小值' % info, 'N/A'])
            cluster_info.append(['集群%s的最大值' % info, 'N/A'])

        return cluster_info

    def node_usage_avg(self, data_api, frequency, info, size=False, rate=False, suffix=True):
        node_info = []
        query_step = self._set_query_step(frequency)
        usage_data = self._get_range_date(data_api, query_step)

        for node_usage_data in usage_data:
            node_name = re.match(r'(.*):.*', node_usage_data["metric"]["instance"]).group(1)

            try:
                node_usage_data = np.array(node_usage_data["values"], float)
                node_usage_avg = node_usage_data.mean(axis=0)[1]
                node_data_min = node_usage_data.min(axis=0)[1]
                node_data_max = node_usage_data.max(axis=0)[1]

                if not size and suffix:
                    node_info.append(['%s %s的平均值' % (node_name, info), '%.2f%%' % node_usage_avg])
                    node_info.append(['%s %s的最小值' % (node_name, info), '%.2f%%' % node_data_min])
                    node_info.append(['%s %s的最大值' % (node_name, info), '%.2f%%' % node_data_max])
                else:
                    node_usage_avg = self.size_format(node_usage_avg, rate, suffix)
                    node_data_min = self.size_format(node_data_min, rate, suffix)
                    node_data_max = self.size_format(node_data_max, rate, suffix)
                    node_info.append(['%s %s的平均值' % (node_name, info), '%s' % node_usage_avg])
                    node_info.append(['%s %s的最小值' % (node_name, info), '%s' % node_data_min])
                    node_info.append(['%s %s的最大值' % (node_name, info), '%s' % node_data_max])
            except IndexError:
                node_info.append(['%s %s的平均值' % (node_name, info), 'N/A'])
                node_info.append(['%s %s的最小值' % (node_name, info), 'N/A'])
                node_info.append(['%s %s的最大值' % (node_name, info), 'N/A'])

        return node_info

    def format_usage_current(self, mode, query, frequency, info, size=False, suffix=True):
        data = []
        line_breaker = [['', '']]
        cluster_info = self.cluster_usage_current('%s(%s)' % (mode, query), frequency, info, size, suffix)
        data.extend(cluster_info)
        node_info = self.node_usage_current(query, frequency, info, size, suffix)
        data.extend(node_info)
        data.extend(line_breaker)
        return data

    def format_usage_avg(self, mode, query, frequency, info, size=False, rate=False, suffix=True):
        data = []
        line_breaker = [['', '']]
        cluster_info = self.cluster_usage_avg('%s(%s)' % (mode, query), frequency, info, size, rate, suffix)
        data.extend(cluster_info)
        node_info = self.node_usage_avg(query, frequency, info, size, rate, suffix)
        data.extend(node_info)
        data.extend(line_breaker)
        return data

    def receive_data(self):
        esgyn_info = []
        system_info = []
        # cpu usage
        cpu_info = self.format_usage_avg('avg', '(1-avg%20by%20(instance)%20(irate(node_cpu_seconds_total{mode="idle"}[5m])))*100',
                                          15, 'CPU使用率')
        esgyn_info.extend(cpu_info)

        # memory usage
        memory_info = self.format_usage_avg('avg', '(1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes)*100',
                                             15, '内存使用率')
        esgyn_info.extend(memory_info)

        # TPM
        tpm_abort = self.cluster_usage_avg('irate(esgyn_dtm_status_txnAborts[5m])', 60, '事务终止数', suffix=False)
        esgyn_info.extend(tpm_abort)
        tpm_begin = self.cluster_usage_avg('irate(esgyn_dtm_status_txnBegins[5m])', 60, '事务开始数', suffix=False)
        esgyn_info.extend(tpm_begin)
        tpm_commit = self.cluster_usage_avg('irate(esgyn_dtm_status_txnCommits[5m])', 60, '事务提交数', suffix=False)
        esgyn_info.extend(tpm_commit)
        esgyn_info.extend(['', ''])

        # session
        session_info = self.format_usage_avg('sum', 'sum%20by%20(instance)%20(esgyn_active_sessions_connected)', 60, '会话数', suffix=False)
        esgyn_info.extend(session_info)

        # Esp count
        esp_info = self.format_usage_avg('sum', 'esgyn_db_status_esp', 15, 'ESP数量', suffix=False)
        esgyn_info.extend(esp_info)

        # schema usage
        schema_info = self.format_usage_current('sum', 'esgyn_schema_usage', 300, 'Schema已使用', size=True)
        esgyn_info.extend(schema_info)

        # root usage
        root_info = self.format_usage_current('avg', '(1-(node_filesystem_free_bytes{mountpoint="/",fstype=~"ext4|xfs"})/(node_filesystem_size_bytes{mountpoint="/",fstype=~"ext4|xfs"}))*100',
                                            15, '磁盘根目录已使用')
        esgyn_info.extend(root_info)

        # core usage
        core_info = self.format_usage_current('sum', 'esgyn_file_usage_esgyndb_core', 20, 'Core文件已使用', size=True)
        esgyn_info.extend(core_info)

        # log file usage
        log_info = self.format_usage_current('sum', 'esgyn_file_usage_esgyndb_log', 20, 'Esgyndb日志文件已使用', size=True)
        esgyn_info.extend(log_info)

        # memory usage of mxosrvr
        mxosrve_info = self.format_usage_avg('sum', 'esgyn_memory_usage_mxosrvr', 15, 'Mxosrvr使用内存', size=True)
        esgyn_info.extend(mxosrve_info)

        # network download
        download_info = self.format_usage_avg('sum', 'sum%20by%20(instance)%20(irate(node_network_receive_bytes_total{device!~"tap.*"}[5m]))',
                                               15, '网络下载流量', size=True, rate=True)
        system_info.extend(download_info)

        # network upload
        upload_info = self.format_usage_avg('sum', 'sum%20by%20(instance)%20(irate(node_network_transmit_bytes_total{device!~"tap.*"}[5m]))',
                                             15, '网络上传流量', size=True, rate=True)
        system_info.extend(upload_info)

        # disk read
        disk_read = self.format_usage_avg('sum', 'avg%20by%20(instance)%20(irate(node_disk_read_bytes_total[1m]))',
                                           15, '磁盘读数据量', size=True, rate=True)
        system_info.extend(disk_read)

        # disk write
        disk_write = self.format_usage_avg('sum', 'avg%20by%20(instance)%20(irate(node_disk_written_bytes_total[1m]))',
                                            15, '磁盘写数据量', size=True, rate=True)
        system_info.extend(disk_write)

        # node load
        load_info = self.format_usage_avg('avg', 'node_load5', 15, '系统负载', suffix=False)
        system_info.extend(load_info)

        # file descriptor
        descriptor_info = self.format_usage_avg('avg', 'node_filefd_allocated', 15, '打开的文件描述符', suffix=False)
        system_info.extend(descriptor_info)

        # swap usage
        swap_info = self.format_usage_avg('avg', '(1-node_memory_SwapFree_bytes/node_memory_SwapTotal_bytes)*100', 15, 'Swap分区使用率')
        system_info.extend(swap_info)

        # hadoop log file usage
        hadooplog_info = self.format_usage_current('sum', 'esgyn_file_usage_hadoop_log', 20, 'Hadoop日志文件已使用', size=True)
        system_info.extend(hadooplog_info)

        return esgyn_info, system_info

    def gen_excel_report(self):
        esgyn_info, system_info = self.receive_data()

        # generate excel
        workbook = xlwt.Workbook(encoding='utf-8')
        esgynsheet = workbook.add_sheet('Esgyndb Info')
        systemsheet = workbook.add_sheet('System Info')

        head_style = xlwt.XFStyle()
        # set font for header
        font = xlwt.Font()
        font.bold = True
        font.height = 0x0198

        alignment = xlwt.Alignment()
        alignment.horz = 0x02
        alignment.vert = 0x01

        borders = xlwt.Borders()
        borders.top = 0x01
        borders.bottom = 0x01
        borders.left = 0x01
        borders.right = 0x01

        head_style.font = font
        head_style.alignment = alignment
        head_style.borders = borders

        data_style = xlwt.XFStyle()
        data_style.borders = borders

        # write header
        esgynsheet.write_merge(0, 0, 0, 1, 'Esgyndb 信息\n%s - %s' % (self.start_time, self.end_time), head_style)
        systemsheet.write_merge(0, 0, 0, 1, '系统信息\n%s - %s' % (self.start_time, self.end_time), head_style)

        # set width for column
        esgynsheet.col(0).width = 10000
        systemsheet.col(0).width = 10000

        tall_style = xlwt.easyxf('font:height 920')
        esgynsheet.row(0).set_style(tall_style)
        systemsheet.row(0).set_style(tall_style)

        # write esgyndb info
        i = 1
        for data in esgyn_info:
            for j in xrange(len(data)):
                esgynsheet.write(i, j, data[j], data_style)
            i = i + 1

        # write system info
        i = 1
        for data in system_info:
            for j in xrange(len(data)):
                systemsheet.write(i, j, data[j], data_style)
            i = i + 1
        workbook.save('./Esgyn report%s - %s.xls' % (self.start_time, self.end_time))

    def gen_csv_report(self):
        esgyn_info, system_info = self.receive_data()

        with open('./Esgyndb-report%s - %s.csv' % (self.start_time, self.end_time), 'w') as f:
            f.write(codecs.BOM_UTF8)
            header = ['Esgyndb 信息', '%s - %s' % (self.start_time, self.end_time)]
            f_csv = csv.writer(f)
            f_csv.writerow(header)
            f_csv.writerows(esgyn_info)

        with open('./System-report%s - %s.csv' % (self.start_time, self.end_time), 'w') as f:
            header = ['系统信息', '%s - %s' % (self.start_time, self.end_time)]
            f.write(codecs.BOM_UTF8)
            f_csv = csv.writer(f)
            f_csv.writerow(header)
            f_csv.writerows(system_info)
