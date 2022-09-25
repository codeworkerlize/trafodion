#!/usr/bin/env python

# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018-2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
import sys
import getpass
import json
import os
import subprocess
import datetime
import socket
from optparse import OptionParser
from crontab import CronTab
import logging, logging.handlers

traf_log = os.environ.get("TRAF_LOG")
traf_var = os.environ.get("TRAF_VAR")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Create the Handler for logging data to a file
logger_handler = logging.handlers.RotatingFileHandler(traf_log + '/cron_scheduler.log', 'a', 10000000, 5)
logger_handler.setLevel(logging.INFO)
 
# Create a Formatter for formatting the log messages
customFields = { 'component': 'CRON_SCHEDULER', 'host': socket.gethostname() }
logger_formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(component)s, Node: %(host)s,, PIN: %(process)d, Process Name: %(processName)s,,,%(message)s')
 
# Add the Formatter to the Handler
logger_handler.setFormatter(logger_formatter)
 
# Add the Handler to the Logger
logger.addHandler(logger_handler)

# Add the custom fields to the logger format
logger = logging.LoggerAdapter(logger, customFields)

def get_options():
    usage = 'usage: %prog [options]\n'
    usage += '  Esgyn cron scheduler script.'
    parser = OptionParser(usage=usage)
    parser.add_option("-a", "--add", action="store_true", dest="add", default=False,
                      help="Schedule a new job.")
    parser.add_option("-m", "--modify", action="store_true", dest="modify", default=False,
                      help="Modify an existing job.")
    parser.add_option("-p", "--pause", action="store_true", dest="pause", default=False,
                      help="Pause an existing job schedule.")
    parser.add_option("-r", "--resume", action="store_true", dest="resume", default=False,
                      help="Resume an existing job schedule.")
    parser.add_option("-d", "--delete", action="store_true", dest="delete", default=False,
                      help="Delete an existing job.")
    parser.add_option("-g", "--get", action="store_true", dest="get", default=False,
                      help="Get details for one or more jobs")
    parser.add_option("-s", "--sync", action="store_true", dest="sync", default=False, 
                      help="Synchronizes the crontab files to all nodes")
    parser.add_option("-c", "--cmd", dest="job_cmd", metavar="JOBCMD",
                      help="The command or script to schedule.")
    parser.add_option("-e", "--expr", dest="cron_expr", default=False,
                      help="Specify the cron expression")
    parser.add_option("-u", "--user", dest="user", metavar="USER",
                      help="Specify the user name that should own the job")
    parser.add_option("-j", "--job", dest="job_id",  metavar="JOBID",
                      help="The job id")
    parser.add_option("--comment", dest="comment",  metavar="COMMENT",
                      help="A comment for the cron job")

    (options, args) = parser.parse_args()
    return options

class CronManager(object):
    def __init__(self, uname):
        self.uname = uname
        self.my_cron = CronTab(user=uname)
        self.my_cron.env['SHELL'] = '/bin/bash'
        
    def _new_job(self, options):
        job = self.my_cron.new(command='source $HOME/.bash_profile;'+options.job_cmd, comment=options.comment)
        job.setall(options.cron_expr)
        job.user = self.uname
        self.my_cron.write()
        logger.info("Scheduled job " + str(options.comment))
        
    def _modify_job(self, options):
        jobs = self.my_cron.find_comment(options.job_id)
        job = jobs.next()
        job.setall(options.cron_expr)
        job.set_command(options.job_cmd)
        self.my_cron.write()
        logger.info("Modified schedule for job " + str(options.job_id))
        
    def _delete_job(self, jobId):
        if jobId == 'backup-aging' or jobId == 'bk-aging' or jobId == 'br-aging':
           logger.warn("Backup aging is a system created job. You cannot delete it. You can however disable it.")
           return

        for job in self.my_cron.find_comment(jobId):
            self.my_cron.remove(job)
            self.my_cron.write()
        logger.info("Deleted job " + jobId)
    
    def _pause_job(self, jobId):
        for job in self.my_cron.find_comment(jobId):
           job.enabled = False
        self.my_cron.write()
        logger.info("Pause job " + jobId)

    def _resume_job(self, jobId):
        for job in self.my_cron.find_comment(jobId):
           job.enabled = True
        self.my_cron.write()
        logger.info("Resume job " + jobId)
        
    def _get_job_details(self, job):
        #sch = job.schedule(date_from=datetime.datetime.now()).get_next()
        jobId = job.comment.encode('utf-8')
        logger.info('getting job info '+ jobId)
        cron_expr = str(job.slices.clean_render()).encode('utf-8')
        cmd = job.command.encode('utf-8')
        is_enabled = job.is_enabled()
        return {'jobID':jobId, 'command': cmd, 'cron_expression': cron_expr , 'state': is_enabled}

    def _get_job(self, jobId):
        jobs = self.my_cron.find_comment(jobId)
        return self._get_job_details(jobs.next())
    
    def _get_all_jobs(self):
        jobarr = []
        for job in self.my_cron:
            jobarr.append(self._get_job_details(job))
        return jobarr

    def _copy_to_all_nodes(self):
        backup_file = 'trafcrontab.bak'
        local_file = traf_var + '/' + backup_file
        hdfs_dir = '/user/trafodion/backups/'
        hdfs_file = hdfs_dir + backup_file 

        retcode, msg = self._exec_cmd('crontab -l > %s' % local_file)
        if retcode !=0 :
           logger.error('crontab export failed : %s' % msg)
           return
 
        retcode, msg = self._exec_cmd('hdfs dfs -copyFromLocal -f %s %s' % (local_file, hdfs_dir))
        if retcode !=0 :
           logger.error('Failed to copy crontab to hdfs: %s' % msg)
           return
        
        retcode, msg = self._exec_cmd('edb_pdsh -a mv %s %s.1' % (local_file, local_file))

        retcode, msg = self._exec_cmd('edb_pdsh -a hdfs dfs -copyToLocal %s %s' % (hdfs_file, local_file))
        if retcode !=0 :
           logger.error('Failed to copy crontab from hdfs to local : %s' % msg)
           return
 
        retcode, msg = self._exec_cmd('edb_pdsh -a crontab %s' % local_file)
        if retcode !=0 :
           logger.error('Failed to import crontab file : %s' % msg)
           return
        logger.info('crontab file updated on all nodes')

    def _exec_cmd(self, cmd):
        logger.info('executing command %s' % cmd)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, executable='/bin/bash')
        stdout, stderr = p.communicate()
        msg = stdout
        if p.returncode != 0:
            msg = stderr if stderr else stdout
        return p.returncode, msg

def main():
    options = get_options()

    def err(msg):
        sys.stderr.write('[ERROR] ' + msg + '\n')
        sys.exit(1)
    
    ### validate parameters ###
    val = 0
    if options.add: val += 1
    if options.delete: val += 1
    if options.modify: val += 1
    if options.pause: val += 1
    if options.resume: val += 1
    if options.get: val += 1

    if val != 1:
        err('Must specify only one operation: <add,delete,modify,pause,resume,get>')
    
    login_user = getpass.getuser()
    datetime.datetime.now()
    try:
        cm = CronManager(login_user)
        if options.job_id:
            options.job_id = options.job_id.decode("utf-8")
        if options.add:
            if not options.job_cmd:
                err("Specify the job's command string, using the -c option")
            if not options.cron_expr:
                err("Specify the cron schedule expression using the -e option")
            cm._new_job(options)
        elif options.pause:
            if not options.job_id:
                err('Please specify the job id to pause, using the -j option')
            cm._pause_job(options.job_id)
        elif options.resume:
            if not options.job_id:
                err('Please specify the job id to resume, using the -j option')
            cm._resume_job(options.job_id)
        elif options.delete:
            if not options.job_id:
                err('Please specify the job id to delete, using the -j option')
            cm._delete_job(options.job_id)
        elif options.modify:
            if not options.job_id:
                err('Please specify the job id to modify, using the -j option')
            if not options.job_cmd:
                err("Specify the job's command string, using the -c option")
            if not options.cron_expr:
                err("Specify the cron schedule expression using the -e option")
            cm._modify_job(options)
        elif options.get:
            if options.job_id:
                print json.dumps(cm._get_job(options.job_id))
            else:
                print json.dumps(cm._get_all_jobs())

        if options.sync and not options.get:
           cm._copy_to_all_nodes()

    except Exception as e:
        logger.error(str(e))
        err(str(e))
            
if __name__ == '__main__':
    main()
