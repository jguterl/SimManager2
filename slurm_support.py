#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 22:01:47 2021

@author: guterlj
"""


import sys
import os
import subprocess
import tempfile
import atexit
import hashlib
import datetime


from parse import *
def ConvertTimeStr(StrTime):
    
    if StrTime.count('-') == 0:
        r = parse("{}:{}:{}",StrTime)
        r =[float(rr) for rr in r]
        t = r[0]*3600+r[1]*60+r[2]
    elif StrTime.count('-') == 1:
        r= parse("{}-{}:{}:{}",StrTime)
        r = [float(rr) for rr in r]
        t = r[0]*24*3600+r[1]*3600+r[2]*60+r[3]
    else:
        t = -1
        
    return t

def tmp(suffix=".sh"):
    t = tempfile.mktemp(suffix=suffix)
    atexit.register(os.unlink, t)
    return t

import os
import time


class sbatch_utils(dict):
    def __init__(self,command, *args, **kwargs):
        super(sbatch_utils, self).__init__(*args, **kwargs)
        self.__dict__ = self
    
        KwDefaults = { 'date_in_name': False,
                     'script_dir':os.path.expanduser("~/slurm-scripts"),
                     'script_name':None,
                     'log_dir':None,
                     'bash_strict':False,
                     'run_cmd':'sbatch',
                     'depends_on':None,
                     'verbose':False,
                     'shell':'#!/bin/bash',
                     'pyslurm':False
                    }
    
        KwDefaults.update( dict((k,kwargs.pop(k)) for k in KwDefaults.keys() if k in list(kwargs.keys()) ))
        self.update(KwDefaults)
        self.slurm_kwargs = {}
        self.slurm_kwargs.update(kwargs)
        if self.pyslurm:
            try:
                import pyslurm
            except:
                self.pyslurm = False
        

    def get_template(self):
        
        return "{}\n{}\n{}\n{}".format(self.shell,self.header,self.bash_setup,self.commands)

        
    def make_header(self):
        
        # set jobname
        if ('J' or 'job_name') not in list(self.slurm_kwargs.keys()):
            self.slurm_kwargs['J'] = 'job_%j'
        
        if self.slurm_kwargs.get('job_name') is not None:
            self.slurm_kwargs['J'] = self.slurm_kwargs.pop('job_name')
            
        if self.date_in_name:
             self.slurm_kwargs['J'] = self.slurm_kwargs['J'] + str(time.strftime("_%Y%m%d_%H%M%S"))
            
        #set output/error log files    
        if ('o' or 'output') not in list(self.slurm_kwargs.keys()):
            self.slurm_kwargs['o'] = '%j.out'
        
        if ('e' or 'error') not in list(self.slurm_kwargs.keys()):
            self.slurm_kwargs['e'] = '%j.err'
        # add directory for log files
       
        if self.slurm_kwargs.get('error') is not None:
            self.slurm_kwargs['e'] =  self.slurm_kwargs.pop('error')
            
        if self.slurm_kwargs.get('output') is not None:
            self.slurm_kwargs['o'] =  self.slurm_kwargs.pop('output')
        if self.log_dir is not None:
            self.log_dir = os.path.expanduser(self.log_dir)
            if not os.path.exists(self.log_dir):
                    os.makedirs(self.log_dir)
            self.slurm_kwargs['e'] = os.path.join(self.log_dir,self.slurm_kwargs['e'])
            self.slurm_kwargs['o'] = os.path.join(self.log_dir,self.slurm_kwargs['o'])
        
        
        #create SBATCH header
        header = []
        for k, v in self.slurm_kwargs.items():
            if len(k) > 1:
                k = "--" + k + "="
            else:
                k = "-" + k + " "
            header.append("#SBATCH {}{}".format(k, v))
        
        self.header = "\n".join(header)
    
    def make_bash_setup(self):
        bash_setup = []
        if self.bash_strict:
            bash_setup.append("set -eo pipefail -o nounset")

        self.bash_setup = "\n".join(bash_setup)
    
    def make_commands(self):
        self.commands = "\n".join([self.commands]) if type(self.commands) != list else "\n".join(self.commands)



class SlurmSbatch(sbatch_utils):
    def __init__(self,commands:list or str,*args,**kwargs):
        super(SlurmSbatch, self).__init__(commands,*args, **kwargs)
        self.commands = commands
        self.make_header()
        self.make_bash_setup()
        self.make_commands()
        
    def sbatch(self):
        self.write_job_file(self.get_template())
        self.submit_job()
        
    def submit_job(self):
        
        if self.pyslurm:
            if self.verbose: print ('Submitting with pyslurm sbatch file: {}'.format(self.filename))
            self.job_id = pyslurm.job().submit_batch_job({"script": self.filename})
            print('pyslurm:',self.job_id)
            
        else:
            if self.verbose: print ('Submitting with process sbatch file: {}'.format(self.filename))
            res = subprocess.check_output([self.run_cmd,self.filename]).strip()
            print(res, file=sys.stderr)
            if not res.startswith(b"Submitted batch"):
                self.job_id = None
            self.job_id = int(res.split()[-1])  
            print(self.job_id)
            
        
    def write_job_file(self,template):
        if self.script_name is None:
            jobname = self.slurm_kwargs.get('J') if self.slurm_kwargs.get('J') is not None else 'job'
            self.filename =  jobname + str(time.strftime("_%Y%m%d_%H%M%S")) +'.sbatch'
        else:
            self.filename = os.path.splitext(self.script_name)[0] +'.sbatch'
            
        if self.script_dir is not None:
            self.script_dir = os.path.abspath(os.path.expanduser(self.script_dir))
            if not os.path.exists(self.script_dir):
                    os.makedirs(self.script_dir)
            self.filename =  os.path.join(os.path.expanduser(self.script_dir),self.filename)
            print('job script:',self.filename)
        
        with open(self.filename, "w") as sh:
            sh.write(self.get_template())
        if self.verbose: print('Created sbatch file {} '.format(self.filename))
        
        
class sbatch(SlurmSbatch):
    def __init__(self,commands:list,*args,**kwargs):
        super(sbatch, self).__init__(commands,*args, **kwargs)
        self.sbatch()            
        