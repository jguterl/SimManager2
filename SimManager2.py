#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 11:58:05 2021

@author: jguterl
"""

#import libconf
#import os,io
#import shutil, itertools
import numpy as np
import datetime
import select
import subprocess
import curses
import multiprocessing
import time
import os
from functools import reduce
import itertools
import shutil
from slurm_support import sbatch
try:
    import pyslurm
except:
    pass

#from multiprocessing import Process
class classinstancemethod(classmethod):
    def __get__(self, instance, type_):
        descr_get = super().__get__ if instance is None else self.__func__.__get__
        return descr_get(instance, type_)

class SimManagerUtils():
    verbose = False
    def __init__(self):
        self.verbose=False
        
    @classinstancemethod    
    def copy_files(self,filenames:list or str, dest_path):
        if type(filenames) ==str: filenames = [filenames]            
        for fn in filenames:
            src = fn
            dest = os.path.join(dest_path,os.path.basename(fn))
            if self.verbose:
                print('Copying "{}" to "{}"'.format(src,dest))
            shutil.copyfile(src, dest)
    @classinstancemethod 
    def create_directory(self,directory, overwrite=False):
        if os.path.exists(directory): 
            if not overwrite:
                print('Directory "{}" already exists. Set overwrite=True to erase it')
            else:
                shutil.rmtree(directory)
            if self.verbose:
                print('The directory "{}" already exists.'.format(directory))
        else:
            
            if self.verbose:
                    print('Creating the directory "{}"'.format(directory))
            os.makedirs(directory, exist_ok=True)
            
     #%%   

            
          

    
class SimProcRunner():
    def __init__(self, command:list = None , directory:str = None , env_list=[],**kwargs):
        assert type(command) == list
        assert type(directory) == str
        assert type(env_list)  == list
        
        self.directory = directory
        self.output = ''
        self.start_time = 0
        self.runtime = 0
        self.process = None
        self.name = 'unknown'
        self.update_env(env_list)
        self.pipeR = None
        self.pipeW = None
        self.status = 'idle'
        self.verbose = True
        self.command = command

        
    @property
    def runtime(self):
        self.__runtime = time.time()-self.start_time
        return self__runtime

    @runtime.setter
    def runtime(self, runtime):
            self.__runtime = runtime
            
    def update_output(self)->None:
        self.output = self.output + self.flush_output()
        
    def update_status(self)->None:
        self.status = self.get_process_status(self.process)
        
    def get_status(self)->str:
        self.update_status()
        return self.status
    
    def get_output(self)->str:
        self.update_output()
        return self.output
    
    def plot_output(self,nlines = 50):
        self.update_output()
        output = self.output.split('\n')
        
        print('{}'.format("\n".join(output[max(0,len(output)-nlines):])))

    def get_last_output(self)->str:
        self.update_output()
        try:
            return self.output.split('\n')[-2]
        except:
            return self.output.split('\n')[-1]
        
    @staticmethod    
    def get_process_status(process)->str:
            if process is not None :
                try:
                    s = process.poll()
                    if s is None:
                        return 'R'
                    else:
                        return s
                except:
                    return 'U'
            else:
                return 'N'
    
    def stop(self):
        print('Stopping simulation {} through shell process ....)'.format(self.name))
        self.update_output()
        try:
            self.process.terminate()
            self.update_status()
            os.close(self.pipeW)
            os.close(self.pipeR)
            print('Simulation {} through shell process terminated.)'.format(self.name))
        except:
            pass
        

    def start(self,  **kwargs):
        if self.verbose:
            for k in ['command','directory']:
                print(k,getattr(self,k))
        
        assert self.command!=[] and type(self.command) == list
        assert self.directory is not None
        print('Starting simulation {} through shell process ....)'.format(self.name))

        (self.pipeR, self.pipeW) = os.pipe()
        self.process = subprocess.Popen(self.command,
                                        stdout=self.pipeW,
                                        stderr=self.pipeW,
                                        stdin=subprocess.PIPE,
                                        cwd=self.directory,
                                        env = self.env 
                                        )
        self.start_time = time.time()
        self.update_status()
        
    def update_env(self,env_list:list=[])->None:
        assert type(env_list) == list
        osenv = os.environ
        for (k,v) in env_list:
            print('---',k,v)
            if osenv.get(k) is not None:
                osenv[k] =  v +':' + osenv[k]
            else:
                osenv[k] = v
        self.env = osenv
        print(self.env)
       
         
    def flush_output(self)->str:
        try:
            buf = ''
            if self.pipeR is not None:
                while len(select.select([self.pipeR], [], [], 0)[0]) == 1:
                    buf = buf+os.read(self.pipeR, 10024).decode()
        except:
            buf = 'Cannot read stdout'
        return buf
    
    def is_active(self):
        try:
            if self.process.poll() is None:
                return True
        except:
            pass
        return False
    
    
    
class SimSlurmRunner():
    def __init__(self, command:list = None , directory:str = None , env_list=[],**kwargs):
        assert type(command) == list
        assert type(directory) == str
        assert type(env_list)  == list
        
        self.directory = directory
        self.output = ''
        self.start_time = 0
        self.runtime = 0
        self.process = None
        self.name = 'unknown'
        #self.update_env(env_list)
        self.pipeR = None
        self.pipeW = None
        self.status = 'idle'
        self.verbose = True
        self.command = command

        
    @property
    def runtime(self):
        self.__runtime = time.time()-self.start_time
        return self__runtime

    @runtime.setter
    def runtime(self, runtime):
            self.__runtime = runtime
            
    def update_output(self)->None:
        self.output = self.output + self.flush_output()
        
    def update_status(self)->None:
        self.status = self.get_job_status()
        
    def get_status(self)->str:
        self.update_status()
        return self.status
    
    def get_output(self)->str:
        self.update_output()
        return self.output
    
    def plot_output(self,nlines = 50):
        self.update_output()
        output = self.output.split('\n')
        
        print('{}'.format("\n".join(output[max(0,len(output)-nlines):])))

    def get_last_output(self)->str:
        self.update_output()
        try:
            return self.output.split('\n')[-2]
        except:
            return self.output.split('\n')[-1]
        
    @staticmethod 
    def get_job_status()->str:
            try:
                return pyslurm.job().get()[self.job.job_id]['job_state']
            except:
                return 'U' 
    
    def stop(self):
        print('Stopping simulation {} with slurm process ....)'.format(self.name))
        self.update_output()
        try:
            pyslurm.slurm_kill_job(self.job.job_id)
            print('Simulation {} through shell process terminated.)'.format(self.name))
        except:
            pass
        

    def start(self,  **kwargs):
        if self.verbose:
            for k in ['command','directory']:
                print(k,getattr(self,k))
        
            
        if kwargs.get('log_dir') is None: 
            kwargs['log_dir'] = os.path.join(self.directory,'logs')
            
        print('Submitting simulation {}  with slurm ....)'.format(self.name))

        assert self.command!=[] and type(self.command) == list

        
        self.job = sbatch(self.command,
                            chdir=self.directory,
                            e='%j.log',
                            o='%j.log',
                            **kwargs
                            )
        try:
            self.pipeR = pyslurm.job().get()[self.job.job_id]['std_out']
        except:
            self.pipeR = self.job.slurm_kwargs['o']
        self.start_time = time.time()
        self.update_status()
        
         
    def flush_output(self)->str:
        try:
            with open( self.pipeR, "r") as f:
                    buf = f.readlines()[-1]
        except:
                buf = 'failed to read log file {}'.format(self.pipeR)
        return buf

    
    def is_active(self):
        try:
            if self.get_process_status() == 'R' or self.get_process_status() == 'PD':
                return True
        except:
            pass
        return False
        

    



class SimMonitoring():
    
    def __init__(self):
        super(SimMonitoring,self).__init__()
        self.display_linewidth = 120
    @property
    def display_linewidth(self):
        return self.__display_linewidth
    @display_linewidth.setter
    def display_linewidth(self,lw):
        self.__display_linewidth = lw
        self.line_sep = "|{0: <{width}.{width}}|".format('-'*self.__display_linewidth, width=self.__display_linewidth)

        
    def monitor(self, refresh=2, **kwargs):
        print(self.get_monitoring_display())
        self.screen = curses.initscr()
        self.screen.clear()
        self.screen.addstr(0, 0, self.get_monitoring_display(), curses.A_PROTECT)
        self.screen.refresh()
        time.sleep(refresh)
        
            
        while True:
          try:
                self.screen.clear()
                self.screen.addstr(0, 0, self.get_monitoring_display(), curses.A_PROTECT)
                self.screen.refresh()
                time.sleep(refresh) 
                if self.get_num_active()<1:
                    break
          except:
                break
          finally:
                try:
                    curses.endwin()
                except:
                    pass
        self.display()

    def display(self):
        print('{}'.format(self.get_monitoring_display()))
        
    def get_monitoring_display(self):
        if hasattr(self,'tstart'):
            time_elapsed= time.time() -self.tstart
        else:
            time_elapsed= -1
        out = []
        out.append(self.line_sep)
        out.append("|{0: <{width}.{width}}|".format(
            'Simulations status - Time elapsed:{:.1f} - Runs:{}/{}'.format(time_elapsed, self.get_num_active(), len(self.simulations)), width=self.display_linewidth))
        out.append(self.line_sep)
        string = '{0: ^5.5s} | {1: ^6.6s} | {2: <50.50s} | {3: <50.50s}'.format(
            '#', 'Status', 'Directory', 'Output')
        out.append("|{0: <{width}.{width}}|".format(string, width=self.display_linewidth))
        out.append(self.line_sep)
        for s in self.simulations:
            string = '{0:^5} | {1:^6.6s} | {2: <50s} | {3: <50s}'.format(*(str(ss) for ss in self.get_info(s)))
            out.append("|{0: <{width}.{width}}|".format(string, width=self.display_linewidth))
        out.append(self.line_sep)
        return '\n'.join(out)  
          
    def setup_monitoring(self, runner_field='runner', name_field='name', directory_field='directory'):
        self.runner_field =runner_field
        self.name_field = name_field
        self.directory_field = directory_field
        
    def get_num_active(self):
        return [self.get_is_active(s) for s in self.simulations].count(True)
    

    def get_info(self,s):
        status = getattr(s,self.runner_field).get_status()
        output = getattr(s,self.runner_field).get_last_output()
        directory = os.path.basename(getattr(s,self.directory_field))
        name = getattr(s,self.name_field)
        return name, status, directory, output

    def get_is_active(self,s):
        return getattr(s,self.runner_field).is_active()
    
