# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 15:45:34 2021

@author: lihe.wang
"""
# import pysnooper
import yaml as ym
import tdsp
import numpy as np
from multiprocessing import Process, shared_memory

class worker(Process):
    
    def __init__(self, job_queue, r_queue, i):
        super(worker, self).__init__()
        with open(r'..\Data\control.yaml') as file:
            par = ym.full_load(file)
        self.i = i    
        self.num_zones = par['num_processor']
        self.node_file = par['node_file']
        self.link_file = par['link_file']
        self.num_time_steps = par['num_time_steps']
        self.job_queue = job_queue
        self.r_queue = r_queue
        
 
    def run(self):
        
        d = 0
        sp = tdsp.tdsp(self.node_file, self.link_file, self.num_zones, self.num_time_steps)
        while True :              
            tasks = self.job_queue.get()
            
            for task in tasks:
                if task[0] == 'Done':
                    d = 1
                    break
                sp.build(task) 
                t, d = sp.trace(task)
                
            if d == 1:
                break
            
        vol = sp.get_vol()
        # with pysnooper.snoop('snooper_log.log'):
        shm = shared_memory.SharedMemory(name='shared_vol')
        shared_vol = np.ndarray(vol.shape, dtype=vol.dtype, buffer=shm.buf)
        shared_vol += vol
        # self.r_queue.put(shared_vol[1])
        self.r_queue.put('processor ' + str(self.i))
        shm.close() 

                     

