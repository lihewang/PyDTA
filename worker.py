# -*- coding: utf-8 -*-
"""
Created on Wed Feb 3 2021
@author: Lihe Wang  

Multiprocessing worker process
"""
import logging
import yaml as ym
import tdsp
import numpy as np
import pandas as pd
from multiprocessing import Process, shared_memory

class worker(Process):

    def __init__ (self, job_queue, rtn_queue, i, shm_par, par, iter):
        super(worker, self).__init__()
        self.num_processor = par['num_processor']
        self.num_time_steps = par['num_time_steps']
        self.num_zones = par['num_zones']
        self.job_queue = job_queue
        self.rtn_queue = rtn_queue
        self.shm_par = shm_par
        self.i = i
        self.iter = iter

    def run(self):
        try:
            sp = tdsp.tdsp(self.shm_par, self.num_zones, self.num_time_steps)
            shm = shared_memory.SharedMemory(name='shared_vol')
            shared_vol = np.ndarray((self.shm_par[2][0], self.num_time_steps), dtype=np.dtype(np.float32), buffer=shm.buf) 

            if self.iter > 1:    #update time
                sp.update_time(shared_vol)

            while True :              
                tasks = self.job_queue.get()

                if tasks.empty:                         #run finished
                    vol = sp.get_vol() 
                    shared_vol += vol*(1/self.iter)     #MSA
                    break

                tgrp = tasks.groupby(['I','period','class']).agg({'J':list, 'trip':list})
                tgrp.reset_index(inplace=True)           
                for task in tgrp.to_numpy():        #build task for one-to-many path building
                    t = [task[0], task[1], task[2], np.array(task[3]), np.array(task[4])]
                    nodes = sp.build(t) 
                    sp.trace(t, nodes)

        except:
            logging.exception("message")   
                
        self.rtn_queue.put('processor ' + str(self.i))
        shm.close() 

                     

