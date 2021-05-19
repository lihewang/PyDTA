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
import timeit
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
        self.par = par

    def run(self):
        try:
            sp = tdsp.tdsp(self.shm_par, self.par)
            trip_cls = list(self.par['vot'].keys()) # get trip classes
            shm = shared_memory.SharedMemory(name='shared_vol')
            shared_vol = np.ndarray((len(trip_cls), self.shm_par[2][0], self.num_time_steps), dtype=np.dtype(np.float32), buffer=shm.buf) 
            count = 0
            if self.iter > 1:    #update time
                sp.update_time(shared_vol, len(trip_cls))
            t1 = timeit.default_timer()

            
            while True :              
                tasks = self.job_queue.get()
                
                if tasks.empty:                         #run finished
                    vol = sp.get_vol(len(trip_cls)) 
                    shared_vol += np.asarray(vol)*(1/self.iter)     #MSA
                    break

                tgrp = tasks.groupby(['I','period','class']).agg({'J':list, 'trip':list})
                tgrp.reset_index(inplace=True)  
                count += len(tgrp)         
                for task in tgrp.to_numpy():                           #build task for one-to-many path building
                    ctoll = 1/(self.par['vot'][task[2]]/60) 
                    tcls_idx = trip_cls.index(task[2])           #minute per dollar
                    t = [task[0], task[1], task[2], np.array(task[3], dtype='i'), np.array(task[4]), ctoll, tcls_idx]                   
                    nodes = sp.build(t)                    
                    sp.trace(t, nodes)
            t2 = timeit.default_timer()
            time_length = round(t2 - t1, 1)
            # print(f"Run time path builder {t2 - t1:0.2f} seconds", flush=True) 
        except:
            logging.exception("message")   
                
        self.rtn_queue.put('processor ' + str(self.i) + ' number of paths built ' + str(count) + ' time ' + str(time_length) + ' seconds')
        shm.close() 

                     

