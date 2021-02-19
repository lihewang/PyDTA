# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 15:45:34 2021

@author: lihe.wang
"""
import logging
import yaml as ym
import tdsp
import numpy as np
from multiprocessing import Process, shared_memory

def worker(job_queue, r_queue, i, shm_par):
    
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)

    num_zones = par['num_processor']
    num_time_steps = par['num_time_steps']

    d = 0   #done
    sp = tdsp.tdsp(shm_par, num_zones, num_time_steps)
    try:
        while True :              
            tasks = job_queue.get() #array of task as ['I','period','class',['dest'],['trip']]             
            for task in tasks: #build task for one-to-many path building
                if task[0] == 'Done':
                    d = 1
                    break
                t = [task[0], task[1], task[2], np.array(task[3]), np.array(task[4])]
                nodes = sp.build(t) 
                sp.trace(t, nodes)
                    
            if d == 1:
                break
    except:
         logging.exception("message")       
    vol = sp.get_vol()

    shm = shared_memory.SharedMemory(name='shared_vol')
    shared_vol = np.ndarray(vol.shape, dtype=vol.dtype, buffer=shm.buf)
    shared_vol += vol
    # r_queue.put(shared_vol[1])
    r_queue.put('processor ' + str(i))
    shm.close() 

                     

