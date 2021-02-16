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

def worker(job_queue, r_queue, i, node_shape, node_type, link_shape, link_type):

    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)

    num_zones = par['num_processor']
    # node_file = par['node_file']
    # link_file = par['link_file']
    num_time_steps = par['num_time_steps']
    
    d = 0
    sp = tdsp.tdsp(node_shape, node_type, link_shape, link_type, num_zones, num_time_steps)
    while True :              
        tasks = job_queue.get()
            
        for task in tasks:
            if task[0] == 'Done':
                d = 1
                break
            sp.build(task) 
            sp.trace(task)
                
        if d == 1:
            break
            
    vol = sp.get_vol()
    # with pysnooper.snoop('snooper_log.log'):
    shm = shared_memory.SharedMemory(name='shared_vol')
    shared_vol = np.ndarray(vol.shape, dtype=vol.dtype, buffer=shm.buf)
    shared_vol += vol
    # r_queue.put(shared_vol[1])
    r_queue.put('processor ' + str(i))
    shm.close() 

                     

