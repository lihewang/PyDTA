# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
#packages installed: cython, pandas, cymem, pyyaml, numpy
import pyximport; pyximport.install(reload_support=True)
import logging
import logging.config
import yaml as ym
import pandas as pd
import numpy as np
import timeit
import time
from multiprocessing import Process, Queue, shared_memory
from worker import worker
from read_network import read
import tdsp
from importlib import reload

def set_log(file_name):
    logging.shutdown()
    reload(logging)

    # logging.config.fileConfig('logging.config', disable_existing_loggers=False)
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)
    
    fh = logging.FileHandler(file_name, mode='w')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s : %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    log.addHandler(ch)
    log.addHandler(fh) 
    return log
    
if __name__ == "__main__":  
        
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)
        
    #set logging
    log = set_log(par['log_file'])    
    log.info('//--- PyDTA start ---//') 
    
    multi_p = True
    # multi_p = False
        
    t1 = timeit.default_timer()
    
    # read network files to shared memory
    shm, shm_par  = read(par['node_file'], par['link_file'])
    shm_node = shm[0] 
    shm_link = shm[1] 
    
    #create shared memory for volume
    df_link = pd.read_csv(par['link_file']) 
    df_ab = df_link[['A','B']]                                     
    arr = np.zeros((len(df_link), par['num_time_steps']))           #numpy array for volume
    shm_vol = shared_memory.SharedMemory(name='shared_vol', create=True, size=arr.nbytes)
    ndarr_vol = np.ndarray(arr.shape, dtype=arr.dtype, buffer=shm_vol.buf)
    ndarr_vol[:] = arr[:]

    if multi_p:
        #start multiprocessors
        processes = []
        q = Queue()
        r = Queue()

        for i in range(par['num_processor']):
            p=Process(target=worker, args=(q, r, i, shm_par))
            time.sleep(1.5)
            p.start()
            processes.append(p)  
    else:
        sp = tdsp.tdsp(shm_par, par['num_zones'], par['num_time_steps'])
    
    log.info('--- read trips ---')
    # trips = pd.read_csv(par['trip_file'], skiprows=1)
    # trips.set_index(['ORG','DES','PERIOD'], inplace=True) 
    trips = pd.read_csv(par['trip_file'])
    trips.set_index(['I','J','period'], inplace=True)
    
    trips = pd.DataFrame(trips.stack())
    trips.reset_index(inplace=True)
    trips.columns = ['I','J','period','class','trip'] 
    trips.drop(trips[trips.trip == 0].index, inplace=True)
    # trips = trips[:100000]
    if multi_p:
        #send task in batch
        log.info('--- distribute tasks ---')
        num_trips = len(trips)
        start = 0
        end = par['task_size']
        while start <= num_trips:
            if end <= num_trips:
                tend = end
            else:
                tend = num_trips
            task = trips[start:tend]

            q.put(task)
            start = end
            end = start + par['task_size']
            
        for i in range(par['num_processor']):
            end_task = pd.DataFrame([[]])
            q.put(end_task)
    else:
        # single processor
        log.info('--- run tasks ---')
        for task in trips.to_numpy(): 
            t = [task[0], task[1], task[2], np.array(task[3]), np.array(task[4])]
            # t = [1, 1, 'AUTOVOT1', np.array([549,551,25,49,229,619]), np.array([0.3,0.1,0.3,0.1,0.3,0.1])]
            # t = [9, 1, 'AUTOVOT1', np.array([571,619]), np.array([0.3,0.1])]
            nodes = sp.build(t) 
            sp.trace(t, nodes) 
            
        # vol = sp.get_vol()

    if multi_p:
        for p in processes:
            p.join()
        
        while not r.empty():
            log.info("{0}".format(r.get()))
        
        shm_vol.unlink()
        shm_node.unlink()
        shm_link.unlink()
    t2 = timeit.default_timer()

    col_name = []
    for i in range( par['num_time_steps']):
        col_name.append('vol'+str(i+1))
    df_arr = pd.DataFrame(ndarr_vol,columns=col_name)
    df_vol = pd.concat([df_ab, df_arr], axis=1)
    df_vol.to_csv('vol.csv', index=False)

    log.info(f"Run time {t2 - t1:0.2f} seconds")   
    log.info('//--- PyDTA end ---//')
