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
    log.info('### start ###') 
    
    multi_p = True
    # multi_p = False
        
    t1 = timeit.default_timer()
    
    # read network files to shared memory
    shm_node, shm_link, node_shape, node_type, link_shape, link_type = read(par['node_file'], par['link_file'])

    if multi_p:
        #start multiprocessors
        processes = []
        q = Queue()
        r = Queue()
        df_link = pd.read_csv(par['link_file'])
        arr = np.zeros((len(df_link), par['num_time_steps']))
        shm = shared_memory.SharedMemory(name='shared_vol', create=True, size=arr.nbytes)
        # arr = Array('d', len(df_link)*par['num_time_steps'])
        # ndarr = np.frombuffer(arr.get_obj()).reshape((len(df_link), par['num_time_steps']))
        ndarr = np.ndarray(arr.shape, dtype=arr.dtype, buffer=shm.buf)
        ndarr[:] = arr[:]
        for i in range(par['num_processor']):
            p=Process(target=worker, args=(q, r, i, node_shape, node_type, link_shape, link_type))
            p.start()
            processes.append(p)  
    else:
        sp = tdsp.tdsp(par['node_file'], par['link_file'], par['num_zones'], par['num_time_steps'])
    
    trips = pd.read_csv(par['trip_file'], skiprows=1)
    trips.set_index(['ORG','DES','PERIOD'], inplace=True) 
    # trips = pd.read_csv(par['trip_file'])
    # trips.set_index(['I','J','period'], inplace=True)
    
    trips = pd.DataFrame(trips.stack())
    trips.reset_index(inplace=True)
    trips.columns = ['I','J','period','class','trip'] 
    num_trip_table = len(trips)
    trips = trips[:100000]

    if multi_p:
        #send task in batch
        num_trips = len(trips)
        start = 0
        end = par['task_size']
        while start <= num_trips:
            if end <= num_trips:
                tend = end
            else:
                tend = num_trips
            task = trips[start:tend].to_numpy()
            q.put(task)
            start = end
            end = start + par['task_size']
            
        for i in range(par['num_processor']):
            q.put([['Done']])
    else:
        # single processor
        for task in trips.to_numpy(): 
            # task = [1,3,1,'auto',0.12]
            sp.build(task) 
            sp.trace(task) 
            
        vol = sp.get_vol()

    if multi_p:
        for p in processes:
            p.join()
        
        while not r.empty():
            log.info("{0}".format(r.get()))
        
        shm.unlink()
        shm_node.unlink()
        shm_link.unlink()
    t2 = timeit.default_timer()
    np.savetxt("vol.csv", ndarr, delimiter=",")
    log.info(f"Run time {t2 - t1:0.2f} seconds")   
    log.info('### end of run ###')
