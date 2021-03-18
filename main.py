# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 2021
@author: Lihe Wang  
Required Python package: cython, pandas, numpy, cymem, pyyaml
Python 3.8 or higher
Start of the program
"""

import pyximport; pyximport.install(reload_support=True)
import logging
import logging.config
import yaml as ym
import pandas as pd
import numpy as np
import timeit
import time
from multiprocessing import Process, Queue, shared_memory
import worker
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
        
    with open(r'..\Data\Bench\control.yaml') as file:
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
    link_shape = shm_par[2]
    link_type = shm_par[3]
    shm_link = shared_memory.SharedMemory(name='shared_link')
    shared_link = np.ndarray(link_shape, dtype=link_type, buffer=shm_link.buf)
    df_link = pd.DataFrame(shared_link.copy())
    df_link.columns = shm_par[5]
    df_ab = df_link[['A','B']] 
    df_cgspd = df_link[['CAPACITY', 'ALPHA', 'BETA']] 
       
    arr_v = np.zeros((len(df_link), par['num_time_steps']))           
    shm_vol = shared_memory.SharedMemory(name='shared_vol', create=True, size=arr_v.nbytes)
    ndarr_vol = np.ndarray(arr_v.shape, dtype=np.dtype(np.float32), buffer=shm_vol.buf)
    ndarr_vol[:] = arr_v[:]

    log.info('--- read trips ---')
    trips = pd.read_csv(par['trip_file'], skiprows=par['skip_rows'])
    '''
    trips.set_index(par['index_column_names'], inplace=True) 
    #stack trip classes    
    trips = pd.DataFrame(trips.stack())
    trips.reset_index(inplace=True)
    trips.columns = ['I','J','period','class','trip'] 
    #drop rows of zero trip and intrazonal trips
    trips.drop(trips[trips['trip'] == 0].index, inplace=True)
    trips.drop(trips[trips['I'] == trips['J']].index, inplace=True)
    # trips = trips[:20]
    '''
    iter = 1
    while iter <= par['max_iter']:
        if multi_p:
            #start multiprocessors
            processes = []
            q_task = Queue()
            q_rtn = Queue()

            for i in range(par['num_processor']):
                p=worker.worker(q_task, q_rtn, i, shm_par, par, iter)
                time.sleep(1.5)
                p.start()
                processes.append(p)  
        else:
            sp = tdsp.tdsp(shm_par, par['num_zones'], par['num_time_steps'])
    
        if multi_p:
            ndarr_vol *= float(1 - 1/iter)  #MSA
            #send task in batch
            log.info('--- iter ' + str(iter) + ': distribute tasks ---')
            num_trips = len(trips)
            start = 0
            end = par['task_size']
            while start <= num_trips:
                if end <= num_trips:
                    tend = end
                else:
                    tend = num_trips
                task = trips[start:tend]

                q_task.put(task)
                start = end
                end = start + par['task_size']

            for i in range(par['num_processor']):
                end_task = pd.DataFrame([[]])
                q_task.put(end_task)

        else:
            # single processor
            log.info('--- run tasks ---')
            for task in trips.to_numpy(): 
                t = [task[0], task[1], task[2], np.array(task[3]), np.array(task[4])]
                # t = [1, 1, 'AUTOVOT1', np.array([549,551,25,49,229,619]), np.array([0.3,0.1,0.3,0.1,0.3,0.1])]
                # t = [9, 1, 'AUTOVOT1', np.array([571,619]), np.array([0.3,0.1])]
                nodes = sp.build(t) 
                sp.trace(t, nodes) 

        for p in processes:
            p.join()
        
        while not q_rtn.empty():
            log.info("{0}".format(q_rtn.get()))

        iter += 1

    shm_vol.unlink()
    shm_node.unlink()
    shm_link.unlink()
    t2 = timeit.default_timer()

    col_name = []
    for i in range(par['num_time_steps']):
        col_name.append('vol'+str(i+1))
    df_arr = pd.DataFrame(ndarr_vol,columns=col_name)
    df_vol = pd.concat([df_ab, df_arr], axis=1)
    # congested time
    # for i in range( par['num_time_steps']):
    #     df_vol['t'+str(i+1)] = df_vol['FFTIME'] * (1+df_vol['ALPHA']*np.power((df_vol['vol'+str(i+1)]/df_vol['CAPACITY']*4),df_vol['BETA']))

    df_vol.to_csv('vol.csv', index=False)

    log.info(f"Run time {t2 - t1:0.2f} seconds")   
    log.info('//--- PyDTA end ---//')
