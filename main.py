# -*- coding: utf-8 -*-
#cython: language_level=3
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
from read_ctl_s3 import read_control
from read_network_snowflake import read_network
from read_trips_snowflake import read_trips
from write_output_snowflake import save_vol
import worker
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

    par = read_control()    
    # with open('app/control.yaml') as file:
    #    par = ym.full_load(file)
      
    #set logging
    log = set_log(par['log_file'])    
    log.info('//--- PyDTA start ---//') 
    
    multi_p = True     
    t1 = timeit.default_timer()
    
    # read network files to shared memory
    shm, shm_par  = read_network(par['data_base'], par['node_file'], par['link_file'])   
    shm_node = shm[0] 
    shm_link = shm[1]
    # create shared memory for volume    
    link_shape = shm_par[2]
    link_type = shm_par[3]
    shm_link = shared_memory.SharedMemory(name='shared_link')
    shared_link = np.ndarray(link_shape, dtype=link_type, buffer=shm_link.buf)
    df_link = pd.DataFrame(shared_link.copy())
    log.info('--- retrieved network number of links ' + str(len(df_link)) + ' ---')
    df_link.columns = shm_par[5]
    df_ab = df_link[['A','B']] 
    df_cgspd = df_link[['CAPACITY', 'ALPHA', 'BETA']] 
    trip_cls = list(par['vot'].keys())      # get trip classes 
    arr_v = np.zeros((len(trip_cls), len(df_link), par['num_time_steps']))           
    shm_vol = shared_memory.SharedMemory(name='shared_vol', create=True, size=arr_v.nbytes)   
    ndarr_vol = np.ndarray(arr_v.shape, dtype=np.dtype(np.float32), buffer=shm_vol.buf)
    ndarr_vol[:] = arr_v[:]
    log.info('--- read ' + par['trip_file'] + ' from ' + par['data_base'] + ' ---')
    # trips = pd.read_csv('C:/Users/lihe.wang/Documents/PyDTA/Data/Regional/Trips.csv', skiprows=par['skip_rows'])
    trips = read_trips(par['data_base'], par['trip_file'], trip_cls)
    # trips = trips.head(2000)
    log.info('--- retrieved trips ' + str(len(trips)) + ' rows ---')
    trips.columns = par['trip_column_names']
    log.info('--- load trips to ' + str(par['num_processor']) + ' processors ---')
    iter = 1
    while iter <= par['max_iter']:
        #start multiprocessors
        processes = []
        q_task = Queue()
        q_rtn = Queue()

        for i in range(par['num_processor']):
            p=worker.worker(q_task, q_rtn, i, shm_par, par, iter)
            time.sleep(1.5)
            p.start()
            processes.append(p) 
            # log.info('processor ' + str(i) + ' started') 
   
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

        for p in processes:
            p.join()
        
        while not q_rtn.empty():
            log.info("{0}".format(q_rtn.get()))

        iter += 1

    shm_vol.unlink()
    shm_node.unlink()
    shm_link.unlink()
    # write outputs
    col_name = []
    for i in range(par['num_time_steps']):
        col_name.append('vol'+str(i+1))
    trip_cls = list(par['vot'].keys()) # get trip classes
    df_vol_tot = pd.DataFrame(np.zeros((len(df_link), par['num_time_steps'])), columns=col_name)
    for i in range(len(trip_cls)):
        df_arr = pd.DataFrame(ndarr_vol[i], columns=col_name)
        df_vol = pd.concat([df_ab, df_arr], axis=1)

        df_vol.to_csv('/output/Volume_' + trip_cls[i] + '.csv', index=False)
        save_vol(par['data_base'], par['num_time_steps'], trip_cls[i])
        df_vol_tot += df_arr

    vol_tot = pd.concat([df_ab, df_vol_tot], axis=1)
    vol_tot.to_csv('/output/Volume_total.csv', index=False)
    save_vol(par['data_base'], par['num_time_steps'], 'total')
    
    t2 = timeit.default_timer()
    log.info(f"Run time {t2 - t1:0.2f} seconds")   

    log.info('//--- PyDTA end ---//')
