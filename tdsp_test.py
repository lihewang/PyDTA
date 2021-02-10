# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
import pyximport; pyximport.install(reload_support=True)

import yaml as ym
import pandas as pd
import timeit
from multiprocessing import Queue
from worker import worker
import tdsp

      
if __name__ == "__main__":
    print('--- start ---') 
    multi_p = True
    # multi_p = False
    
    processes = []
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)
    
    t1 = timeit.default_timer()
    
    if multi_p:
        #start multiprocessors
        q = Queue()
        r = Queue()
        for i in range(par['num_processor']):
            p=worker(q, r, i)
            # p.daemon = True
            p.start()
            processes.append(p)  
    else:
        sp = tdsp.tdsp(par['node_file'], par['link_file'], par['num_zones'])
    
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
    
    if multi_p:
        for p in processes:
            p.join()
        
        while not r.empty():
            print ("RESULT: {0}".format(r.get()))
     
    t2 = timeit.default_timer()
    print(f"Run time: {t2 - t1:0.2f} seconds")   
    print('--- end of run ---')
