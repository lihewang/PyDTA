# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
import pyximport; pyximport.install(reload_support=True)

import yaml as ym
import pandas as pd
import timeit
from multiprocessing import JoinableQueue
from worker import worker
import tdsp

      
if __name__ == "__main__":
    print('--- start ---') 
    multip = True
    # multip = False
    
    processes = []
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)
    
    t1 = timeit.default_timer()
    
    if multip:
        #start multiprocessors
        q = JoinableQueue()
        r = JoinableQueue()
        for i in range(par['num_processor']):
            p=worker(q, r, i)
            p.start()
            processes.append(p)  
    else:
        sp = tdsp.tdsp(par['node_file'], par['link_file'], par['num_zones'])
    
    trips = pd.read_csv(par['trip_file'], skiprows=1)
    trips.set_index(['ORG','DES','PERIOD'], inplace=True)    
    trips = pd.DataFrame(trips.stack())
    trips.reset_index(inplace=True)
    trips.columns = ['I','J','period','class','trip']   
    
    sp_task = trips[:100000].to_numpy()
    
    for task in sp_task:       
        if multip:
            # multiprocessors
            q.put(task)
        else:
            #single processor
            sp.build(task) 
            sp.trace(task)
    
    if multip:
        q.join()
        
        # while not r.empty():
        #     print ("RESULT: {0}".format(r.get()))
     
    t2 = timeit.default_timer()
    print(f"Run time: {t2 - t1:0.2f} seconds")   
    print('--- end of run ---')
