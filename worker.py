# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 15:45:34 2021

@author: lihe.wang
"""

import yaml as ym
import tdsp

from multiprocessing import Process

class worker(Process):
    
    def __init__(self, job_queue, r_queue, i):
        super(worker, self).__init__()
        with open(r'..\Data\control.yaml') as file:
            par = ym.full_load(file)
        self.i = i    
        self.num_zones = par['num_processor']
        self.node_file = par['node_file']
        self.link_file = par['link_file']

        self.job_queue = job_queue
        self.r_queue = r_queue
        
        
    def run(self):
        self.r_queue.put('processor ' + str(self.i))
        d = 0
        sp = tdsp.tdsp(self.node_file, self.link_file, self.num_zones)
        while True :              
            tasks = self.job_queue.get()
            
            for task in tasks:
                if task[0] == 'Done':
                    d = 1
                    break
                sp.build(task) 
                t, d = sp.trace(task)
                
            if d == 1:
                break
                # self.r_queue.put('task time ' + str(t))
            # self.job_queue.task_done()

             
        
        # for task in iter(self.job_queue.get(), 'DONE'):
        #     try:
        #         sp.build(task) 
        #         sp.trace(task)
        #     except Exception as e:
        #         logger.error(e)
        #     finally:
        #         self.job_queue.task_done()
      
                     

