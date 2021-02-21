# -*- coding: utf-8 -*-
#cython: language_level=3
"""
Created on Wed Jan 27 09:13:49 2021

@author: lihe.wang
"""
import logging
from cymem.cymem cimport Pool
cimport typedef as td
import pandas as pd
import numpy as np
import timeit
import time
cimport heap as hp
from multiprocessing import shared_memory

cdef class tdsp:
    cdef td.node* nodes
    cdef td.link* links
    cdef Pool mem
    cdef object node_index
    cdef int num_nodes 
    cdef int num_zones
    cdef int num_links
    cdef int num_time_steps

    def __init__(self, shm_par, num_zones, num_time_steps):
        log = logging.getLogger(__name__)
        
        t1 = timeit.default_timer()        
        cdef int i, link_index
        cdef double t
        self.mem = Pool()
        self.num_zones = num_zones
        self.num_time_steps = num_time_steps
        
        #get nodes and links from shared memory
        node_shape = shm_par[0]
        node_type = shm_par[1]
        link_shape = shm_par[2]
        link_type = shm_par[3]
  
        shm_node = shared_memory.SharedMemory(name='shared_node')
        shared_node = np.ndarray(node_shape, dtype=node_type, buffer=shm_node.buf)
        df_node = pd.DataFrame(shared_node.copy())
        shm_link = shared_memory.SharedMemory(name='shared_link')
        shared_link = np.ndarray(link_shape, dtype=link_type, buffer=shm_link.buf)
        df_link = pd.DataFrame(shared_link.copy())

        df_node.columns = shm_par[4]
        df_link.columns = shm_par[5]

        self.num_nodes = len(df_node)
        self.num_links = len(df_link)
        self.nodes = <td.node*>self.mem.alloc(self.num_nodes, sizeof(td.node))
        self.links = <td.link*>self.mem.alloc(self.num_links, sizeof(td.link))
        
        #--- create node struct ---
        #use node index internally to identify nodes
        log.info('create node struct')
        self.node_index = pd.Series(df_node.index, index=df_node['N'], name='nd_index')
        #link's a_node index
        index_a = pd.Series(self.node_index[df_link['A']].values, index=df_link.index)
        #link index of a_node
        df_index_a = pd.DataFrame({'anode':index_a, 'link':index_a.index})
        df_next_links = df_index_a.groupby('anode')['link'].apply(list)    

        for index, row in df_node.iterrows(): 
            self.nodes[index].ni = index
            self.nodes[index].n = row['N']

            if index in df_next_links:
                list_next_links = df_next_links[index]
            else:
                list_next_links = []
            
            self.nodes[index].num_nxlinks = len(list_next_links)
            self.nodes[index].nxt_links = <td.link**>self.mem.alloc(len(list_next_links), sizeof(td.link*))
            
            i = 0
            for link_index in list_next_links:            
                self.nodes[index].nxt_links[i] = &self.links[link_index]
                i += 1
        
        t2 = timeit.default_timer()  
        log.info(f"Run time for node struct is {t2 - t1:0.2f} seconds")
            
        t1 = timeit.default_timer()       
        # --- create link struct ---  
        log.info('create link struct')
        #ffspeed can't be zero        
        df_link['FFSPEED'] = df_link['FFSPEED'].replace(0, 25)  
        # loop links
        for index, row in df_link.iterrows():
            self.links[index].ai = self.node_index[row['A']]
            self.links[index].bi = self.node_index[row['B']]
            self.links[index].a = row['A']
            self.links[index].b = row['B']

            self.links[index].dist = row['DISTANCE']
            self.links[index].ffspd = row['FFSPEED']
            #time for time steps
            self.links[index].time = <double*>self.mem.alloc(num_time_steps, sizeof(double))
            self.links[index].vol = <double*>self.mem.alloc(num_time_steps, sizeof(double))
            t = row['DISTANCE']/row['FFSPEED']*60       
            for i in range(num_time_steps):
                self.links[index].time[i] = t
                self.links[index].vol[i] = 0

        shm_node.close() 
        shm_link.close()       
        t2 = timeit.default_timer()  
        log.info(f"Run time for link struct is {t2 - t1:0.2f} seconds")
        
    #build one-to-many shourtest path    
    cpdef build(self, sp_task):
        # print('--build path from ' + str(sp_task[0]) + ' period ' + str(sp_task[1]) + ' ts ' + str(sp_task[2]))
        cdef hp.heap my_heap = hp.heap(self.num_nodes+1)
        cdef int o_node_index, start_ts, curr_ts, i, j
        cdef td.node *top_node
        cdef td.node *b_node
        cdef td.node *nd
        cdef td.link *nxlink
        cdef double link_time, imp, new_imp, max_imp = 99999.9
        cdef int[:] d_nodes
        cdef double[:] trips

        o_node_index = self.node_index.get(sp_task[0], -1)
        if o_node_index == -1:
            return np.array([])
        d_nodes = sp_task[3]
        start_ts = sp_task[1]

        #initialize
        d_node_found = []
        for j in range(self.num_nodes):
            self.nodes[j].imp = max_imp
            self.nodes[j].popped = 0
            # self.nodes[j].time = 0
            # self.nodes[j].dist = 0
            # self.nodes[j].toll = 0
            self.nodes[j].parent = NULL
            
        self.nodes[o_node_index].imp = 0
        self.nodes[o_node_index].parent = NULL
        my_heap.insert(&self.nodes[o_node_index])

        while not my_heap.is_empty():                       #loop till all nodes have been visited
            top_node = my_heap.pop()
            top_node.popped = 1
            # print('--popped node ' + str(top_node.n) + ' imp ' + str(top_node.imp))
            # check if destination nodes have been found
            for j in range(len(d_nodes)):
                if top_node.n == d_nodes[j]:                #a destination node is found
                    d_node_found.append(top_node.n)
                    break
                if len(d_node_found) == len(d_nodes):       #all destination nodes are found
                    return np.array(d_node_found)

            #zones can't be transpassed    
            if top_node.n <= self.num_zones and top_node.ni != o_node_index:
                continue
            
            #Calculate time step
            curr_ts = int(top_node.node_time/15) + start_ts
            if curr_ts > 96:
                curr_ts -= 96
            
            #Next links index
            # for i in range(1):
            for i in range(top_node.num_nxlinks):
                nxlink = top_node.nxt_links[i]
                b_node = &self.nodes[nxlink.bi]
                # print('B node ' + str(b_node.n))
                # time.sleep(1)
                #Path can't be backward
                if b_node == top_node or b_node.popped == 1:
                    continue
                
                link_time = nxlink.time[curr_ts]
                imp = link_time #+ 0.2 * nxlink.toll
               
                #Update node impedance
                new_imp = top_node.imp + imp
                # print('new imp time ' + str(new_imp) + ' b node imp ' + str(b_node.imp))
                # time.sleep(1)
                if new_imp < b_node.imp:
                    if b_node.imp == max_imp:
                        b_node.imp = new_imp
                        # print('insert b node ' + str(b_node.n))
                        # time.sleep(1)
                        my_heap.insert(b_node)                        
                    else:
                        b_node.imp = new_imp
                        # print('increase priority ' + str(b_node.n))
                        # time.sleep(1)
                        my_heap.increase_priority(b_node)
                        # print('lower imp for node ' + str(b_node.n))
                        # time.sleep(1)

                    b_node.time = top_node.time + link_time
                    b_node.dist = top_node.dist + nxlink.dist
                    b_node.toll = top_node.toll + nxlink.toll
                    b_node.parent = top_node
                    b_node.parent_link = nxlink
                    b_node.ts = curr_ts

        #print('d nodes to build path') 
        #for j in range(len(d_nodes)):
        #    print(d_nodes[j])
        #print('d_node found ' + str(d_node_found))           
        return np.array(d_node_found)

    #trace path
    cpdef trace(self, sp_task, d_nodes_found):
        cdef int d_node_index, path_size
        cdef td.node *curr_node
        cdef int[:] d_nodes
        cdef double[:] trips
        
        d_nodes = sp_task[3]
        trips = sp_task[4]
        if len(d_nodes_found) == 0:
            return
        for j in range(len(d_nodes)):
            if (d_nodes[j] in d_nodes_found):
                d_node_index = self.node_index[d_nodes[j]]
                curr_node = &self.nodes[d_node_index]
                # load volume
                while curr_node.n != sp_task[0]:
                    curr_node.parent_link.vol[curr_node.ts-1] += trips[j]
                    curr_node = curr_node.parent

                # d_node = &self.nodes[d_node_index]    
                # print('path from ' + str(sp_task[0]) + ' to ' + str(d_node.n) + ' skim time ' + str(d_node.time) + ' dist ' + str(d_node.dist))
            else:
                print(d_nodes_found)
                print('path from ' + str(sp_task[0]) + ' to ' + str(d_nodes[j]) + ' not found')
        
        return (curr_node.time, curr_node.dist)
    
    cpdef get_vol(self):
        cdef int i
        cdef td.link *link
        
        vol = np.zeros((self.num_links, self.num_time_steps))

        for i in range(self.num_links):
            for j in range(self.num_time_steps):
                vol[i][j]=self.links[i].vol[j]  
        
        return vol

 