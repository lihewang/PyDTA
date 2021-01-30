# -*- coding: utf-8 -*-
#cython: language_level=3
"""
Created on Wed Jan 27 09:13:49 2021

@author: lihe.wang
"""

from cymem.cymem cimport Pool
cimport typedef as td
import pandas as pd
import numpy as np
import timeit
cimport heap as hp

cdef class tdsp:
    cdef td.node* nodes
    cdef td.link* links
    cdef Pool mem
    cdef object node_index
    cdef int num_nodes 
    
    def __init__(self, nodefile, linkfile):
        t1 = timeit.default_timer()        
        cdef int i, link_index
        cdef int num_time_steps = 96
        cdef double t
        self.mem = Pool()
        
        df_node = pd.read_csv(nodefile)
        df_link = pd.read_csv(linkfile)
        
        self.num_nodes = len(df_node)
        self.nodes = <td.node*>self.mem.alloc(len(df_node), sizeof(td.node))
        self.links = <td.link*>self.mem.alloc(len(df_link), sizeof(td.link))
        
        #--- create node struct ---
        #use node index internally to identify nodes
        print('create node struct')
        self.node_index = pd.Series(df_node.index, index=df_node['N'], name='nd_index')
        #link's a_node index
        index_a = pd.Series(self.node_index[df_link['A']].values, index=df_link.index)
        #link index of a_node
        df_index_a = pd.DataFrame({'anode':index_a, 'link':index_a.index})
        df_next_links = df_index_a.groupby('anode')['link'].apply(list)    

        for index, row in df_node.iterrows(): 
            self.nodes[index].ni = index
            self.nodes[index].n = row['N']
            self.nodes[index].imp = 99999.9
            self.nodes[index].popped = 0
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
        print(f"Run time for node struct is {t2 - t1:0.6f} seconds")
            
        t1 = timeit.default_timer()       
        #--- create link struct ---  
        print('create link struct')
        #ffspeed can't be zero        
        df_link['FFSPEED'] = df_link['FFSPEED'].replace(0, 25)  
        #loop links
        for index, row in df_link.iterrows():
            self.links[index].ai = self.node_index[row['A']]
            self.links[index].bi = self.node_index[row['B']]
            self.links[index].a = row['A']
            self.links[index].b = row['B']
            self.links[index].link_id = index
            
            #time for time steps
            self.links[index].time = <double*>self.mem.alloc(num_time_steps, sizeof(double))
            t = row['DISTANCE']/row['FFSPEED']*60       
            for i in range(num_time_steps):
                self.links[index].time[i] = t
              
        t2 = timeit.default_timer()  
        print(f"Run time for link struct is {t2 - t1:0.6f} seconds")

    #build path    
    cpdef build(self, sp_task):
        print('build path')
        cdef hp.heap my_heap = hp.heap(self.num_nodes+1)
        cdef int o_node_index, d_node_index, start_ts, curr_ts, i, j
        cdef td.node *top_node
        cdef td.node *b_node
        cdef td.link *nxlink
        cdef double link_time, imp, new_imp
        
        o_node_index = self.node_index[sp_task[0]]
        d_node_index = self.node_index[sp_task[1]]        
        start_ts = sp_task[2]
        self.nodes[o_node_index].imp = 0
        self.nodes[o_node_index].parent = NULL
        my_heap.insert(&self.nodes[o_node_index])
        
        #for j in range(10):
        while not my_heap.is_empty():
            top_node = my_heap.pop()
            #print('pop node ' + str(top_node.n))
            top_node.popped = 1
            if top_node.ni == d_node_index:
                break
            #zones can't be transpassed    
            if top_node.n <= 4800 and top_node.ni != o_node_index:
                continue
            
            #Calculate time step
            curr_ts = int(top_node.node_time/15) + start_ts
            if curr_ts > 96:
                curr_ts -= 96
            
            #Next links index           
            for i in range(top_node.num_nxlinks):
                nxlink = top_node.nxt_links[i]
                b_node = &self.nodes[nxlink.bi]
                #Path can't be backward
                if b_node == top_node or b_node.popped == 1:
                    continue
                #print('B node ' + str(b_node.n))
                link_time = nxlink.time[curr_ts]
                imp = link_time + 0.2 * nxlink.toll
                
                #Update node impedance
                new_imp = top_node.imp + imp
                if new_imp < b_node.imp:
                    if b_node.imp == 99999.9:
                        my_heap.insert(b_node)
                        #print('insert node ' + str(b_node.n))
                    else:
                        my_heap.increase_priority(b_node)
                        #print('lower imp for node ' + str(b_node.n))
                    b_node.imp = new_imp
                    b_node.time = top_node.time + link_time
                    b_node.dist = top_node.dist + nxlink.dist
                    b_node.toll = top_node.toll + nxlink.toll
                    b_node.parent = top_node
    
    #trace path
    cpdef trace(self, sp_task):
        cdef int d_node_index, path_size
        cdef td.node *curr_node
        
        path = np.zeros(self.num_nodes, dtype='i')
        path_size = 0
        d_node_index = self.node_index[sp_task[1]]
        curr_node = &self.nodes[d_node_index]
        while curr_node != NULL:
            path_size += 1
            path[path_size-1] = curr_node.n
            curr_node = curr_node.parent
        
        path_nodes = np.flip(path[:path_size])
        print(path_nodes)
        # path_skim = pd.DataFrame({'time':self.node_time[self.path_nodes], 
        #                                'dist':self.node_dist[self.path_nodes], 
        #                                'toll':self.node_toll[self.path_nodes]}, 
        #                                index=self.path_nodes)
        # return path_skim
        
        # #test
        # cdef int node_idx = self.node_index[17193], ilink
        # for ilink in range(self.nodes[node_idx].num_nxlinks):
        #     print('node ' + str(self.nodes[node_idx].n) + ' index ' + str(node_idx) + ' next link anode ' + 
        #           str(self.nodes[node_idx].nxt_links[ilink].a) + ' bnode ' + str(self.nodes[node_idx].nxt_links[ilink].b))

        