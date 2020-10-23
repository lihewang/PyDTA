# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 09:14:22 2020

@author: lihe.wang
python -m cProfile -o pf.txt tdsp_py.py
cprofilev -f pf.txt
"""

import numpy as np
import pandas as pd
import heap as hp
import timeit

#Time Dependent Shortest Path
class tdsp:

    def __init__(self, num_nodes, next_links, link_attribute, args):
        #Node impedance and index
        self.num_nodes = num_nodes
        self.node_imp = np.array(np.full(self.num_nodes,args[1]), dtype='f') 
        self.node_time = np.zeros(self.num_nodes, dtype=np.float32)
        self.node_dist = np.zeros(self.num_nodes, dtype=np.float32)
        self.node_toll = np.zeros(self.num_nodes, dtype=np.float32)
        self.my_heap = hp.heap(self.num_nodes, self.node_imp)
        self.next_links = next_links
        self.link_attribute = link_attribute
        self.args = args
        
    def build(self, sp_task):
        
        self.o_node_index = sp_task[0]
        self.d_node_index = sp_task[1]        
        self.start_ts = sp_task[2]
        self.node_imp[self.o_node_index] = 0
        self.my_heap.insert(self.o_node_index)
        self.parent_node = np.array(np.full(self.num_nodes,-1), dtype='i')
        self.popped = np.zeros(self.num_nodes, dtype='i')
        
        while not self.my_heap.is_empty():
            self.top_node_index = self.my_heap.pop()
            #print('pop node ' + str(self.top_node_index))
            self.popped[self.top_node_index] = 1     
            #Find destination
            if self.top_node_index == self.d_node_index:
                break
            
            #Calculate time step
            self.curr_ts = int(self.node_time[self.top_node_index]//15) + self.start_ts
            if self.curr_ts > 96:
                self.curr_ts -= 96
            self.curr_ts_col = self.curr_ts + 3
            
            #Next links index                  
            self.nxlinks = self.next_links[self.top_node_index]
            for self.i in self.nxlinks[0]:
                #self.i = self.link
                self.b_node = int(self.link_attribute[self.i, 1])
                self.t = self.link_attribute[self.i, self.curr_ts_col]
                self.imp = self.t + self.args[2] * self.link_attribute[self.i, 3]
                #Path can't be backward 
                if self.b_node == self.top_node_index or self.popped[self.b_node] == 1:
                    continue
                #print('B node ' + str(self.b_node))
                #Update node impedance
                self.new_imp = self.node_imp[self.top_node_index] + self.imp
                if self.new_imp < self.node_imp[self.b_node]:
                    if self.node_imp[self.b_node] == self.args[1]:
                        self.my_heap.insert(self.b_node)
                        #print('insert node ' + str(self.b_node))
                    else:
                        self.my_heap.increase_priority(self.b_node)
                        #print('heapify node ' + str(self.b_node))
                    self.node_imp[self.b_node] = self.new_imp
                    self.node_time[self.b_node] = self.node_time[self.top_node_index] + self.t
                    self.node_dist[self.b_node] = self.node_dist[self.top_node_index] + self.link_attribute[self.i, 2]
                    self.node_toll[self.b_node] = self.node_toll[self.top_node_index] + self.link_attribute[self.i, 3]
                    self.parent_node[self.b_node] = self.top_node_index
                    
                #zones can't be transpassed    
                if self.top_node_index <= self.args[0] and self.top_node_index != self.o_node_index:
                    continue

        return self.parent_node 

    def trace(self, sp_task, parent_node):
        self.path = np.zeros(self.num_nodes, dtype='i')
        self.path_size = 0
        self.d_node_index = sp_task[1]
        self.curr_node = self.d_node_index
        while self.curr_node != -1:
            self.path_size += 1
            self.path[self.path_size-1] = self.curr_node
            self.curr_node = parent_node[self.curr_node]
        
        self.path_nodes = np.flip(self.path[:self.path_size])
        self.path_skim = pd.DataFrame({'time':self.node_time[self.path_nodes], 
                                       'dist':self.node_dist[self.path_nodes], 
                                       'toll':self.node_toll[self.path_nodes]}, 
                                       index=self.path_nodes)
        return self.path_skim
    
#Test code    
if __name__ == "__main__":
      
    link = pd.read_csv("Data/LINK.CSV")
    node = pd.read_csv("Data/NODE.CSV")

    sp_task = pd.Series({'O':1, 'D':605, 'TS':12, 'Type':0, 'Cost':0})
    args = pd.Series({'num_zones':363, 'max_imp':9999.9, 'VOT_factor':0}, dtype='f')
      
    #Node id to node index look up
    node_index = pd.Series(node.index, index=node['N'], name='node_index')
    index_node = pd.Series(node['N'], index=node.index, name='node_number')
    d_node = node[node['DTA_Type'].isin([90,91])][['DTA_Type', 'DNGRP']]    #Decision nodes
    
    link_attribute = link[['DISTANCE','TOLL']]
    link_attribute.insert(0, column='B', value=node_index[link['B']].values)
    link_attribute.insert(0, column='A', value=node_index[link['A']].values)
    #Link's AB node and link index
    index_a = pd.Series(node_index[link['A']].values, index=link.index)
    index_b = pd.Series(node_index[link['B']].values, index=link.index)
    link_AB = pd.DataFrame({'link_index':link.index},index=index_a.astype(str)+'_'+index_b.astype(str))
    df_index_a = pd.DataFrame({'anode':index_a, 'link':index_a.index})
    a_link = df_index_a.groupby('anode')['link'].apply(list)
    
    next_links = pd.DataFrame(range(len(node_index))).join(a_link).drop(columns=0)
    for row in next_links.loc[next_links.link.isnull(), 'link'].index:
        next_links.at[row, 'link'] = []

    #Create time for 96 time steps
    time = link['DISTANCE']/link['FFSPEED']*60
    for i in range(0,96):
        link_attribute.loc[:,'time'+str(i+1)] = time
    
    t1 = timeit.default_timer()
    my_sp = tdsp(len(node_index), next_links.to_numpy(), link_attribute.to_numpy(dtype='f'), args.to_numpy())
    sp_task['O'] = node_index[sp_task['O']]
    sp_task['D'] = node_index[sp_task['D']]
    #build path
    parent_node = my_sp.build(sp_task.to_numpy())         
    path = my_sp.trace(sp_task.to_numpy(), parent_node)
    
    t2 = timeit.default_timer() 
    d_node_in_path = path[path.index.isin(d_node.index)]
    if not d_node_in_path.empty:
        first_d_node_loc = np.where(path.index == d_node_in_path.index[0])[0][0]
        path_to_first_d_node_a = path.index[:first_d_node_loc]
        path_to_first_d_node_b = path.index[1:first_d_node_loc+1]
        ab = path_to_first_d_node_a.astype(str)+'_'+path_to_first_d_node_b.astype(str)
        link_update = link_AB[link_AB.index.isin(ab)]
   
    print('Node_index Node_ID')
    print(index_node[path.index]) 
    print('Decision nodes')
    print(index_node[d_node_in_path.index])
    print('time ' + str(path['time'][sp_task[1]]))
    print('distance ' + str(path['dist'][sp_task[1]]))
    print('toll ' + str(path['toll'][sp_task[1]]))    
    print(f"Run time is {t2 - t1:0.6f} seconds")


