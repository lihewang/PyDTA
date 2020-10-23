# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 16:39:10 2020

@author: lihe.wang
"""


import numpy as np
import pandas as pd
import heap as hp
import timeit
import tdsp_py as sp

#Test code    
if __name__ == "__main__":
    link = pd.read_csv("Data/LINK.CSV")
    node = pd.read_csv("Data/NODE.CSV")

    sp_task = pd.Series({'O':4, 'D':603, 'TS':12, 'Type':0, 'Cost':0})
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
    my_sp = sp.tdsp(len(node_index), next_links.to_numpy(), link_attribute.to_numpy(dtype='f'), args.to_numpy())
    sp_task['O'] = node_index[sp_task['O']]
    sp_task['D'] = node_index[sp_task['D']]
    
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