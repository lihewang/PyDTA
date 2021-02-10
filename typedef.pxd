# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 14:32:08 2021

@author: lihe.wang
"""


cdef struct node:
    unsigned int n, ni, num_nxlinks
    bint popped
    double imp, time, dist, toll, node_time
    int ts
    link** nxt_links
    node* parent
    link* parent_link
    
cdef struct link:
    unsigned int a, b, ai, bi
    double dist, ffspd, toll
    double *time
    double *vol
