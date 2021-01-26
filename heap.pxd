# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 12:47:44 2021

@author: lihe.wang
"""

from cymem.cymem cimport Pool
from cpython cimport array
cdef struct node:
    unsigned int n
    double imp, time, dist, toll
    
cdef class heap:
    cdef unsigned int size, FRONT
    cdef array.array ndpos       
    cdef node** minheap  #minheap stores pointers of node
    cdef Pool mem
    
    cdef swap(self, int fpos, int spos)
    cdef bubble_down(self, int pos)
    cdef bubble_up(self, int pos)
    cdef insert(self, node *nd)
    cdef node* pop(self)
    cdef increase_priority(self, node *nd)