# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 12:47:44 2021

@author: lihe.wang
"""

from cymem.cymem cimport Pool
from cpython cimport array
cimport typedef as td
    
cdef class heap:
    cdef unsigned int size, FRONT
    cdef array.array ndpos 
    cdef td.node** minheap  #minheap stores pointers of node
    cdef Pool mem
    
    cdef swap(self, int fpos, int spos)
    cdef bubble_down(self, int pos)
    cdef bubble_up(self, int pos)
    cdef insert(self, td.node *nd)
    cdef td.node* pop(self)
    cdef increase_priority(self, td.node *nd)
    cdef bint is_empty(self)
    cdef print_heap(self)