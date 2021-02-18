# PyDTA
DTA in Python.
Macroscopic Dynamic Traffic Assignment.
Fast and multiprocessing using Cython, Pandas, and Numpy.

This package provides building blocks for DTA model developers who want to build customized Dynamic Traffic Assignment (DTA) models. Note that this is a macroscopic application of DTA model. It still uses time-delay function to calculate congested link travel time as the static assignment model does. The key feature of this DTA model is the Time Dependent Shortest Path (TDSP). 

Traffic assingment model is known for its computationally intensive needs mostly because of the shortest path building algorithm. The Dijkistra algorithm in this package is implemented in Cython, which gives C/C++ equivalent performance with Python style code.
