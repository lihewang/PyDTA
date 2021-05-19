import yaml as ym
import numpy as np

with open('control.yaml') as file:
    par = ym.full_load(file)

trip_cls = list(par['vot'].keys())

vol = np.zeros((96,10))
mv = memoryview(vol)

print(len(trip_cls))
print(trip_cls.index('RES_H'))
