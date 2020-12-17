#!/usr/bin/env python3

import darshan
from darshan.experimental.plots.matplotlib import *

darshan.enable_experimental()

r = darshan.DarshanReport("ior_hdf5_example.darshan", dtype="numpy")
plot_opcounts(r).show()
