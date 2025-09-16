#!/usr/bin/env python3

from mpi4py import MPI
import numpy as np
import os

def main():
    comm = MPI.COMM_WORLD
    arr1 = np.arange(50)
    np.save('single_array.npy', arr1)
    os.remove('single_array.npy')


if __name__ == "__main__":
    main()
