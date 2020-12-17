#!/usr/bin/env python3

"""
A minimal example for a cli utility to wrap pydarshan functionality.
"""


import argparse
import darshan

def main(args=None):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input', help='darshan log file', nargs='?', default='ior_hdf5_example.darshan')
    parser.add_argument('--debug', help='', action='store_true')
    args = parser.parse_args()

    if args.debug:
        print(args)

    report = darshan.DarshanReport(args.input, read_all=True, dtype="numpy")
    print(report.to_json())


if __name__ == "__main__":
    main()
