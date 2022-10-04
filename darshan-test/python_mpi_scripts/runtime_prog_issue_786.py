#!/usr/bin/env python

import os

def parent_child_io():
    # parent creates a file record before fork
    f = open("./pre-fork-parent", "w")
    f.close()

    pid = os.fork()
    if pid > 0:
        # parent process creates a file record after the fork
        f = open("./post-fork-parent", "w")
        f.close()
    else:
        # child process creates a file record after the fork
        f = open("./post-fork-child", "w")
        f.close()

if __name__ == "__main__":
    parent_child_io()
