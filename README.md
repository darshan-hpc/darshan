Darshan is a lightweight I/O characterization tool that transparently
captures I/O access pattern information from HPC applications.
Darshan can be used to tune applications for increased scientific
productivity or to gain insight into trends in large-scale computing
systems.

Please see the 
[Darshan web page](http://www.mcs.anl.gov/research/projects/darshan)
for more in-depth news and documentation.

The Darshan source tree is divided into two main parts:

- darshan-runtime: to be installed on systems where you intend to 
  instrument MPI applications.  See darshan-runtime/doc/darshan-runtime.txt
  for installation instructions.

- darshan-util: to be installed on systems where you intend to analyze
  log files produced by darshan-runtime.  See
  darshan-util/doc/darshan-util.txt for installation instructions.

The darshan-test directory contains various test harnesses, benchmarks,
patches, and unsupported utilites that are mainly of interest to Darshan
developers.

