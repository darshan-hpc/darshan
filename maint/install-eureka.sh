#!/bin/bash

PREFIX=/soft/apps/unsupported/darshan-1.1.0

#darshan 
cd ../
install -d $PREFIX
install -d $PREFIX/bin
install -d $PREFIX/lib
install -d $PREFIX/lib/TeX
install -d $PREFIX/share
install -m 755 util/bin/darshan-job-summary.pl $PREFIX/bin/
install -m 644 util/lib/TeX/Encode.pm $PREFIX/lib/TeX/
install -m 644 util/share/* $PREFIX/share/

#gnuplot (new version with additional histogram support)
cd extern
tar -xvzf gnuplot-4.2.4.tar.gz
cd gnuplot-4.2.4
./configure --prefix $PREFIX && make &&  make install

