# Build and install darshan-util

PREFIX=$PWD/devenv/libdarshanutil

cd ../
./configure --prefix=${PREFIX} --enable-shared
make install
make distclean


echo
echo
echo export PATH=$PREFIX/bin:\$PATH
echo export LD_LIBRARY_PATH=$PREFIX/lib:\$LD_LIBRARY_PATH
