#!/bin/sh

startdir=$PWD

tmp=tmpd98d40iijej0

rm -rf $tmp
mkdir -p $tmp
cd $tmp

python3 -m venv venv
source venv/bin/activate

pip install darshan

echo ""
echo "PyDarshan version is:"
python -m darshan --version


echo ""
echo "Test a log can be succesfully parsed:"
python -m darshan info $startdir/../examples/example-logs/ior_hdf5_example.darshan
