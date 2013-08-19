#!/bin/sh

#
# (C) 2013 by Argonne National Laboratory.
#     See COPYRIGHT in top-level directory.
#

if [ $# -ne 2 ]; then
    echo "Usage: darshan-summary-per-file.sh <input_file.gz> <output_directory>"
    exit 1
fi

mkdir $2
rc=$?
if [ $rc -ne 0 ]; then
   exit $rc
fi

darshan-parser --file-list $1| egrep -v '^(#|$)' | 
while read -r hash suffix stuff ; do
	file=$(basename $suffix)
	if [ -x $file.gz ] ; then
		$file = $file.$hash.gz
	fi
	darshan-convert --file $hash $1 $2/$file.gz
	darshan-job-summary.pl $2/$file.gz --output $2/$file.pdf
done 
