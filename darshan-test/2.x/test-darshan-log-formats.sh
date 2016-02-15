#!/bin/bash 
#
# Test darshan-parser/darshan-job-summary.pl on the previous versions
# of the darshan log format.
#

LOGS=../example-output
RUNLOG=/tmp/run.log
PATH=../:${PATH}
namelist=()

for log in $(ls ${LOGS});
do
    if [ $log = 'README.txt' ];
    then
        continue;
    fi

    name=${log/.gz/.pdf}
    namelist=(${namelist[*]} $name)
    ../util/bin/darshan-job-summary.pl --output=${LOGS}/$name ${LOGS}/$log >> $RUNLOG 2>&1
    rc=$?
    if [ $rc -ne 0 ];
    then
        echo "failed: $rc : $log";
    fi
done

for name in ${namelist[*]};
do
    if [ -f ${LOGS}/$name ];
    then
        xpdf ${LOGS}/$name;
        rm ${LOGS}/$name;
    else
        echo "summary not found: $name";
    fi
done
