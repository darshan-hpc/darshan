#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

TODAY_DATE_PATH=`date "+%Y/%-m/%-d"`
TST_DARSHAN_LOG_PATH="${TST_DARSHAN_LOG_PATH}/${TODAY_DATE_PATH}"
mkdir -p ${TST_DARSHAN_LOG_PATH}

# check what file system is used
echo "df -T ${TST_DARSHAN_LOG_PATH}"
df -T ${TST_DARSHAN_LOG_PATH}

echo "findmnt -n -o FSTYPE --target ${TST_DARSHAN_LOG_PATH}"
findmnt -n -o FSTYPE --target ${TST_DARSHAN_LOG_PATH}

if test "x$USERNAME_ENV" = xno ; then
   USERNAME_ENV=$USER
fi

if test -f $DARSHAN_INSTALL_DIR/bin/darshan-parser ; then
   DARSHAN_PARSER=$DARSHAN_INSTALL_DIR/bin/darshan-parser
else
   DARSHAN_PARSER=../../darshan-util/darshan-parser
fi
echo "DARSHAN_PARSER=$DARSHAN_PARSER"

if test -f $DARSHAN_INSTALL_DIR/bin/darshan-config ; then
   DARSHAN_CONFIG=$DARSHAN_INSTALL_DIR/bin/darshan-config
else
   DARSHAN_CONFIG=../../darshan-util/darshan-config
fi
echo "DARSHAN_CONFIG=$DARSHAN_CONFIG"

$DARSHAN_CONFIG --all

# run NP number of MPI processes
# Note when using OpenMPI, setting NP > 2 will fail.
if test "x$NP" = x ; then
   NP=2
fi

TEST_FILE=./testfile.dat

# tst_mpi_io.c takes the following command-line options.
#        [-i] test read API
#        [-c] test collective API
#        [-a] test asynchronous API
#        [-s] test shared API
#        [-p] test split API
#        [-o] test ordered API
#        [-x] test explicit offset API
#        [-l] test large-count API

if test "x$HAVE_MPI_LARGE_COUNT" = x1 ; then
   OPTS="c a s p o x l"
else
   OPTS="c a s p o x"
fi

list=($OPTS)
nelems=${#list[@]}

clear_opt() {
    has_c=no
    has_a=no
    has_s=no
    has_p=no
    has_o=no
    has_x=no
    do_skip=no
}

check_opt() {
    if test $1 = c ; then has_c=yes ; fi
    if test $1 = a ; then has_a=yes ; fi
    if test $1 = s ; then has_s=yes ; fi
    if test $1 = p ; then has_p=yes ; fi
    if test $1 = o ; then has_o=yes ; fi
    if test $1 = x ; then has_x=yes ; fi
}

check_skip_opt() {
    if test $has_c = yes && (test $has_s = yes ||
                             test $has_o = yes) ; then
        # collective has no shared, order APIs
        do_skip=yes
    elif test $has_a = yes && (test $has_o = yes ||
                               test $has_p = yes) ; then
        # async has no order, split APIs
        do_skip=yes
    elif test $has_s = yes && (test $has_c = yes ||
                               test $has_p = yes ||
                               test $has_o = yes ||
                               test $has_x = yes) ; then
        # shared has no collective, split, order, at APIs
        do_skip=yes
    elif test $has_p = yes && (test $has_c = yes ||
                               test $has_a = yes ||
                               test $has_s = yes) ; then
        # split has no collective, async, shared APIs
        do_skip=yes
    elif test $has_o = yes && (test $has_c = yes ||
                               test $has_a = yes ||
                               test $has_s = yes ||
                               test $has_x = yes) ; then
        # order has no collective, async, shared, at APIs
        do_skip=yes
    elif test $has_x = yes && (test $has_s = yes ||
                               test $has_o = yes) ; then
        # at has no shared, ordered APIs
        do_skip=yes
    fi
}

for ((i=0; i < nelems; i++)); do
for ((j=i+1; j < nelems; j++)); do
    clear_opt
    check_opt ${list[$i]}
    check_opt ${list[$j]}

    check_skip_opt
    if test $do_skip = yes ; then
       continue
    fi
    # echo "${list[$i]}${list[$j]}"
    OPTS="$OPTS ${list[$i]}${list[$j]}"
done
done

for ((i=0; i < nelems; i++)); do
for ((j=i+1; j < nelems; j++)); do
for ((k=j+1; k < nelems; k++)); do
    clear_opt
    check_opt ${list[$i]}
    check_opt ${list[$j]}
    check_opt ${list[$k]}

    check_skip_opt
    if test $do_skip = yes ; then
       continue
    fi
    # echo "${list[$i]}${list[$j]}${list[$k]}${list[$l]}"
    OPTS="$OPTS ${list[$i]}${list[$j]}${list[$k]}"
done
done
done

for ((i=0; i < nelems; i++)); do
for ((j=i+1; j < nelems; j++)); do
for ((k=j+1; k < nelems; k++)); do
for ((l=k+1; l < nelems; l++)); do
    clear_opt
    check_opt ${list[$i]}
    check_opt ${list[$j]}
    check_opt ${list[$k]}
    check_opt ${list[$l]}

    check_skip_opt
    if test $do_skip = yes ; then
       continue
    fi
    # echo "${list[$i]}${list[$j]}${list[$k]}${list[$l]}"
    OPTS="$OPTS ${list[$i]}${list[$j]}${list[$k]}${list[$l]}"
done
done
done
done

for ((i=0; i < nelems; i++)); do
for ((j=i+1; j < nelems; j++)); do
for ((k=j+1; k < nelems; k++)); do
for ((l=k+1; l < nelems; l++)); do
for ((m=l+1; m < nelems; m++)); do
    clear_opt
    check_opt ${list[$i]}
    check_opt ${list[$j]}
    check_opt ${list[$k]}
    check_opt ${list[$l]}
    check_opt ${list[$m]}

    check_skip_opt
    if test $do_skip = yes ; then
       continue
    fi
    # echo "${list[$i]}${list[$j]}${list[$k]}${list[$l]}${list[$m]}"
    OPTS="$OPTS ${list[$i]}${list[$j]}${list[$k]}${list[$l]}${list[$m]}"
done
done
done
done
done

for ((i=0; i < nelems; i++)); do
for ((j=i+1; j < nelems; j++)); do
for ((k=j+1; k < nelems; k++)); do
for ((l=k+1; l < nelems; l++)); do
for ((m=l+1; m < nelems; m++)); do
for ((n=m+1; n < nelems; n++)); do
    clear_opt
    check_opt ${list[$i]}
    check_opt ${list[$j]}
    check_opt ${list[$k]}
    check_opt ${list[$l]}
    check_opt ${list[$m]}
    check_opt ${list[$n]}

    check_skip_opt
    if test $do_skip = yes ; then
       continue
    fi
    # echo "${list[$i]}${list[$j]}${list[$k]}${list[$l]}${list[$m]}${list[$n]}"
    OPTS="$OPTS ${list[$i]}${list[$j]}${list[$k]}${list[$l]}${list[$m]}${list[$n]}"
done
done
done
done
done
done

for ((i=0; i < nelems; i++)); do
for ((j=i+1; j < nelems; j++)); do
for ((k=j+1; k < nelems; k++)); do
for ((l=k+1; l < nelems; l++)); do
for ((m=l+1; m < nelems; m++)); do
for ((n=m+1; n < nelems; n++)); do
for ((o=n+1; o < nelems; o++)); do
    clear_opt
    check_opt ${list[$i]}
    check_opt ${list[$j]}
    check_opt ${list[$k]}
    check_opt ${list[$l]}
    check_opt ${list[$m]}
    check_opt ${list[$n]}
    check_opt ${list[$o]}

    check_skip_opt
    if test $do_skip = yes ; then
       continue
    fi
    # echo "${list[$i]}${list[$j]}${list[$k]}${list[$l]}${list[$m]}${list[$n]}${list[$o]}"
    OPTS="$OPTS ${list[$i]}${list[$j]}${list[$k]}${list[$l]}${list[$m]}${list[$n]}${list[$o]}"
done
done
done
done
done
done
done

echo "OPTS=$OPTS"

if test -f $DARSHAN_INSTALL_DIR/lib/libdarshan.so ; then
   export LD_PRELOAD=$DARSHAN_INSTALL_DIR/lib/libdarshan.so
else
   export LD_PRELOAD=../lib/.libs/libdarshan.so
fi
echo "LD_PRELOAD=$LD_PRELOAD"

for exe in ${check_PROGRAMS} ; do

   if test "x$exe" = xtst_mpi_io ; then

      DARSHAN_LOG_FILE="${TST_DARSHAN_LOG_PATH}/${USERNAME_ENV}_${exe}*"
      echo "DARSHAN_LOG_FILE=$DARSHAN_LOG_FILE"

      DARSGAN_FIELD=MPIIO_BYTES_WRITTEN
      for opt in "" ${OPTS} ; do
          if test "x$opt" = x ; then
             CMD="${TESTMPIRUN} -n ${NP} ./$exe $TEST_FILE"
          else
             CMD="${TESTMPIRUN} -n ${NP} ./$exe -$opt $TEST_FILE"
          fi
          echo "CMD=$CMD"
          rm -f $TEST_FILE $DARSHAN_LOG_FILE
          $CMD

          echo "ls -l ${DARSHAN_LOG_FILE}"
          ls -l ${DARSHAN_LOG_FILE}

          echo "parsing ${DARSHAN_LOG_FILE}"
          EXPECT_NBYTE=`stat -c %s $TEST_FILE`
          nbytes=`$DARSHAN_PARSER ${DARSHAN_LOG_FILE} | grep $DARSGAN_FIELD | cut -f5`
          # echo "EXPECT_NBYTE=$EXPECT_NBYTE nbytes=$nbytes"
          if test "x$nbytes" != "x$EXPECT_NBYTE" ; then
             echo "Error: CMD=$CMD nbytes=$nbytes"
             exit 1
          fi
      done

      DARSGAN_FIELD=MPIIO_BYTES_READ
      EXPECT_NBYTE=`stat -c %s $TEST_FILE`
      for opt in "" ${OPTS} ; do
          if test "x$opt" = x ; then
             CMD="${TESTMPIRUN} -n ${NP} ./$exe -i $TEST_FILE"
          else
             CMD="${TESTMPIRUN} -n ${NP} ./$exe -$opt -i $TEST_FILE"
          fi
          echo "CMD=$CMD"
          rm -f $DARSHAN_LOG_FILE
          $CMD

          echo "parsing ${DARSHAN_LOG_FILE}"
          nbytes=`$DARSHAN_PARSER ${DARSHAN_LOG_FILE} | grep $DARSGAN_FIELD | cut -f5`
          # echo "EXPECT_NBYTE=$EXPECT_NBYTE nbytes=$nbytes"
          if test "x$nbytes" != "x$EXPECT_NBYTE" ; then
             echo "Error: CMD=$CMD nbytes=$nbytes"
             exit 1
          fi
      done
   fi
done

