#!/bin/bash

# Note this script is run during "make check" and "make install" must
# run before "make check".

# Exit immediately if a command exits with a non-zero status.
set -e

# run NP number of MPI processes, default 2
if test "x$NP" = x ; then
   NP=2
fi

# When TESTMPIRUN is not set, it is most likely built in the cross-compile
# environment. In this case, skip the test.
if test "x$TESTMPIRUN" = x ; then
   exit 0
fi

if test "x$HAVE_OPEN_MPI" = x1 ; then
   TESTMPIRUN="$TESTMPIRUN --oversubscribe"
fi

if test "x$USERNAME_ENV" = xno ; then
   USERNAME_ENV=$USER
fi

if test -f $DARSHAN_INSTALL_DIR/bin/darshan-config ; then
   DARSHAN_CONFIG=$DARSHAN_INSTALL_DIR/bin/darshan-config
else
   DARSHAN_CONFIG=../../darshan-runtime/darshan-config
fi
# echo "DARSHAN_CONFIG=$DARSHAN_CONFIG"

# obtain today's string
TODAY_DATE_PATH=`date "+%Y/%-m/%-d"`
# echo "TODAY_DATE_PATH=$TODAY_DATE_PATH"

# There are 3 ways to set the path for storing Darshan log files.
#   1. Option --with-log-path set at configure time
#   2. Option --with-log-path-by-env set at configure time
#   3. Variable 'LOGPATH' set in configure file, pointed by the runtime
#      environment variable, DARSHAN_CONFIG_PATH.
#   4. Runtime environment variable DARSHAN_LOGPATH
# The precedence is 1 < 2 < 3 < 4, i,e, 4 has the highest precedence, which
# overwrite all other settings.

# retrieve value of option '--with-log-path', required set at configure time.
# Note the path of this option must use a year/month/day hierarchy. Note this
# option --with-log-path is required by Darshan at configure time.
CONFIG_LOG_PATH=`sh $DARSHAN_CONFIG --log-path`
# echo "CONFIG_LOG_PATH = $CONFIG_LOG_PATH"

# retrieve value of option '--with-log-path-by-env' if set at configure time.
# Note the path of this option does not use a year/month/day hierarchy; it is
# just a flat directory. Note this option '--with-log-path-by-env' is optional
# at configure time.
CONFIG_LOG_PATH_BY_ENV=`sh $DARSHAN_CONFIG --log-path-by-env`
# echo "CONFIG_LOG_PATH_BY_ENV = $CONFIG_LOG_PATH_BY_ENV"
if test "x$CONFIG_LOG_PATH_BY_ENV" != x ; then
   # string is comma separated, test the first token
   CONFIG_LOG_PATH_BY_ENV=`echo ${CONFIG_LOG_PATH_BY_ENV} | cut -d, -f1`
fi

# Create a dummy runtime configure file for testings
RUNTIME_CONF_FILE=$PWD/runtime_conf_file.txt
RUNTIME_CONF_FILE_LOGPATH=$PWD/runtime_conf_file_logpath
# echo "RUNTIME_CONF_FILE = $RUNTIME_CONF_FILE"
# echo "RUNTIME_CONF_FILE_LOGPATH = $RUNTIME_CONF_FILE_LOGPATH"
# delete and re-create the runtime configure file.
rm -f $RUNTIME_CONF_FILE
echo "LOGPATH $RUNTIME_CONF_FILE_LOGPATH" > $RUNTIME_CONF_FILE

# unset DARSHAN_LOGPATH if set, so we can do testing within this script
unset DARSHAN_LOGPATH
DARSHAN_ENV_LOGPATH=$PWD/darshan_env_logpath

# TST_DARSHAN_LOG_PATH="${TST_DARSHAN_LOG_PATH}/${TODAY_DATE_PATH}"
# echo "TST_DARSHAN_LOG_PATH=$TST_DARSHAN_LOG_PATH"

# sh $DARSHAN_CONFIG --all

TEST_FILE=./testfile.dat

if test -f $DARSHAN_INSTALL_DIR/lib/libdarshan.so ; then
   export LD_PRELOAD=$DARSHAN_INSTALL_DIR/lib/libdarshan.so
else
   export LD_PRELOAD=../lib/.libs/libdarshan.so
fi
# echo "LD_PRELOAD=$LD_PRELOAD"

for exe in ${check_PROGRAMS} ; do

   if test "x$exe" = xtst_mpi_init ; then
      # skip tst_mpi_init as it does no I/O
      continue
   fi

   echo ""
   echo "==== Testing program: $exe"
   echo ""

   for env_darshan_logpath in yes no ; do
   for runtime_conf_file_logpath in yes no ; do
   for config_log_path_by_env in yes no ; do

echo "---------------------------------"
echo "env_darshan_logpath = $env_darshan_logpath runtime_conf_file_logpath = $runtime_conf_file_logpath config_log_path_by_env = $config_log_path_by_env"

      # whether runtime environment variable DARSHAN_LOGPATH is set
      if test "x$env_darshan_logpath" = xyes ; then
         export DARSHAN_LOGPATH=$DARSHAN_ENV_LOGPATH
         # This folder must be formatted using data time
         exp_log_path=$DARSHAN_LOGPATH/$TODAY_DATE_PATH
         mkdir -p $exp_log_path
         echo "env DARSHAN_LOGPATH set to $DARSHAN_LOGPATH"
      else
         rm -rf $DARSHAN_LOGPATH/$TODAY_DATE_PATH
         unset DARSHAN_LOGPATH
         echo "env DARSHAN_LOGPATH is NOT set"

         if test "x$runtime_conf_file_logpath" = xyes ; then
            export DARSHAN_CONFIG_PATH=$RUNTIME_CONF_FILE
            exp_log_path=$RUNTIME_CONF_FILE_LOGPATH/$TODAY_DATE_PATH
            # delete and re-create the folder, LOGPATH. Note the folder LOGPATH
            # must use a year/month/day hierarchy.
            rm -rf $exp_log_path
            mkdir -p $exp_log_path
            echo "env DARSHAN_CONFIG_PATH set to $DARSHAN_CONFIG_PATH"
            echo "LOGPATH in the configure file set to $RUNTIME_CONF_FILE_LOGPATH"
         else
            rm -rf $RUNTIME_CONF_FILE_LOGPATH
            unset DARSHAN_CONFIG_PATH
            echo "env DARSHAN_CONFIG_PATH is NOT set"

            if test "x$config_log_path_by_env" = xyes ; then
               if test "x$CONFIG_LOG_PATH_BY_ENV" = x ; then
                  # option --with-log-path-by-env is not set at configure time
                  continue
               fi
               # folder set in --with-log-path-by-env do not use a
               # year/month/day hierarchy.
               exp_log_path=$PWD/config_log_path_by_env
               export $CONFIG_LOG_PATH_BY_ENV=$exp_log_path
               mkdir -p $exp_log_path
               echo "configure time log-path-by-env set to $CONFIG_LOG_PATH_BY_ENV"
               echo "Now set $CONFIG_LOG_PATH_BY_ENV to $exp_log_path"
            else
               rm -rf $PWD/config_log_path_by_env
               unset $CONFIG_LOG_PATH_BY_ENV

               # none of env variables are set
               exp_log_path=$CONFIG_LOG_PATH/$TODAY_DATE_PATH
               mkdir -p $exp_log_path
               echo "configure time log-path set to $CONFIG_LOG_PATH"
            fi
         fi
      fi

      # check if the executable is an MPI program
      is_exe_mpi=`nm ./$exe | grep -i "MPI_Init"`
      # env variable DARSHAN_LOGPATH_HIERARCHY only takes effect on non-MPI jobs
      if test "x$is_exe_mpi" = x && test -v DARSHAN_LOGPATH_HIERARCHY ; then
         # When log path name hierarchy is enabled, the log file path is:
         # USER/PROGRAMNAME/JOBID/HOSTNAME/PID-hashed_name.darshan
         exp_darshan_log_file="${exp_log_path}/${USERNAME_ENV}/${exe}/"
      else
         exp_darshan_log_file="${exp_log_path}/${USERNAME_ENV}_${exe}"
      fi
      echo "exp_darshan_log_file=$exp_darshan_log_file"
      rm -rf ${exp_darshan_log_file}*

      # Run MPI program
      CMD="${TESTMPIRUN} -n ${NP} ./$exe -q $TEST_FILE"
      echo "CMD=$CMD"
      $CMD
      rm -f $TEST_FILE

      # check if log file has been created in the expected folder
      if ! compgen -G "${exp_darshan_log_file}*" > /dev/null; then
         echo "Error: log file not found (expected ${exp_darshan_log_file}*"
         echo "Running command: ls -lt ${exp_darshan_log_file}*"
         ls -lt ${exp_darshan_log_file}*
         exit 1
      fi
      # ls -lt ${exp_darshan_log_file}*

      rm -rf ${exp_darshan_log_file}*

   done
   done
   done
done

