#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import darshan





log = darshan.log_open("example.darshan")


print('log_get_job:')
r = darshan.log_get_job(log)
print(r)
print('-'*80)


print('log_get_exe:')
r = darshan.log_get_exe(log)
print(r)
print('-'*80)


print('log_get_mounts:')
r= darshan.log_get_mounts(log)
print(r)
print('-'*80)


# fails, symbol not found => fixed with pull request
print('log_get_modules:')
darshan.log_get_modules(log)
print(r)
print('-'*80)


print('counter_names(stdio):')
r = darshan.counter_names('stdio')
print(r)
print('-'*80)


print('counter_names(posix):')
r = darshan.counter_names('posix')
print(r)
print('-'*80)


print('counter_names(mpiio):')
r = darshan.counter_names('mpiio')
print(r)
print('-'*80)



print('log_get_posix_record:')
r = darshan.log_get_posix_record(log)
print(r)
print('-'*80)


print('dict(zip(names, values):')
d = dict(zip(darshan.counter_names('posix'), darshan.log_get_posix_record(log)['counters']))
print(d)
print('-'*80)



darshan.log_agg_stdio_records(log)





darshan.log_close(log)
