#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import darshan


log = darshan.log_open("sample.darshan")
mods = darshan.log_get_modules(log)


rec2 = darshan.log_get_stdio_record(log)
print(rec2)

rec = darshan.log_get_stdio_record(log)
print(rec)




#record['counters']
#dict(zip(darshan.counter_names("STDIO"), record['counters']))
