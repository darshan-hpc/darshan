#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import darshan


log = darshan.log_open("example.darshan")

record = darshan.log_get_stdio_record(log)
record['counters']

dict(zip(darshan.counter_names("STDIO"), record['counters']))


record = darshan.log_get_stdio_record(log)
record['counters']

dict(zip(darshan.counter_names("STDIO"), record['counters']))
