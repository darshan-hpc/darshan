#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import darshan.backend.cffi_backend as backend


log = backend.log_open("example.darshan")
mods = backend.log_get_modules(log)


nrecs = backend.log_get_name_records(log)


# reliable hashes: 15920181672442173319 (stdout), 14734109647742566553 (stdin), 7238257241479193519 (stderr)
selected_nrecs = backend.log_lookup_name_records(log, [15920181672442173319])
