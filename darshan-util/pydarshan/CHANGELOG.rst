PyDarshan-3.4.2.0
=================
* Track Darshan 3.4.2 release, no PyDarshan changes

PyDarshan-3.4.1.0
=================
 * Fixed memory leaks in the following backend CFFI bindings
   (reported by Jesse Hines):
  - log_get_modules
  - log_get_mounts
  - log_get_record
  - log_get_name_records
  - log_lookup_name_records
 * Added PnetCDF module information to job summary tool
 * Testing modifications:
  - Switched to use of context managers for log Report objects to
    avoid test hangs in certain environments
  - Marked tests requiring lxml package as xfail when not installed

PyDarshan-3.4.0.1
=================
* New Darshan job summary report styling
* Bug fix to heatmap module plotting code caused by logs
  with inactive ranks
* Fix warnings related to Pandas deprecation of df.append
* Add cibuildwheel support

PyDarshan-3.4.0.0
=================
* First stable public release, including first version of
  'darshan summary' tool

PyDarshan-3.3.1.1
=================
* Added support for manylinux2014 wheels, dropped support
  for manylinux1, manylinux2010

PyDarshan-3.3.1.0
=================
* Darshan 3.3.1 release

PyDarshan-3.3.0.3
=================
* Added support for Darshan's AutoPerf modules

PyDarshan-3.3.0.2
=================
* Initial public release
