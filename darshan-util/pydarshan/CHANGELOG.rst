PyDarshan-3.4.7.0
=================
* Support for extracting and analyzing new DAOS instrumentation
  module data
  - summary CLI tool now integrates DAOS data into reports
* New CLI tools for extracting and printing summary data for
  one or more Darshan logs
  - `job_stats` prints high-level statistics on a per-job level
  - `file_stats` prints high-level statistics on a per-file level
  - output in either CSV format or using the Rich package (which
    is now a proper dependency of PyDarshan)
* Add DarshanReport object capability to filter module records
  according to given regular expressions
  - users can now pass the following parameters to DarshanReport
    objects to use this functionality: `filter_patterns` (a list
    of regex strings to match against) and `filter_mode` (either
    "exclude" or "include")
  - integrated this capability into summary CLI tool to allow
    filtering of data before generating reports

PyDarshan-3.4.6.0
=================
* Track Darshan 3.4.6 release, no PyDarshan changes

PyDarshan-3.4.5.0
=================
* Track Darshan 3.4.5 release, no PyDarshan changes

PyDarshan-3.4.4.0
=================
* Track Darshan 3.4.4 release, no PyDarshan changes

PyDarshan-3.4.3.0
=================
* Various job summary tool improvements
  - add new module overview table
  - add new file count summary table
  - add new plot of POSIX module sequential/consecutive accesses
  - include PnetCDF `wait` time in I/O cost figures
  - drop default generation of DXT-based heatmaps and add
    a new cmdline option to force generate them (--enable_dxt_heatmap)
  - drop usage of scientific notation in "Data access by category"
    plot
  - make captions, axis labels, and annotations clearer and
    easier to read
* Integrated Python support for darshan-util accumulator API for
  aggregating file records and calculating derived metrics
  - Added backend routine `accumulate_records`, which returns
    a derived metric structure and a summary record for an
    input set of records
  - Added backend routine `_df_to_rec` to allow conversion of
    a DataFrame of records into raw byte arrays to pass into
    the darshan-util C library (e.g., for using accumulator API)
* Fixed bug allowing binary wheel installs to prefer darshan-util
  libraries found in LD_LIBRARY_PATH (reported by Jean Luca Bez)
* Fixed bug in DXT heatmap plotting code related to determining
  the job's runtime
* Updated docs for installation/usage of PyDarshan
* Dropped support for Python 3.6

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
