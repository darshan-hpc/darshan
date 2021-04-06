"""Aggregator functions are monkey patched into the DarshanReport object during
initialization of DarshanReport class.

.. note::
   These functions are only enabled if the `darshan.enable_experimental(True)` function is called.

Example usage::

  import darshan
  import darshan.report
  dasrhan.enable_experimental(True)
  report = darshan.report.DarshanReport()
  ...
  result = report.agg_ioops()

"""
