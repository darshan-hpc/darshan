from darshan.log_utils import get_log_path


class GetLogPath:
    params = [[1, 10, 25], ["dxt.darshan"]]
    param_names = ["num_calls", "filename"]
    # TODO: when logs repo is available from
    # asv, use an example file from there
    
    # Alternatively, could "mock" the presence of
    # the logs repo and i.e., parametrize over
    # an increasing number of "synthetic" dirs/log
    # files


    def time_get_log_path_repeat(self, num_calls, filename):
        # it is important for get_log_path() to
        # be fast on repeat calls because this mimics
        # heavy usage of log file retrieval in pytest
        # runs
        for i in range(num_calls):
            get_log_path(filename)
