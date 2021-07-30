import threading
import time

_stat = threading.local()
_stat.stat_object = None


def record_running_time(func):
    def _inner(*args, **kwargs):
        if _stat.stat_object is None:
            return func(*args, **kwargs)
        else:
            begin = time.perf_counter()
            ret = func(*args, **kwargs)
            end = time.perf_counter()
            stat_object = _stat.stat_object
            if func.__name__ not in stat_object.running_time:
                stat_object.running_time[func.__name__] = [0, 0]
            stat_object.running_time[func.__name__][0] += 1
            stat_object.running_time[func.__name__][1] += (end - begin)
            return ret
    return _inner


class FunctionCallPerfStat:
    def __init__(self, print_on_exit):
        self.clear()
        self.print_on_exit = print_on_exit

    def __enter__(self):
        self.last_stat_object = _stat.stat_object
        _stat.stat_object = self

    def clear(self):
        self.running_time = {}

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.print_on_exit:
            import os
            stat_string = f'Perf stat(pid {os.getpid()}):\t'
            for func_name, (run_times, running_time) in self.running_time.items():
                stat_string += f'time {running_time} called {run_times} avg {running_time / run_times}\t'
            print(stat_string)
        _stat.stat_object = self.last_stat_object