from datetime import datetime


# Timer - Helper class to measure the execution time of a specific part of the code
# ------ HOW TO USE ---------------
#
#       timer = Timer.Timer()
#        timer.start()
#        CODE TO BE EVALUATED
#        timer.stop()
#        m = "ELAPSED TIME -> " + str(timer.elapsed()))
#        print(m)
#
#
#
#
###################################
class Timer:
    def __init__(self):
        self._started = False
        self.initial_time = None
        self.end_time = None

    def start(self):
        if not self._started:
            self.initial_time = datetime.now()
            self._started = True
        else:
            print('Already Started')

    def stop(self):
        if self._started is not None:
            self.end_time = datetime.now()
            self._started = False
        else:
            print('Not started')

    def elapsed(self):
        return self.end_time - self.initial_time


def humanbytes(B):
    'Return the given bytes as a human friendly KB, MB, GB, or TB string'
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2)  # 1,048,576
    GB = float(KB ** 3)  # 1,073,741,824
    TB = float(KB ** 4)  # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B / TB)


def memory():
    import os
    from wmi import WMI
    w = WMI('.')
    result = w.query("SELECT WorkingSet FROM Win32_PerfRawData_PerfProc_Process WHERE IDProcess=%d" % os.getpid())
    print(humanbytes(int(result[0].WorkingSet)))
    return int(result[0].WorkingSet)

