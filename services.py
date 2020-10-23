# import multiprocessing as mp
# import os
#
# import time
# import threading
# import traceback
# from stats import Timer, humanbytes, memory
#
#
# class Monitor(threading.Thread):
#
#     def __init__(self, _debug):
#         super(Monitor, self).__init__()
#         self._stop_event = threading.Event()
#         self.debug = _debug
#
#         self.stop = False
#
#     def run(self):
#         last_gc = time.time()
#         gc_interval = 10
#
#         d = None  # Processo a ser criado Traffic(cam=self.cam, _remote=True, _debug=self.debug, debugframequeue=self.debugframequeue)
#         p = mp.Process(target=d.main, args=())
#         p.start()
#
#         while not self.stop:
#             if time.time() - last_gc > gc_interval:
#                 # garbage_collector()
#                 #persist_data(fs.operations.get_entries())
#                 u, d = get_usage()
#                 mem = memory()
#                 print("Undeduped Space Used : " + humanbytes(u))
#                 print("Deduped Space Used : " + humanbytes(d))
#                 print(humanbytes(mem))
#                 last_gc = time.time()
#
#             # p.terminate()
#             # p.join(5)
#             # if p.is_alive():
#             #     try:
#             #         p.kill()
#             #         p.close()
#             #     except:
#             #         pass
#
#             return True
#
#         except Exception as e:
#             self.run()
#
#     def stop(self):
#         self._stop_event.set()
#
#     def stopped(self):
#         return self._stop_event.is_set()
