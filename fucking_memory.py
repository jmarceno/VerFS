import tracemalloctracemalloc.start(10)
try:    
	run_reflector()
except:    
	snapshot = tracemalloc.take_snapshot()    
	top_n(25, snapshot, trace_type='filename')