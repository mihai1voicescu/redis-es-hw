import redis
import time
import os

M = 30
r = redis.Redis(host='localhost', port=6379, db=0)

if __name__ == "__main__":
	print("Master: start")
	# make sure the locks are not taken
	r.delete('start_worker')
	r.delete('update_fib')
	# reset heartbeats
	r.delete('heartbeat')
	r.set('fib', 1)
	r.set('prev_fib', 0)
	while True:
		# start process
		os.system("python worker.py &")
		print("Master: started worker")
		# wait for M seconds
		time.sleep(M)
