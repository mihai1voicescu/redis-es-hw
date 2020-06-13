import redis
import time
import os
import thread
import random

N = 7
T = 10
W = 20
C = 10

r = redis.Redis(host='localhost', port=6379, db=0)

def update_heartbeat():
	while True:
		time.sleep(T)
		mapping = {os.getpid(): time.time()}
		r.zadd('heartbeat', mapping)
		print("Worker " + str(os.getpid()) + " still alive")

def try_crash():
	while True:
		time.sleep(C)
		if (random.randint(0, 10) == 0):
			print("Worker " + str(os.getpid()) + " exiting")
			os._exit(1)

def work():
	while True:
		time.sleep(W)
		# wait turn
		while (r.setnx('update_fib', os.getpid()) == 0):
			# check if the process with the lock is still alive
			has_lock = r.get('update_fib')
			last_hb = r.zscore('heartbeat', has_lock)
			if (last_hb == None):
				r.delete('update_fib')
				continue
			if (int(last_hb) + T < int(time.time())):
				r.delete('update_fib')

		current_fib = int(r.get('fib'))
		prev_fib = int(r.get('prev_fib'))
		next_fib = current_fib + prev_fib
		mapping = {'prev_fib': current_fib, 'fib': next_fib}
		# hoping mset is atomic
		r.mset(mapping)
		print("Worker " + str(os.getpid()) + " updated fib: " + str(next_fib))
		# release lock
		r.delete('update_fib')

if __name__ == "__main__":
	print("Worker " + str(os.getpid()) + " started")
	# assumed the process can't die before checking the heartbeat
	while (r.setnx('start_worker', os.getpid()) == 0):
		pass
	t = time.time()
	heartbeats = r.zrange('heartbeat', 0, -1, False, True)
	# remove the heartbeats of dead processes
	for hb in heartbeats:
		if ((int(hb[1]) + T) < int(t)):
			r.zrem('heartbeat', hb[0]) 
	# enough processes running
	if r.zcard('heartbeat') >= N:
		os._exit(0)
	# this process can run so we're adding its first heartbeat
	mapping = {os.getpid(): time.time()}
	r.zadd('heartbeat', mapping)
	# release the entry lock
	r.delete('start_worker')

	# the worker has 3 threads: heartbeat update, try to crash, work
	thread.start_new_thread(update_heartbeat, ())
	thread.start_new_thread(work, ())
	thread.start_new_thread(try_crash, ())
	while True:
		pass
