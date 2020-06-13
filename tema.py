from threading import Thread

import threading
import redis
import time
import random
import math

r = redis.Redis(host='localhost', port=6379, db=0, password='')

class Manager(Thread):
	def __init__(self, M):
		Thread.__init__(self)
		self.M = M
		self.id_worker = 0

	def run(self):
		print('Manager thread started working...')

		while(True):
			worker = Worker(10, 20, 10, 10, self.id_worker)

			self.id_worker += 1

			worker.run()

			time.sleep(self.M)		

class Worker(Thread):
	def __init__(self, T, W, C, N, id):
		Thread.__init__(self)
		self.T = T
		self.W = W
		self.C = C
		self.N = N
		self.id = id

	def doWork(self):
		lock = r.setnx('lock.foo', 1)

		if not lock:
			return False

		n = r.get('fib')

		nextFib = round(n * (1 + math.sqrt(5))/2.0)

		print('Thread updated fib with %d'  % (nextFib))

		r.set('fib', nextFib)

		r.delete('lock.foo')

		return True

	def updateHeartbeat(self):
		currT= int(time.time())

		currTime = int(time.time())

		if currTime - currT > self.T:
			r.zadd(zname, {worker_id: time.time()})

	def doSomeWork(self):
		currW= int(time.time())

		currTime = int(time.time())

		if currTime - currW > self.W:
			work = self.doWork()

	def checkCrash(self):
		currC= int(time.time())

		random.seed()

		currTime = int(time.time())

		if currTime - currC > self.C:
			chance = random.randint(1, 10)

			if (chance == 1):
				print('Sadly, this worker crashed')

				exit()
	
	def startup(self):
		workers = r.zrange('heartbeats', 0, -1, withscores=True)

		for worker in workers:
			score = worker[1]

			if score < time.time() - self.T:
                print('Delete worker no %d' % (self.id))

                r.zrem('heartbeats', worker[0])
            	
            	if r.zcard('heartbeats') < self.N:
            		print('Adding worker no %d' % (self.id))

            		r.zadd('heartbeats', {self.id: time.time()})

            	else:
            		print('There are enough workers, so I will wait')

            		exit()

	def run(self):
		print('Worker thread no %d started working...' % (self.id))

		self.startup()

		self.updateHeartbeat()

		self.doSomeWork()

		self.checkCrash()


if __name__ == '__main__':
	manager = Manager(30)

	manager.run()