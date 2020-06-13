import redis
import time
import threading
import random

redis_host = "localhost"
redis_port = 6379

r = redis.Redis(host=redis_host, port=redis_port, db=0)
r.set('fib0', 1)
r.set('fib1', 1)

class Manager:

	def __init__(self, nrWorkers):
		self.nrWorkers = nrWorkers

	def start(self):

		M = 30
		T = 10
		W = 20
		C = 10
		idWorker = 0
		print(r.get('fib0'))

		while True:
			# creez un nou worker
			myWorker = Worker(T, W, C, idWorker)
			myWorker.work()
			idWorker += 1
			print(r.get('fib1'))

			time.sleep(M)

class Worker:

	def __init__(self, T, W, C, idWorker):

		self.T = T
		self.W = W
		self.C = C
		self.idWorker = idWorker
		self.score = time.time()
		self.lastTimeW = 0
		self.crashed = 0

	def work(self):

		allWorkers = r.zrange('heartbeats', 0, -1, withscores=True)
		workerEliminated = 0

		for worker in allWorkers:
			workerId, score = worker

			now = int(time.time())

			if score < now - self.T:
				r.zrem('heartbeats', workerId)
				workerEliminated = 1

		if workerEliminated == 1:
			now = int(time.time())
			# adaug worker-ul curent
			r.zadd('heartbeats', {self.idWorker: now})
			self.score = now
			threading.Thread(target=self.moreWork).start()

	def moreWork(self):

		while True:
			now = int(time.time())
			#update score
			if now - self.score > self.T:
				r.zadd('heartbeats', {self.idWorker: now})
				self.score = now

			if now - self.lastTimeW > self.W:
				self.calcFibonacci()
				self.lastTimeW = now

			if now - self.crashed > self.C:
				odds = random.randint(1, 10)
				if odds == 10:
					r.zrem('heartbeats', self.idWorker)
					return
			self.crashed = now

	def calcFibonacci(self):

		myLock = r.setnx("lock", 1)
		if myLock == 1:
			fib = r.get('fib0') + r.get('fib1')
			r.set('fib0', r.get('fib1'))
			r.set('fib1', fib)
			r.delete("lock")

if __name__ == '__main__':

	N = 10
	myManager = Manager(N)

	myManager.start()