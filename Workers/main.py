import redis
import threading
from threading import Thread
import time
import random
import math

redis_host = 'localhost'
redis_port = 6379

keyName = 'heartbeats'
N = 5  # Numarul de noduri
T = 5  # Intervalul de update
W = 10  # Interval de timp la care un worker 'lucreaza'
C = 10  # % sanse ca un nod sa dea crash
M = 5  # Intervalul la care managerul porneste un worker

LOCK = 'lock.foo'

def nextFibonacciNumber(currentFibo):
    return int(round(int(currentFibo) * (1 + math.sqrt(5))/2.0))


class Manager(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.id_worker = 1

    def run(self):
        print('Manager started\n')
        while True:
            worker = Worker(self.id_worker)  # creeaza un nou thread
            self.id_worker += 1  # incrementeaza id-ul urmator
            worker.start()  # worker-ul incepe sa lucreze
            time.sleep(M)  # sleep cat timp nu trebuie sa lucreze


class Worker(threading.Thread):
    def __init__(self, id):
        threading.Thread.__init__(self)
        self.id = id
        self.lastTimeWorked = int(time.time())
        self.lastTimeCrashedCheck = int(time.time())

    def updateHeartbeat(self):
        if int(time.time()) - r.zscore(keyName, self.id) > T:
            r.zadd(keyName, {self.id: int(time.time())})

    def work(self):
        if int(time.time()) - self.lastTimeWorked > W:
            lock = r.setnx(LOCK, int(time.time()) + 10)  # definim un lock

            if lock == 0:
                return False

            fibo = nextFibonacciNumber(r.get('fibo'))
            print('Thread %d updated current fibonacci number: %d' % (self.id, fibo))
            r.set('fibo', fibo)  # setam valoarea
            r.delete(LOCK)  # stergem lock-ul
            self.lastTimeWorked = time.time()

    def crash(self):
        if int(time.time()) - self.lastTimeCrashedCheck > C:
            if random.randint(1, 101) < 10:
                r.zrem(keyName, self.id)
                return True
            self.lastTimeCrashedCheck = int(time.time())
        return False

    def startup(self):

        workers = r.zrange(keyName, start=0, end=-1, withscores=True)
        if len(workers) == 0:
            r.zadd(keyName, {self.id: int(time.time())})
        else:
            for (workerId, workerScore) in workers:
                if workerScore < int(time.time()) - T:
                    print('Delete worker no %s' % workerId)
                    r.zrem(keyName, workerId)

            if r.zcard(keyName) < N:
                print('Adding worker no %d' % self.id)
                r.zadd(keyName, {self.id: int(time.time())})
            else:
                print('Enough workers!')
                exit()

    def run(self):

        print('Worker  %d started' % self.id)
        self.startup()
        while True:
            self.updateHeartbeat()
            self.work()
            if self.crash():
                print("Thread %d has crashed!" % self.id)
                break


if __name__ == '__main__':
    r = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)  #!!!! PENTRU DECODAREA BYTES !!!!
    r.set('fibo', 1)
    r.delete(keyName)
    manager = Manager()
    manager.start()
    manager.join()
