import redis
import time
import random
import threading
import math


id = 0

class Worker:
    # removes outdated elements and returns whether the worker has added itself in the set
    def pruneNodes(self):
        currentTime = int(time.time())

        # get all entries
        entries = self.r.zrange('heartbeats', 0, -1, withscores=True)

        # remove all outdated entries
        for entry in entries:
            id = int(entry[0])
            timestamp = int(entry[1])

            if timestamp < currentTime - self.T:
                self.r.zrem('heartbeats', id)

        # if there are less elements than N in redis, add this worker
        if (self.r.zcard("heartbeats") < self.N):
            self.r.zadd("heartbeats", {self.id: currentTime})
            return True

        return False

    def __init__(self, T, W, C, N):
        self.T = T
        self.W = W
        self.C = C
        self.N = N

        # attribute id and increment it
        global id
        self.id = id
        id += 1

        # connect to redis
        self.r = redis.Redis(host="localhost", port=6379, db=0)

        # remove outadted notes and see if this worker should run or not
        self.isRunning = self.pruneNodes()

    # update entry in redis
    def updateEntry(self):
        currentTime = int(time.time())

        self.r.zadd("heartbeats", {self.id: currentTime}, xx=True)

    # does the work and returns whether it was able to
    def doWork(self):
        # get the fibo number

        # try to acquire the lock
        lock = self.r.setnx("lock", 1)

        # if we didn't receive the lock try again next time
        if lock==False:
            print("Lock active")
            return False

        n = int(self.r.get("fib"))

        next_value = n * (1 + math.sqrt(5))/2.0
        next_value = round(next_value)

        self.r.set("fib", next_value)

        # release the lock, memory critical part is over
        self.r.delete("lock")

        return True

    # thread routine
    def threadRun(self):
        lastTime_T = int(time.time())
        lastTime_W = int(time.time())
        lastTime_C = int(time.time())

        random.seed()

        while(True):
            currentTime = int(time.time())

            if currentTime - lastTime_T > self.T:
                # time to update the entry
                self.updateEntry()
                lastTime_T = currentTime

            if currentTime - lastTime_W > self.W:
                # time to work
                w = self.doWork()

                # if we modified the value
                if w:
                    lastTime_W = currentTime

            if currentTime - lastTime_C > self.C:
                # time to crash
                c = random.randint(1, 10)

                # 10% chance
                if (c == 1):
                    print(str(self.id) + " crashed")
                    exit()
                else:
                    lastTime_C = currentTime

    def start(self):
        if self.isRunning:
            # create a Thread and run
            print("Starting worker with id " + str(self.id))
            x = threading.Thread(target=self.threadRun)
            x.start()
        else:
            print("Enough workers, exiting...")


r = redis.Redis(host="localhost", port=6379, db=0)

r.set("fib", 1)
r.delete("lock")

while(True):
    w = Worker(10, 20, 10, 4)
    w.start()
    print(r.zrange('heartbeats', 0, -1, withscores=True))
    print(r.get("fib"))

    # M=30
    time.sleep(30)
