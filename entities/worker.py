from threading import Thread
import constants
from datetime import datetime
from time import sleep
from math import sqrt
import random
import redis

class Worker(Thread):
    def __init__(self, id, initial = False, **kwargs):
        super().__init__(target=Thread, **kwargs)
        self.id = id
        self.redisClient = redis.Redis(host='localhost', port=6379, db=0)
        self.initial = initial


    def run(self):
        if self.initial == False:
            # get heartbeats content
            elements = [(int(x), y) for (x, y) in self.redisClient.zrange(constants.ZSET_KEY, 0, -1, False, True) ]
            count = 0
            length = 0

            ref_score = datetime.now().timestamp() - constants.T
            for (member, score) in elements:
                length += 1
                if (score < ref_score):
                    self.redisClient.zrem(constants.ZSET_KEY, member)
                    count += 1

            if count == 0 and length == constants.NUM_WORKERS:
                print("[Worker " + str(self.id) + "]: just exit.")
                return

        score = datetime.now().timestamp()
        member = self.id

        self.redisClient.zadd(constants.ZSET_KEY, {
            member: score
        })
    

        T = constants.T
        W = constants.W
        C = constants.C
        
        while True:
            min_val = min(T, W, C)
            sleep(min_val)

            T -= min_val
            W -= min_val
            C -= min_val

            if T == 0:
                T = constants.T
                member = self.id
                score = datetime.now().timestamp()
                self.redisClient.zadd(constants.ZSET_KEY, {
                    member: score
                })
                print("[Worker " + str(self.id) + "]: updated succesfully.")

            if W == 0:
                W = constants.W

                lock = self.redisClient.setnx('lock.fib', 1)

                if lock == False:
                    continue
                value = self.redisClient.get(constants.STRING_KEY)
                new_value = round(int(value) * (1 + sqrt(5)) / 2.0, 0)
                self.redisClient.set(constants.STRING_KEY, int(new_value))
                self.redisClient.delete('lock.fib')

                print("[Worker " + str(self.id) + "]: add next Fibonacci number " + str(new_value))

            if C == 0:
                C = constants.C
                gone = random.randint(1, 10)
                if gone == 1:
                    print("[Worker " + str(self.id) + "]: crashed.")
                    return
