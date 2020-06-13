import redis
import time
import multiprocessing
import random
"""
M = 30s -> numar de sec la care se aprinde worker

T = 10s -> updateaza workerul cu timestampul curent
W = 20s -> Smth work fib
C = 10s -> % sa fie distrus
"""


class Worker:
    def __init__(self, identifier, redis_instance, lock):
        self.identifier = identifier
        self.redis_instance = redis_instance
        self.lock = lock

    def do_update(self):
        self.redis_instance.set(self.identifier,
                                int(time.time()))

    def wait(self, seconds):
        time.sleep(seconds)

    def work(self):
        print("Working %s" % self.identifier)
        self.lock.acquire()
        current_fibonacci = self.redis_instance.get('current_fibonacci')
        prev_fibonacci = self.redis_instance.get('prev_fibonacci')
        self.redis_instance.set('current_fibonacci',
                                current_fibonacci + prev_fibonacci)
        self.redis_instance.set('prev_fibonacci', current_fibonacci)
        self.lock.release()

    def possible_failure(self):
        if int(random.random() * 100) < 10:
            print("%s failed!" % self.identifier)
            exit(1)

    def start(self):
        while True:
            self.work()
            self.wait(10)
            self.possible_failure()
            self.do_update()
            self.wait(20)


class Orchestration:
    WorkerDict = {}
    N = 10
    M = 30
    redis_instance = redis.Redis(host='localhost',
                                 port=6379)
    current_identifier = 0
    lock = multiprocessing.Lock()

    @staticmethod
    def get_new_id():
        Orchestration.current_identifier += 1
        return Orchestration.current_identifier

    def create_worker(self):
        new_id = Orchestration.get_new_id()
        new_wk = Worker(new_id, Orchestration.redis_instance,
                        Orchestration.lock)
        Orchestration.WorkerDict[new_id] = new_wk
        new_process = multiprocessing.Process(target=lambda x: x.start(),
                                              args=(new_wk,))
        new_process.start()

    def orchestrate(self):
        while True:
            time.sleep(10)
            current_ts = int(time.time())

            Orchestration.lock.acquire()
            to_pop = []
            for identifier in Orchestration.WorkerDict:  # TODO: Set
                wk_ts = Orchestration.redis_instance.get(identifier)
                if not wk_ts or int(wk_ts) <= current_ts - 3:
                    to_pop.append(identifier)
            res = [Orchestration.WorkerDict.pop(
                identifier) for identifier in to_pop]

            Orchestration.lock.release()

            if len(Orchestration.WorkerDict.keys()) < Orchestration.N:
                self.create_worker()

    @staticmethod
    def initialize_redis():
        Orchestration.redis_instance.set('current_fibonacci', 1)
        Orchestration.redis_instance.set('prev_fibonacci', 1)


def main():
    Orchestration.initialize_redis()
    orc = Orchestration()
    orc.orchestrate()


main()
