import constants
import redis
from entities.manager import Manager
from entities.worker import Worker
from threading import Lock
from time import sleep

def main():
    # create a redis client
    redisClient = redis.Redis(host='localhost', port=6379, db=0)

    # create 'fib' key
    redisClient.set(constants.STRING_KEY, constants.FIB_TERM)

    # create NUM_WORKERS workers
    for i in range(constants.NUM_WORKERS):
        Worker(i, True, daemon=True).start()
        sleep(0.05)

    # create manager thread
    manager = Manager(constants.NUM_WORKERS)

    manager.start()

    manager.join()

if __name__ == '__main__':
    main()