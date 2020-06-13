from threading import Thread
import constants
from time import sleep
from entities.worker import Worker

class Manager(Thread):
    def __init__(self, contor, **kwargs):
        super().__init__(target=Thread, **kwargs)
        self.contor = contor

    def run(self):
        while True:
            # wait W seconds
            sleep(constants.W)

            # create a daemon worker
            Worker(self.contor, daemon=True).start()
            print("[Manager]: create a new worker with id = " + str(self.contor))
            self.contor += 1
