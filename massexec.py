import logging
import subprocess
import time
from queue import Queue
from threading import Thread

""" A script distribute tasks to remote machine using the typical producer/consumer pattern model """


class ProducerThread(Thread):
    def __init__(self, name, tasks, queues):
        super(ProducerThread, self).__init__()
        self.queues = queues
        self.tasks = tasks
        self.setDaemon(True)

    def run(self):
        logging.debug("starting producer thread")
        while True:
            self.assign_task_to_agent()

    def assign_task_to_agent(self):
        for q in self.queues:
            if not q.full() and len(self.tasks) > 0:
                t = self.tasks.pop()
                q.put(t)


class ConsumerThread(Thread):
    def __init__(self, name, machine, queue):
        super(ConsumerThread, self).__init__()
        self.name = name
        self.machine = machine
        self.queue = queue
        self.setDaemon(True)

    def run(self):
        logging.debug("starting consumer thread on %s", self.machine)
        while True:
            task = self.queue.get()
            logging.debug("[%s]: start [%s: %s]" % (self.name, self.machine, task))
            result = self.do_task(self.machine, task)
            logging.debug("[%s]: end [%s: %s]/%d" % (self.name, self.machine, task, result))
            self.queue.task_done()

    def do_task(machine, task):
        cmd = "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o BatchMode=yes %s %s" % (machine, task)
        cmd = task
        result = subprocess.call(cmd, shell=True)
        return result


if __name__ == '__main__':
    FORMAT = "%(asctime)-15s [%(levelname)-6s] %(filename)s:%(lineno)3d  %(message)s"
    logging.basicConfig(format=FORMAT, level=10)

    logging.debug("start of program")

    machines = [ "localhost", "localhost" ]
    tasks = [ "echo 1", "echo 2" ]

    queues=[]
    for m in machines:
        q = Queue(1)
        c = ConsumerThread( "consumer - %s" % m, m, q)
        queues.append(q)
        c.start()

    p = ProducerThread("producer", tasks, queues)
    p.start()

    while True:
        n = len(tasks)
        logging.debug("remaining task: %d" % n)
        if n > 0:
            time.sleep(5)
        else:
            # TODO: handle edge case that the last task takes long
            logging.debug("All tasks completed..")
            break

