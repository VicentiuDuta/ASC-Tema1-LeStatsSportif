from queue import Queue
from threading import Thread, Event, Lock
import time
import os

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
        # Note: the TP_NUM_OF_THREADS env var will be defined by the checker
        if 'TP_NUM_OF_THREADS' in os.environ:
            num_threads = int(os.environ['TP_NUM_OF_THREADS'])
        
        else:
            num_threads = os.cpu_count()
        
        self.threads = []
        self.queue = Queue()        
        
        

class TaskRunner(Thread):
    def __init__(self, id, queue):
        # TODO: init necessary data structures
        Thread.__init__(self)
        self.id = id
        self.queue = queue

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            pass
