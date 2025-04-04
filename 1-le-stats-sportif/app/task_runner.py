from queue import Queue
from threading import Thread, Event, Lock
import time
import os
import json

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
            self.num_threads = int(os.environ['TP_NUM_OF_THREADS'])
        
        else:
            self.num_threads = os.cpu_count()
        
        self.threads = []
        self.queue = Queue()
        self.jobs = {}
        self.remaining_jobs = 0
        self.remaining_jobs_lock = Lock()
        
    # Add a job to the queue
    def add_job(self, job_id, task):
        job_info = {
            'job_id': job_id,
            'status': 'running',
            'task' : task
        }
        
        self.jobs[job_id] = job_info
        self.queue.put(job_info)
    
    # Start the thread pool
    def start(self):
        for i in range(self.num_threads):
            thread = TaskRunner(i, self)
            self.threads.append(thread)
            thread.start()

        
        

class TaskRunner(Thread):
    def __init__(self, id, threadpool):
        # TODO: init necessary data structures
        Thread.__init__(self)
        self.id = id
        self.threadpool = threadpool

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            try:
                # Get job from queue
                job_info = self.threadpool.queue.get()
                job_id = job_info['job_id']
                task = job_info['task']
                # Execute the task
                result = task()
                
                # Save the result to disk
                with open(f'results/{job_id}', 'w') as f:
                    json.dump(result, f)
                
                # Mark the job as done
                job_info['status'] = 'done'
                
                self.threadpool.queue.task_done()
                
                with self.threadpool.remaining_jobs_lock:
                    self.threadpool.remaining_jobs -= 1
                    
            except Exception as e:
                print(f"Error in TaskRunner {self.id}: {e}")
                
                
                    
