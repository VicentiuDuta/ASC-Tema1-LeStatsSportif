"""
This module implements a thread pool system for handling asynchronous tasks.
It provides classes to manage a pool of worker threads and execute submitted jobs.
"""
from queue import Queue, Empty
from threading import Thread, Event, Lock
import os
import json

class ThreadPool:
    """
    Manages a pool of worker threads to execute tasks asynchronously.
    
    This class creates and maintains a fixed number of threads that pull tasks
    from a shared queue. The number of threads is determined by either an environment
    variable (TP_NUM_OF_THREADS) or by the system's CPU count.
    """
    def __init__(self):
        if 'TP_NUM_OF_THREADS' in os.environ:
            self.num_threads = int(os.environ['TP_NUM_OF_THREADS'])

        else:
            self.num_threads = os.cpu_count()

        self.threads = []
        self.queue = Queue()
        self.jobs = {}
        self.remaining_jobs = 0
        self.remaining_jobs_lock = Lock()
        self.graceful_shutdown = Event()

    # Add a job to the queue
    def add_job(self, job_id, task):
        """
        Add a job to the thread pool's task queue.
        
        Args:
            job_id (int): Unique identifier for the job
            task (callable): The function to execute
        """
        job_info = {
            'job_id': job_id,
            'status': 'running',
            'task' : task
        }

        self.jobs[job_id] = job_info
        self.queue.put(job_info)

    # Start the thread pool
    def start(self):
        """
        Start all the worker threads in the thread pool.
        
        Creates and starts the specified number of TaskRunner threads.
        """
        for i in range(self.num_threads):
            thread = TaskRunner(i, self)
            self.threads.append(thread)
            thread.start()


class TaskRunner(Thread):
    """
    Worker thread that executes tasks from the thread pool's queue.
    
    Inherits from Thread and continuously pulls jobs from the queue,
    executes them, and saves their results to disk.
    """
    def __init__(self, tid, threadpool):
        Thread.__init__(self)
        self.id = tid
        self.threadpool = threadpool

    def run(self):
        """
        Main execution method for the thread.
        
        Continuously pulls jobs from the queue, executes them, and saves results to disk.
        """
        while not self.threadpool.graceful_shutdown.is_set():
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            try:
                # Get job from queue
                job_info = self.threadpool.queue.get(timeout = 1.0)
                job_id = job_info['job_id']
                task = job_info['task']
                # Execute the task
                result = task()

                # Save the result to disk
                with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                    json.dump(result, f)

                # Mark the job as done
                job_info['status'] = 'done'
                self.threadpool.queue.task_done()

                # Update the remaining jobs count
                with self.threadpool.remaining_jobs_lock:
                    self.threadpool.remaining_jobs -= 1

            except Empty:
                # No jobs in the queue
                continue
