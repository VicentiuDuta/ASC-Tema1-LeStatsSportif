"""
This module defines all the API routes for the Le Stats Sportif server.
It handles various endpoints for calculating statistics based on nutrition and health data.
"""

import os
import json
from flask import request, jsonify
from app import webserver

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    """
    Example endpoint that processes and returns POST data.
    
    Returns:
        JSON: Response containing the received data or an error message
    """
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")
        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)

    # Method Not Allowed
    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_results(job_id):
    """
    Retrieve the results of a previously submitted job.
    
    Args:
        job_id (str): The ID of the job to retrieve results for
    
    Returns:
        JSON: Status of the job and results if completed
    """
    webserver.logger.info("Received get_results request for job_id: %s", job_id)
    job_id = int(job_id)
    # Check if the job_id is valid
    if job_id not in webserver.tasks_runner.jobs:
        webserver.logger.error("Invalid job_id: %s", job_id)
        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        })
    # Check if the job is still running
    if webserver.tasks_runner.jobs[job_id]['status'] == 'running':
        webserver.logger.info("Job %s is still running", job_id)
        return jsonify({
            "status": "running"
        })
    # Check if the job is done
    if webserver.tasks_runner.jobs[job_id]['status'] == 'done':
        webserver.logger.info("Job %s is done", job_id)
        result_file = f"results/{str(job_id)}"
        if os.path.exists(result_file):
            with open(result_file, 'r', encoding='utf-8') as f:
                result = json.load(f)
            return jsonify({
                "status": "done",
                "data": result
            })

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    Handle requests to calculate the mean value for each state for a specific question.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    # Check if the graceful shutdown event is set
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.states_mean(data['question'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    Handle requests to calculate the mean value for a specific state and question.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.state_mean(data['question'], data['state'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
            "status": "error",
            "reason": "shutting down"
    })

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    Handle requests to get the top 5 performing states for a specific question.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.best5(data['question'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    Handle requests to get the 5 worst performing states for a specific question.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.worst5(data['question'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    Handle requests to calculate the global mean value for a specific question.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.global_mean(data['question'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    Handle requests to calculate difference between global mean and each state's mean.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.diff_from_mean(data['question'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutdown"
    })

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    Handle requests to calculate difference between global mean and a specific state's mean.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.state_diff_from_mean(data['question'], data['state'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    Handle requests to calculate mean values grouped by stratification categories.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.mean_by_category(data['question'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    Handle requests to calculate mean values by category for a specific state.
    
    Returns:
        JSON: Job ID for the created task or error if server is shutting down
    """
    data = request.json
    webserver.logger.info("Received state_mean_by_category request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        # Create task as a closure
        def task():
            result = webserver.data_ingestor.state_mean_by_category(data['question'], data['state'])
            return result

        # Add task to the thread pool. Task will contain the job_id, the task and the status
        webserver.tasks_runner.add_job(job_id, task)
        # Increment job_id counter
        webserver.job_counter += 1
        # Increment threadpool remaining jobs
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        # Return associated job_id
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })


@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    """
    Get the number of jobs currently in the queue waiting to be processed.
    
    Returns:
        JSON: Number of remaining jobs
    """
    webserver.logger.info("Received request for number of jobs in the queue.")
    # Return the number of jobs in the queue
    webserver.logger.info("Number of jobs in the queue: %s", webserver.tasks_runner.remaining_jobs)
    return jsonify({
        'num_jobs': webserver.tasks_runner.remaining_jobs
    })

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """
    Initiate a graceful shutdown of the server.
    
    Sets the shutdown flag and continues processing existing jobs without accepting new ones.
    
    Returns:
        JSON: Status indicating if the server is still processing jobs or ready to shut down
    """
    webserver.logger.info("Received request for graceful shutdown.")
    # Set the graceful shutdown event
    webserver.tasks_runner.graceful_shutdown.set()
    # Check if there are any remaining jobs in the queue
    if webserver.tasks_runner.remaining_jobs > 0:
        webserver.logger.info("Server is still processing jobs.")
        return jsonify({
            "status": "running"
        })

    webserver.logger.info("Server is ready to shut down.")
    # If there are no remaining jobs, we can proceed with the shutdown
    return jsonify({
        "status": "done"
    })

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    """
    Get information about all jobs that have been submitted to the server.
    
    Returns:
        JSON: List of all job IDs and their current status
    """
    webserver.logger.info("Received request for all jobs information.")
    data = []
    # Extract job informations from the tasks_runner
    for job_id, job_info in webserver.tasks_runner.jobs.items():
        data.append({
            f'job_id_{str(job_id)}': job_info['status']
        })
    webserver.logger.info("Sent jobs information: %s", data)
    return jsonify({
        "status": "done",
        "data": data  
    })

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    """
    Serve the home page with information about available routes.
    
    Returns:
        str: HTML formatted list of available routes
    """
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs = "".join(f"<p>{route}</p>" for route in routes)

    msg += paragraphs
    return msg

def get_defined_routes():
    """
    Helper function to get all defined routes in the Flask application.
    
    Returns:
        list: List of strings describing each route and its allowed methods
    """
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
