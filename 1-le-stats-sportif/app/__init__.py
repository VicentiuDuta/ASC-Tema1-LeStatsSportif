"""
Initialization module for the Le Stats Sportif application.

This module creates and configures the Flask webserver, initializes the data ingestor,
and sets up the task runner thread pool.
"""
# for the webserver
if __name__ != "unittests.TestWebserver":
    import os
    import logging
    from logging.handlers import RotatingFileHandler
    import time
    from flask import Flask
    from app.data_ingestor import DataIngestor
    from app.task_runner import ThreadPool
    if not os.path.exists('results'):
        os.mkdir('results')

    webserver = Flask(__name__)
    # Create a logger for the webserver
    webserver.logger = logging.getLogger(__name__)
    # Set the logging level to INFO
    webserver.logger.setLevel(logging.INFO)
    # Check if the logs directory exists, if not create it
    if not os.path.exists('logs'):
        os.mkdir('logs')
    # Create a rotating file handler for logging
    handler = RotatingFileHandler('logs/webserver.log', maxBytes = 10000, backupCount = 10)
    # Set the timezone to UTC
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter.converter = time.gmtime
    # Add the formatter to the handler
    handler.setFormatter(formatter)
    # Add the handler to the logger
    webserver.logger.addHandler(handler)
    webserver.logger.info("Webserver started")

    webserver.tasks_runner = ThreadPool()
    webserver.tasks_runner.start()

    webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
    webserver.job_counter = 1
    from app import routes
# for the unittests
else:
    data_ingestor = DataIngestor("./test.csv")

