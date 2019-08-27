# example_use_of_apache_airflow
An example of Apache Airflow for loading S3 data into Apache Redshift and transform

# Data Pipeline with Apache Airflow
This project is for building a sample data pipeline using Apache Airflow.

## Project Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

## Project Goal
Using Apache Airflow, create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. After ETL steps done, run data quality check to make sure the integrity of data.

## Data source
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## About Apache Airflow
[Excerpt from Apache Airflow Documentation](https://airflow.apache.org/)

#### 1. What is Apache Airflow
Airflow is a platform to programmatically author, schedule and monitor workflows.

Use airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

#### 2. Apache Airflow Components

![Apache Airflow Components!](./images/airflow-diagram.png)

  - <b>Scheduler</b> orchestrates the execution of jobs on a trigger or schedule. The Scheduler chooses how to prioritize the running and execution of tasks within the system. You can learn more about the Scheduler from the official [Apache Airflow documentation](https://airflow.apache.org/scheduler.html).
  - <b>Work Queue</b> is used by the scheduler in most Airflow installations to deliver tasks that need to be run to the Workers.
  - <b>Worker</b> processes execute the operations defined in each DAG. In most Airflow installations, workers pull from the work queue when it is ready to process a task. When the worker completes the execution of the task, it will attempt to process more work from the work queue until there is no further work remaining. When work in the queue arrives, the worker will begin to process it.
  - <b>Database</b> saves credentials, connections, history, and configuration. The database, often referred to as the metadata database, also stores the state of all tasks in the system. Airflow components interact with the database with the Python ORM, [SQLAlchemy](https://www.sqlalchemy.org/).
  - <b>Web Interface</b> provides a control dashboard for users and maintainers. Throughout this course you will see how the web interface allows users to perform tasks such as stopping and starting DAGs, retrying failed tasks, configuring credentials, The web interface is built using the [Flask web-development microframework](http://flask.pocoo.org/).
