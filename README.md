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
Airflow is a platform to programmatically author, schedule and monitor workflows.

Use airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.
