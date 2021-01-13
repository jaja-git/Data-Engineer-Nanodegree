# Data Pipelines with Airflow

## Project Description

This project is about building data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. 

This project requires creating custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

The source data resides in S3 and needs to be processed in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Dataset

Project contains two datasets that reside in S3:

- Song data: `s3://udacity-dend/song_data`  
- Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

### File structure

The project package contains three major components:
- The dags folder containing the dag `airflow_project_dag.py`
- The `plugins/operators` folder with operator templates
- The `plugins/helpers` folder containing the SQL transformations

### Operators

#### Stage Operator
The stage operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided.


#### Fact and Dimension Operators
Takes as input the SQL statement and the target database on which to run the query against. Default mode is `truncate-insert` (i.e the target table is emptied before the load), but parameter allows switching between insert modes when loading dimensions. 

#### Data Quality Operator
Runs checks on the data itself. The operator's main functionality is to receive a few SQL based test cases along with the expected results and execute the tests. For each the test, if it has failed, the operator will raise an exception and the task will retry and fail eventually.


### Usage

To initiliaze the project, first run 2 files from the previous project named Cloud Data Warehouse:


- Run `create_cluster.py` to create the Redshift cluster and IAM role.  
- Run `create_tables.py` to create the tables.

Then:
- Run `airflow_project_dag.py` to load data.


To connect to the db via psql, run  
`psql -h redshift-cluster.ce5jntlwthav.us-east-1.redshift.amazonaws.com -U awsuser -d dev -p 5439`

