# Data Modeling with Apache Cassandra

## Project Description

This project is about building a data model in Apache Cassandra and completing an ETL pipeline using Python. ETL pipeline must transfer data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

### Dataset

The directory of CSV files partitioned by date is named `event_data`. 
Here are examples of filepaths to two files in the dataset:  

`event_data/2018-11-08-events.csv`  
`event_data/2018-11-09-events.csv`

### File structure

Both the modelling and the ETL take place in the Jupyter notebook named `project_1B-iypnb`.

### Project Step

**Modeling your NoSQL database or Apache Cassandra database**  
- Design tables to answer specific queries
- Write Apache Cassandra CREATE KEYSPACE & CREATE TABLE statements
- Load the data with INSERT statement for each of the tables

**Build ETL Pipeline**
- Iterate through each event file in event_data to process and create a new CSV file in Python
- Load processed records into relevant tables in your data model

### Usage

Run `project_1B-iypnb`