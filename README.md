# Data Lake with EMR - Demo

## Summary
This is a simple project to demonstrate how to implement an ETL process that extracts, transforms and loads data into S3 buckets on AWS. It was developed as a condition for approval in the Data Engineer nanodegree at Udacity.

## ETL Explained
This ETL job is meant to extract song data from two S3 buckets owned by Udacity, and load it into 5 tables arranged in star schema model. This whole process was implemented in a Elastic Map Reduce (EMR) cluster on AWS. All the data will be loaded into a S3 bucket.

## Schema Design 
### Input <br>
**song_data** <br>
| field  | type  |
|---|---|
| artist_id | string |
| artist_latitude | double |
| artist_location | string |
| artist_longitude | double |
| artist_name | string |
| duration | double |
| num_songs | long |
| song_id | string |
| title | string |
| year | integer |
<br>

**log_data** <br>
| field  | type  |
|---|---|
| artist | string |
| auth | string |
| firstName | string |
| gender | string |
| itemInSession | long |
| lastName | string |
| length | double |
| level | string |
| location | string |
| method | string |
| page | string |
| registration | double |
| sessionId | long |
| song | string |
| status | long |
| ts | long |
| userAgent | string |
| userId | string |
<br>
### Output <br>
**song table**
| field  | type |
|---|---|
| song_id | string | 
| title | string | 
| artist_id | string | 
| year | integer | 
| duration | double | 
<br>

**artist table**
| field  | type |
|---|---|
| artist_id | string | 
| name | string | 
| location | string | 
| latitude | double | 
| longitude | double | 
<br>

**time table**
| field  | type |
|---|---|
| start_time | timestamp |
| hour | integer |
| day | integer |
| week | integer |
| month | integer |
| year | integer |
| weekday | integer |
<br>

**songplays table**
| field  | type |
|---|---|
| songplay_id | integer |
| start_time | timestamp |
| user_id | string |
| artist | string |
| song | string |
| level | string |
| session_id | long |
| location | string |
| user_agent | string |
| year | integer |
| month | integer |
| artist_id | string |
| artist_name | string |
| song_id | string |
| title | string |
<br>

**user table**
| field  | type |
|---|---|
| user_id | string |
| first_name | string |
| last_name | string |
| gender | string |
| level | string |
<br>

## ETL Pipeline Requirements
- EMR Cluster Running:
  - EMR 5.20.0 (Spark, Hadoop, Hive and Livy enabled)
  - EMR Role with read and write access for S3

## How to use
1. Adjust the output S3 bucket path in `etl.py` file.
2. Upload `etl.py` file to the master node of the cluster, via SCP. 
3. At master node of the EMR cluster, make sure the environment variable PYSPARK_PYTHON is configured correctly, otherwise run the following command: `export PYSPARK_PYTHON=/usr/bin/python3`
4. Submit the `etl.py` to be run by Spark.
5. Check if no errors were thrown and check if the data was loaded correctly into the output bucket location.

## Removing resources provided
Once the ETL job is done, the output bucket will be filled with processed data. You can use a notebook on EMR cluster to read that data. When you finish all the analysis, don't forget to terminate your EMR cluster and delete data in S3 buckets to make sure there will be no extra charges.
