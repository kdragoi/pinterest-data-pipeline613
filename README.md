# Pineterest Pipeline Project

## Contents
1. [Overview](#overview)
    1. [The batch pipeline](#the-batch-pipeline)
    1. [The stream pipeline](#the-stream-pipeline)
1. [Installation](#installation)
1. [File Structure](#file-structure)
1. [The Data](#the-data)
1. [Workflow](#workflow)

</br>

## Overview
This project was designed to create a data pipeline for extracting, transforming, and loading (ETL) data from Pinterest using various AWS Cloud Services for processing both batch and streaming data in separate pipelines. A combination of technologies were used to achieve this, including `AWS EC2`, `AWS S3`, `Apache Kafka`, `AWS MSK cluster`, `SQL`, `AWS API Gateway`, `Apache Spark`, `Databricks`, `AWS MWAA` and `AWS Kinesis`.

### The batch pipeline 
Batch data is sent via HTTP POST requests to the AWS API Gateway. The API routes the requests to a REST proxy, which sends them to the Pinterest MSK Cluster. The MSK cluster writes the data to topics stored in a S3 bucket. Databricks is configured to mount the S3 bucket. A databricks notebook is scheduled to run daily via AWS MWAA. The notebook read, cleans and queries the data.

### The stream pipeline

Streaming data is sent via HTTP PUT requests to the AWS API Gateway. The API routes the requests to the respective Amazon Kinesis Data Streams. A Databricks Notebook connects to the Kinesis Data Streams, reads, cleans and writes the streaming data into delta tables.

</br>

## Installation

### 1. Clone the repository:
```
git clone https://github.com/kdragoi/pinterest-data-pipeline613.git
```
### 2. Install the required Python packages:
```
pip install -r environment_requirements.txt
```
Required packages can be accessed via the `environment_requirements.txt` document located in the repository.

</br>

## File structure

<pre>
<b>pinterest-data-pipeline613/</b>
├─ <b>data_emulation/</b>
│  ├─ <b>user_posting_emulation.py</b>
│  │  <i>Code that indefinitely emulates data from pinterest</i>
│  ├─ <b>batch_user_posting_emulation.py</b>
│  │  <i>Code that ingests emulated data into MSK via API HTTP POST messages</i>
│  └─ <b>stream_user_posting_emulation.py</b>
│     <i>Code that ingests emulated data into Kinesis via API HTTP PUT messages</i>
├─ <b>databricks/</b>
│  ├─ <b>mount_s3_bucket.ipynb</b>
│  │  <i>Workbook that mounts s3 bucket storing the data</i>
│  ├─ <b>cleaning_batch_data.ipynb</b>
│  │  <i>Workbook that ingests and cleans batch data from S3</i>
│  ├─ <b>querying_batch_data.ipynb</b>
│  │  <i>Workbook that queries cleaned batch data</i>
│  └─ <b>all_stream_data_processing.ipynb</b>
│     <i>Workbook that ingests and cleans stream data from Kinesis</i>
├─ <b>documentation/</b>
│  └─ <i>Contains a detailed markdown file for each milestone and a screenshots
│     folder containing the screenshots used within the files sepatated into folders </i>
├─ <b>0a40ea42f8d1_dag.py</b>
│  <i>The DAG file uploaded to MWAA (Airflow UI) to run the Databricks notebook</i>
├─ <b>environment_requirements.txt</b>
│  <i>Environment requirements for the project environment</i>
└─ <b>README.md</b>
</pre>

</br>

## The Data

The python file (`link to file`), when run will minic an infinite stream of random data points that would be recieved by the pinterest API. The data points generated come from the following tables:
- `pinterest_data` containing data about the posts updated to Pinterest
- `geolocation_data` containing data about the geolocation of each post within the `pinterest_data` table
- `user_data` containing data about the users of each post within the `pinterest_data` table

Examples of the data generated:

`pinterest_data`:

```
{'index': 7528, 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf', 'title': 'No Title Data Available', 'description': 'No description available Story format', 'poster_name': 'User Info Error', 'follower_count': 'User Info Error', 'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'is_image_or_video': 'multi-video(story page format)', 'image_src': 'Image src error.', 'downloaded': 0, 'save_location': 'Local save in /data/mens-fashion', 'category': 'mens-fashion'}
```

`geolocation_data`:

```
{'ind': 7528, 'timestamp': datetime.datetime(2020, 8, 28, 3, 52, 47), 'latitude': -89.9787, 'longitude': -173.293, 'country': 'Albania'}
```

`user_data`:

```
{'ind': 7528, 'first_name': 'Abigail', 'last_name': 'Ali', 'age': 20, 'date_joined': datetime.datetime(2015, 10, 24, 11, 23, 51)}
```

</br>

## Workflow

Below, the workflow of the project is outlined in steps. For a more detailed look at each step, click the link to the relative documentation at the end of each step.

### Batch processing:
- With all neccessary credentials and environmental features set up, the EC2 instance was connected to, and configured as a kafka client (`link to milestone 3`)
- The Pinterest MSK cluster was connected to the appropriate S3 bucket by creating custom plugin and connecter via MSK Connect (`link to milestone 4`)
- A Kafka REST API was built and configured in AWS API Gateway and data was sent to the S3 bucket via the API (`link to milestone 5`)
- Databricks was set up and the S3 bucket was mounted. Data from the S3 bucket was ingested (`link to milestone 6`)
- In Databricks the ingested data was cleaned and queried using Apache Spark (`link to milestone 7`)
- A DAG file was created and uploaded to AWS MWAA (Airflow UI) to schedule the running of the batch processing notebooks in databricks (`link to milestone 8`)

### Stream processing:
- Kinesis data streams were set up for the pinterest data and the API was reconfigured to allow the requests to be routed to Kenisis
- In Databricks, the streams data was ingested from Kenisis and cleaned
- The clean data was then written into delta tables
- (`link to milestone 9`)

</br>

[Back to Contents Table](#contents)