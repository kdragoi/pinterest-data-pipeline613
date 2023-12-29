# Milestone 4 - Batch Processing: Connect an MSK Cluster to an S3 Bucket

## Task 1
### Create a custom plugin with MSK Connect

Make a note of the S3 bucket name. While in the EC2 Client, download the Confluent.io Amazon S3 Connector and copy it to the S3 bucket:

```
sudo -u ec2-user -i
mkdir kafka-connect-s3 && cd kafka-connect-s3
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://user-0a40ea42f8d1-bucket/kafka-connect-s3/
```

Create custom plugin in the MSK Connect console:

![custom_plugin](screenshots/m4/1.png)

## Task 2
### Create a connector with MSK Connect

Create the connector, selecting the custom plugin just created and the ```pinterest-msk-cluster```.

Add the Connector configuration settings:
```
connector.class=io.confluent.connect.s3.S3SinkConnector

# same region as our bucket and cluster
s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3

# include nomeclature of topic name, given here as an example will read all data from topic names starting with msk.topic....
topics.regex=0a40ea42f8d1.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=user-0a40ea42f8d1-bucket
```

![custom_plugin](screenshots/m4/2.png)

