# LEWS Data-pipeline Module for Language Identification

This is the pipeline module for Language Identification

## Pre-Requisites

- Docker 18.09.0 or higher

- Kafka Broker

- Streaming data in json format

### Install and run Kafka Broker
#### Ubuntu 18.04
Follow https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-18-04
#### Windows 
Follow https://medium.com/@shaaslam/installing-apache-kafka-on-windows-495f6f2fd3c8
#### MacOS
Follow https://medium.com/pharos-production/apache-kafka-macos-installation-guide-a5a3754f09c

## Running in local environment
### Install dependancies
Install dependancies given in requirements.txt. 
```bash
pip install -r requirements.txt
```

### Running the module

Running
```bash
python language_detect.py
```

## Running in Docker (Recommended for Production)
### Building the Docker Image


```bash
docker build --tag lews-language-detect .
```

### Usage


```bash
docker run -e MODULE_NAME="LEWS-TWEET-LANGUAGE-DETECT" \
-e CONSUMER_GROUP="LEWS-TWEET-LANGUAGE-DETECT-CG01" \
-e KAFKA_SOURCE_BOOTSTRAP_SERVERS="<source_kafka_bootstrap_server>" \
-e KAFKA_SOURCE_TOPIC="<source_topic>" \
-e KAFKA_TARGET_BOOTSTRAP_SERVERS="<target_kafka_bootstrap_server>" \
-e KAFKA_TARGET_TOPIC="<target_topic>" lews-language-detect 
```