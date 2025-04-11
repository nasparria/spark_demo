# Apache Spark Demo

A demonstration project for Apache Spark data processing and analytics using Python and PySpark.

## Overview

This repository contains a proof of concept for integrating Apache Spark with an existing architecture that includes PostgreSQL RDS, S3 storage, and microservices. The demo showcases how Spark can be used to:

- Process large volumes of customer data
- Perform complex analytical queries
- Visualize results
- Integrate with PostgreSQL databases
- Process data from JSON and CSV files

## Requirements

- Python 3.8+
- Java 11+ (OpenJDK recommended)
- Apache Spark 3.3+
- Jupyter Notebook

Dependencies:
- pyspark
- pandas
- matplotlib
- findspark
- psycopg2-binary

## Setup

1. Install Java:
```bash
brew install openjdk@17
```

2. Install Apache Spark:
```bash
brew install apache-spark
```

3. Create a virtual environment:

```bash
python3 -m venv spark-demo-env
source spark_env/bin/activate
```
4. Install dependencies:
```bash
pip install -r requirements.txt
```
5. Configure Java for M1 Mac:

```bash
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"' >> ~/.zshrc
source ~/.zshrc
```

## Usage
# Running the Jupyter Notebook
```bash
cd spark_demo
jupyter notebook
```

## Running the Demo Script
```bash
cd spark_demo
python scripts/demo_spark.py
```