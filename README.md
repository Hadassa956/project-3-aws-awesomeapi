# project-3-aws-awesomeapi

## AWS Medallion Architecture Pipeline

## Overview

This project implements an end-to-end data engineering pipeline on AWS using a Medallion Architecture approach.

The pipeline extracts real-time foreign exchange quotation data from the AwesomeAPI public endpoint, stores raw data in Amazon S3, transforms and enriches the data into Silver and Gold layers, and catalogs the datasets using AWS Glue Crawlers for querying through Amazon Athena.

The entire infrastructure was provisioned with Terraform.

---

# Architecture

## Technologies

* Apache Airflow
* AWS S3
* AWS Glue Crawlers
* AWS Glue Data Catalog
* Amazon Athena
* Terraform
* PyArrow
* Parquet
* Python

---

# Medallion Layers

## Bronze Layer

Raw JSON data ingested directly from the API.

Characteristics:

* Immutable raw ingestion
* Partitioned by ingestion date
* Stored in JSON format
* UTC standardized timestamps

Example:

```text
bronze/year=2026/month=05/day=18/
```

---

## Silver Layer

Cleaned and standardized data.

Transformations:

* Numeric type conversion
* Schema normalization
* Ingestion timestamp enrichment
* Conversion to Parquet
* Snappy compression

Example:

```text
silver/year=2026/month=05/day=18/
```

---

## Gold Layer

Business-oriented curated dataset.

Business rules:

* Spread calculation
* Volatility classification
* Analytical schema simplification

Example:

```text
gold/year=2026/month=05/day=18/
```

---

# Pipeline Flow

```text
AwesomeAPI
   ↓
Airflow DAG
   ↓
S3 Bronze Layer
   ↓
Glue Crawler
   ↓
S3 Silver Layer
   ↓
Glue Crawler
   ↓
S3 Gold Layer
   ↓
Glue Crawler
   ↓
Glue Data Catalog
   ↓
Amazon Athena
```

---

# Features

* End-to-end orchestration with Apache Airflow
* Infrastructure as Code with Terraform
* AWS Glue Catalog integration
* Athena-ready Parquet datasets
* S3 partitioning strategy
* Structured logging
* Retry strategy and timeout handling
* Medallion Architecture implementation
* IAM least privilege approach

---

# Infrastructure Provisioning

Infrastructure resources created with Terraform:

* S3 Data Lake
* IAM Roles and Policies
* Glue Crawlers
* Glue Catalog Database
* S3 Layer Structure

---

# Example Athena Query

```sql
SELECT *
FROM awesomeapi_gold
WHERE volatility = 'HIGH';
```

---

# Key Learnings

This project focused on practical cloud data engineering concepts such as:

* Data Lake organization
* AWS Glue Catalog integration
* Parquet optimization
* Partitioning strategies
* Airflow orchestration
* Infrastructure as Code
* Cloud-native ETL design
* Data pipeline reliability

---

# Future Improvements

Possible future enhancements:

* Terraform modularization
* CI/CD integration
* Great Expectations data validation
* Dockerized Airflow deployment
* Lake Formation governance
* Incremental processing strategy
* Monitoring and alerting

---

# LinkedIn Post

I’ve just finished building an end-to-end AWS data engineering pipeline using a Medallion Architecture approach.

The project extracts real-time foreign exchange quotation data from AwesomeAPI, orchestrates the entire workflow with Apache Airflow, stores the data in Amazon S3 across Bronze, Silver, and Gold layers, catalogs the datasets with AWS Glue Crawlers, and makes them queryable through Amazon Athena.

The infrastructure was provisioned with Terraform.

Main concepts explored in the project:

* Apache Airflow orchestration
* AWS Glue Crawlers and Data Catalog
* Amazon Athena
* S3 partitioning strategies
* Parquet optimization
* Medallion Architecture
* Infrastructure as Code
* IAM least privilege principles
* Cloud-native ETL design

One of the most valuable parts of the project was dealing with real cloud engineering issues during implementation:

* AWS region configuration
* Glue crawler behavior
* schema inference
* partitioning structure
* IAM permissions
* Athena catalog integration

Building the pipeline itself was important, but understanding the operational behavior of AWS services was what made the experience especially valuable.

Repository:

[add your GitHub repository here]

Architecture diagram:

[add architecture image here]
