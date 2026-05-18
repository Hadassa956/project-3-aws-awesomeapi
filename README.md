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
[AwesomeAPI] ──(Airflow)──► [S3 Bronze Layer] ──(Airflow/PyArrow)──► [S3 Silver Layer] ──(Airflow/PyArrow)──► [S3 Gold Layer]
                                     │                                         │                                       │
                              (Glue Crawler)                            (Glue Crawler)                          (Glue Crawler)
                                     │                                         │                                       │
                                     ▼                                         ▼                                       ▼
                              [Data Catalog]                            [Data Catalog]                          [Data Catalog]
                                                                                                                       │
                                                                                                                       ▼
                                                                                                                [Amazon Athena]
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
FROM gold
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
