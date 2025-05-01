# Reddit ETL Pipeline

A professional data engineering project that extracts data from Reddit, processes it through a medallion architecture, and loads it into AWS S3 with Athena for analytics queryability.

## Project Overview

This pipeline extracts posts from multiple subreddits (datascience, machinelearning, python), transforms the data through bronze/silver/gold layers, and makes it accessible for analytics via AWS Athena. The entire workflow is orchestrated using Prefect, providing reliable scheduling and monitoring.

## Architecture

The pipeline follows a medallion architecture:

1. **Bronze Layer**: Raw data extracted from Reddit
2. **Silver Layer**: Cleansed and standardized data
3. **Gold Layer**: Analytics-ready aggregated data

## Features

- **Multi-Subreddit Data Extraction**: Pulls data from multiple programming and data science communities
- **Data Transformation**: Cleans, standardizes, and enriches Reddit post data
- **Data Quality Monitoring**: Validates data across all processing layers
- **AWS Integration**: Leverages S3 for data lake storage and Athena for SQL analytics
- **Workflow Orchestration**: Complete pipeline management with Prefect 3.3.7

## Tech Stack

- **Python 3.11**: Core programming language
- **Prefect 3.3.7**: Workflow orchestration
- **AWS Services**: 
  - S3 for data storage
  - Athena for SQL queries
- **Data Processing**: 
  - Pandas for transformation
  - PyArrow for Parquet file handling
- **PRAW**: Reddit API wrapper

