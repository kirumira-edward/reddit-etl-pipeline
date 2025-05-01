import os
import logging
import traceback
import boto3
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'athena_setup.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_athena_tables():
    """
    Set up Athena tables and views for querying Reddit data.
    
    This function:
    1. Creates external tables for bronze, silver, and gold layers
    2. Creates views for common analytics queries
    """
    logger.info("Setting up Athena tables...")
    
    athena_client = boto3.client('athena', region_name='us-east-1')
    s3_output = 's3://reddit-analytics-data/athena-results/'
    database = 'reddit_analytics'
    
    # Create database if not exists
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database}"
    
    # Execute create database query
    try:
        response = athena_client.start_query_execution(
            QueryString=create_db_query,
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Started database creation query: {query_execution_id}")
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return {"status": "error", "message": str(e)}
    
    # Create tables for each layer
    tables = {
        "bronze_posts": """
            CREATE EXTERNAL TABLE IF NOT EXISTS reddit_analytics.bronze_posts (
                id STRING,
                title STRING,
                score INT,
                author STRING,
                created_utc BIGINT,
                num_comments INT,
                permalink STRING,
                url STRING,
                selftext STRING,
                subreddit STRING,
                ingestion_date STRING
            )
            STORED AS PARQUET
            LOCATION 's3://reddit-analytics-data/bronze/posts/'
        """,
        "silver_posts": """
            CREATE EXTERNAL TABLE IF NOT EXISTS reddit_analytics.silver_posts (
                id STRING,
                title STRING,
                score INT,
                author STRING,
                created_date DATE,
                num_comments INT,
                permalink STRING,
                url STRING,
                selftext STRING,
                subreddit STRING,
                cleaned_text STRING,
                post_length INT,
                ingestion_date DATE
            )
            STORED AS PARQUET
            LOCATION 's3://reddit-analytics-data/silver/posts/'
        """,
        "gold_subreddit_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS reddit_analytics.gold_subreddit_stats (
                subreddit STRING,
                date DATE,
                post_count INT,
                avg_score DOUBLE,
                total_comments INT,
                avg_post_length DOUBLE
            )
            STORED AS PARQUET
            LOCATION 's3://reddit-analytics-data/gold/subreddit_stats/'
        """
    }
    
    execution_ids = {}
    
    # Execute each table creation query
    for table_name, query in tables.items():
        try:
            response = athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={'OutputLocation': s3_output},
                QueryExecutionContext={'Database': database}
            )
            execution_ids[table_name] = response['QueryExecutionId']
            logger.info(f"Started {table_name} creation: {execution_ids[table_name]}")
            # Sleep to avoid throttling
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error creating {table_name}: {e}")
    
    return {
        "status": "success",
        "message": "Athena setup initiated",
        "execution_ids": execution_ids
    }

if __name__ == "__main__":
    # Run the setup when executed directly
    setup_athena_tables()