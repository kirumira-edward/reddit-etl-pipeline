import logging
import boto3
import pandas as pd
import time
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from botocore.exceptions import ClientError
from config.settings import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET_NAME, ATHENA_OUTPUT_LOCATION, ATHENA_DATABASE, ATHENA_CATALOG_NAME
)

# Import the S3 utilities
from src.utils.s3_utils import get_s3_client

logger = logging.getLogger(__name__)

def get_athena_client():
    """Create and return an Athena client."""
    return boto3.client(
        'athena',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def get_athena_connection():
    """Create and return a connection to Athena."""
    return connect(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        s3_staging_dir=ATHENA_OUTPUT_LOCATION,
        catalog_name=ATHENA_CATALOG_NAME,  # Add catalog name here
        schema_name=ATHENA_DATABASE,
        cursor_class=PandasCursor
    )

def ensure_athena_output_location():
    """
    Ensure the Athena query results location exists in S3.
    Creates an empty object to initialize the path if needed.
    
    Returns:
        bool: True if the location is ready, False if there was an error
    """
    s3_client = get_s3_client()
    
    try:
        # Extract the bucket and key from ATHENA_OUTPUT_LOCATION
        # Format is s3://bucket/path/
        path_parts = ATHENA_OUTPUT_LOCATION.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        
        # Create a test object to ensure the path exists
        if len(path_parts) > 1:
            prefix = path_parts[1]
            key = f"{prefix.rstrip('/')}/.athena_test_placeholder"
        else:
            key = ".athena_test_placeholder"
            
        # Check if the bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logger.error(f"S3 bucket {bucket} does not exist")
            elif error_code == 403:
                logger.error(f"No permission to access bucket {bucket}")
            else:
                logger.error(f"Error checking bucket {bucket}: {str(e)}")
            return False
            
        # Create placeholder file
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body="Athena query results placeholder"
            )
            logger.info(f"Athena output location is ready: {ATHENA_OUTPUT_LOCATION}")
            return True
        except ClientError as e:
            logger.error(f"Error creating placeholder in {bucket}/{key}: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Error ensuring Athena output location: {str(e)}")
        return False

def create_database_if_not_exists(database_name=ATHENA_DATABASE):
    """
    Create an Athena database if it doesn't exist.
    
    Args:
        database_name (str): Name of the database to create
        
    Returns:
        bool: True if successful, False otherwise
    """
    client = get_athena_client()
    try:
        # Check if the database exists - add CatalogName parameter
        response = client.list_databases(
            CatalogName=ATHENA_CATALOG_NAME
        )
        databases = [db['Name'] for db in response['DatabaseList']]
        
        if database_name in databases:
            logger.info(f"Database {database_name} already exists")
            return True
        
        # Create the database
        query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        execution_id = execute_query(query)
        
        if wait_for_query_completion(execution_id):
            logger.info(f"Database {database_name} created successfully")
            return True
        else:
            logger.error(f"Failed to create database {database_name}")
            return False
            
    except ClientError as e:
        logger.error(f"Error creating database: {str(e)}")
        return False

def execute_query(query, database=ATHENA_DATABASE):
    """Execute a query in Athena."""
    client = get_athena_client()
    
    try:
        # Remove the ResultConfiguration to use workgroup defaults
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database,
                'Catalog': ATHENA_CATALOG_NAME
            },
            WorkGroup='primary'  # Use the primary workgroup's default settings
        )
        return response['QueryExecutionId']
    except ClientError as e:
        logger.error(f"Error executing query: {str(e)}")
        raise

def get_existing_databases():
    """Get list of existing databases."""
    client = get_athena_client()
    try:
        response = client.list_databases(
            CatalogName=ATHENA_CATALOG_NAME
        )
        return [db['Name'] for db in response['DatabaseList']]
    except ClientError as e:
        logger.error(f"Error listing databases: {str(e)}")
        return ['default']

def wait_for_query_completion(execution_id, timeout=30):
    """
    Wait for an Athena query to complete.
    
    Args:
        execution_id (str): Query execution ID
        timeout (int): Maximum time to wait in seconds
        
    Returns:
        bool: True if query succeeded, False otherwise
    """
    client = get_athena_client()
    
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        response = client.get_query_execution(QueryExecutionId=execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            return True
        elif state in ['FAILED', 'CANCELLED']:
            logger.error(f"Query {execution_id} {state}: {response['QueryExecution']['Status'].get('StateChangeReason', '')}")
            return False
            
        # Wait a bit before checking again
        time.sleep(1)
    
    logger.error(f"Query {execution_id} timed out after {timeout} seconds")
    return False

def query_to_dataframe(query, database=ATHENA_DATABASE):
    """
    Execute a query and return the results as a pandas DataFrame.
    
    Args:
        query (str): SQL query to execute
        database (str): Database to use
        
    Returns:
        pandas.DataFrame: Query results
    """
    try:
        conn = get_athena_connection()
        return pd.read_sql(query, conn)
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise

def create_table_from_parquet(
    table_name, 
    s3_location, 
    database=ATHENA_DATABASE,
    partition_fields=None
):
    """
    Create an external table in Athena from Parquet files in S3.
    
    Args:
        table_name (str): Name for the new table
        s3_location (str): S3 path where the data is stored
        database (str): Database to create the table in
        partition_fields (list): Optional list of partition fields
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create the database if it doesn't exist
        create_database_if_not_exists(database)
        
        # Format the S3 location to ensure it ends with a slash
        if not s3_location.endswith('/'):
            s3_location += '/'
            
        # Start building the query
        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
        """
        
        # For now, we'll use hardcoded schemas based on our knowledge of the data
        if "daily_stats" in table_name:
            query += """
            date DATE,
            id_count INT,
            score_mean DOUBLE,
            score_max INT,
            score_sum INT,
            num_comments_mean DOUBLE,
            num_comments_max INT,
            num_comments_sum INT,
            title_length_mean DOUBLE,
            selftext_length_mean DOUBLE,
            upvote_ratio_mean DOUBLE,
            subreddit STRING
            """
        elif "top_posts" in table_name or "top_commented" in table_name:
            query += """
            id STRING,
            title STRING,
            score INT,
            num_comments INT,
            selftext STRING,
            upvote_ratio DOUBLE,
            url STRING,
            author STRING,
            is_original_content BOOLEAN,
            is_self BOOLEAN,
            extracted_at TIMESTAMP,
            created_at TIMESTAMP,
            date DATE,
            title_length INT,
            selftext_length INT,
            has_selftext BOOLEAN,
            subreddit STRING
            """
        else:
            logger.error(f"Unknown table type: {table_name}")
            return False
            
        # Add partition fields if specified
        if partition_fields and len(partition_fields) > 0:
            partition_clause = ", ".join([f"{field['name']} {field['type']}" for field in partition_fields])
            query += f"""
            )
            PARTITIONED BY ({partition_clause})
            """
        else:
            query += """
            )
            """
            
        # Complete the table definition
        query += f"""
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION '{s3_location}'
        TBLPROPERTIES ('has_encrypted_data'='false');
        """
        
        # Execute the query
        execution_id = execute_query(query, database)
        success = wait_for_query_completion(execution_id)
        
        if success:
            logger.info(f"Table {database}.{table_name} created successfully")
            
            # If we have partitions, we need to load them
            if partition_fields:
                repair_query = f"MSCK REPAIR TABLE {database}.{table_name};"
                repair_id = execute_query(repair_query, database)
                repair_success = wait_for_query_completion(repair_id)
                
                if not repair_success:
                    logger.warning(f"Failed to load partitions for {database}.{table_name}")
            
            return True
        else:
            logger.error(f"Failed to create table {database}.{table_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {str(e)}")
        return False