import json
import logging
import boto3
import pandas as pd
import io
from botocore.exceptions import ClientError
from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

logger = logging.getLogger(__name__)

def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def upload_file_to_s3(file_path, bucket, object_key):
    """
    Upload a local file to S3
    
    Args:
        file_path (str): Path to local file
        bucket (str): S3 bucket name
        object_key (str): S3 object key (path in bucket)
        
    Returns:
        bool: True if upload was successful
    """
    s3_client = get_s3_client()
    try:
        s3_client.upload_file(file_path, bucket, object_key)
        logger.info(f"Uploaded {file_path} to s3://{bucket}/{object_key}")
        return True
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        return False

def upload_json_to_s3(json_data, bucket, object_key):
    """
    Upload JSON data directly to S3
    
    Args:
        json_data (dict): JSON-serializable dictionary
        bucket (str): S3 bucket name
        object_key (str): S3 object key (path in bucket)
        
    Returns:
        bool: True if upload was successful
    """
    s3_client = get_s3_client()
    try:
        # Convert dict to JSON string
        json_str = json.dumps(json_data, indent=2)
        
        # Upload as bytes
        s3_client.put_object(
            Body=json_str,
            Bucket=bucket,
            Key=object_key
        )
        logger.info(f"Uploaded JSON data to s3://{bucket}/{object_key}")
        return True
    except ClientError as e:
        logger.error(f"Error uploading JSON to S3: {str(e)}")
        return False

def download_json_from_s3(bucket, object_key):
    """
    Download and parse JSON data from S3
    
    Args:
        bucket (str): S3 bucket name
        object_key (str): S3 object key (path in bucket)
        
    Returns:
        dict: Parsed JSON data
    """
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Downloaded JSON data from s3://{bucket}/{object_key}")
        return json_data
    except ClientError as e:
        logger.error(f"Error downloading JSON from S3: {str(e)}")
        raise

def upload_dataframe_to_s3_parquet(df, bucket, object_key):
    """
    Upload pandas DataFrame directly to S3 as Parquet
    
    Args:
        df (pandas.DataFrame): DataFrame to upload
        bucket (str): S3 bucket name
        object_key (str): S3 object key (path in bucket)
        
    Returns:
        bool: True if upload was successful
    """
    s3_client = get_s3_client()
    try:
        # Convert DataFrame to parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(
            Body=parquet_buffer.getvalue(),
            Bucket=bucket,
            Key=object_key
        )
        logger.info(f"Uploaded DataFrame to s3://{bucket}/{object_key}")
        return True
    except Exception as e:
        logger.error(f"Error uploading DataFrame to S3: {str(e)}")
        return False

def download_dataframe_from_s3_parquet(bucket, object_key):
    """
    Download Parquet data from S3 and load into DataFrame
    
    Args:
        bucket (str): S3 bucket name
        object_key (str): S3 object key (path in bucket)
        
    Returns:
        pandas.DataFrame: DataFrame containing the data
    """
    s3_client = get_s3_client()
    try:
        # Download parquet object
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        parquet_buffer = io.BytesIO(response['Body'].read())
        
        # Load into DataFrame
        df = pd.read_parquet(parquet_buffer)
        logger.info(f"Downloaded DataFrame from s3://{bucket}/{object_key}")
        return df
    except Exception as e:
        logger.error(f"Error downloading DataFrame from S3: {str(e)}")
        raise