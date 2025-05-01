import os
import json
import logging
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from config.settings import (
    BRONZE_PATH, SILVER_PATH, 
    S3_BUCKET_NAME, S3_BRONZE_PREFIX, S3_SILVER_PREFIX
)
from src.utils.s3_utils import (
    download_json_from_s3, upload_dataframe_to_s3_parquet
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'transform.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def transform_reddit_data(source_location):
    """
    Transform raw Reddit data into a clean, structured format
    
    Args:
        source_location (tuple): (bucket, key) for S3 or (None, local_path) for local file
        
    Returns:
        tuple: (S3 bucket, S3 object key) where transformed data was saved
    """
    bucket, path = source_location
    logger.info(f"Starting transformation of data from {'S3' if bucket else 'local path'}")
    
    try:
        # Load raw data
        if bucket:
            # Load from S3
            raw_data = download_json_from_s3(bucket, path)
        else:
            # Load from local file
            with open(path, 'r') as f:
                raw_data = json.load(f)
        
        # Extract metadata
        source = raw_data['source']
        subreddit = raw_data['subreddit']
        
        # Convert posts to DataFrame
        df = pd.DataFrame(raw_data['posts'])
        
        # Basic data cleaning and transformation
        # 1. Convert timestamps to datetime
        df['created_at'] = pd.to_datetime(df['created_utc'], unit='s')
        df['extracted_at'] = pd.to_datetime(df['extracted_at'])
        df['date'] = df['created_at'].dt.date
        
        # 2. Clean and normalize text fields
        df['title'] = df['title'].str.strip()
        df['selftext'] = df['selftext'].fillna('').str.strip()
        
        # 3. Handle missing values
        df['author'] = df['author'].fillna('deleted')
        
        # 4. Create derived features
        df['title_length'] = df['title'].str.len()
        df['selftext_length'] = df['selftext'].str.len()
        df['has_selftext'] = df['selftext_length'] > 0
        
        # 5. Drop unnecessary columns
        df = df.drop(['created_utc'], axis=1)
        
        # Determine output path structure
        current_date = datetime.now().strftime('%Y/%m/%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save locally as backup
        local_directory = os.path.join(SILVER_PATH, 'reddit', subreddit, current_date)
        os.makedirs(local_directory, exist_ok=True)
        local_path = os.path.join(local_directory, f'reddit_{subreddit}_{timestamp}.parquet')
        
        # Save locally
        table = pa.Table.from_pandas(df)
        pq.write_table(table, local_path)
        logger.info(f"Transformed data saved locally to {local_path}")
        
        # Save to S3 silver layer
        s3_object_key = f"{S3_SILVER_PREFIX}/reddit/{subreddit}/{current_date}/reddit_{subreddit}_{timestamp}.parquet"
        upload_success = upload_dataframe_to_s3_parquet(df, S3_BUCKET_NAME, s3_object_key)
        
        if upload_success:
            logger.info(f"Transformed data saved to S3: s3://{S3_BUCKET_NAME}/{s3_object_key}")
            return (S3_BUCKET_NAME, s3_object_key)
        else:
            logger.warning(f"Failed to upload to S3, using local path instead: {local_path}")
            return (None, local_path)
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise