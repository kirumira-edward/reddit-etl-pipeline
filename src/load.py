import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from config.settings import (
    SILVER_PATH, GOLD_PATH, 
    S3_BUCKET_NAME, S3_SILVER_PREFIX, S3_GOLD_PREFIX
)
from src.utils.s3_utils import (
    download_dataframe_from_s3_parquet, 
    upload_dataframe_to_s3_parquet
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'load.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_reddit_aggregates(source_location):
    """
    Load transformed data and create aggregated views for analysis
    
    Args:
        source_location (tuple): (bucket, key) for S3 or (None, local_path) for local file
        
    Returns:
        list: List of (bucket, key) tuples where aggregated data was saved
    """
    bucket, path = source_location
    logger.info(f"Starting loading process for data from {'S3' if bucket else 'local path'}")
    
    try:
        # Read the transformed data
        if bucket:
            # Read from S3
            df = download_dataframe_from_s3_parquet(bucket, path)
        else:
            # Read from local file
            df = pd.read_parquet(path)
        
        # Extract metadata from path
        if bucket:
            # Parse from S3 path
            path_parts = path.split('/')
            subreddit_idx = path_parts.index('reddit') + 1
            subreddit = path_parts[subreddit_idx]
        else:
            # Parse from local path
            path_parts = path.split(os.sep)
            subreddit_idx = path_parts.index('reddit') + 1
            subreddit = path_parts[subreddit_idx]
        
        # Create directory for local gold layer backup
        current_date = datetime.now().strftime('%Y/%m/%d')
        local_gold_dir = os.path.join(GOLD_PATH, 'reddit_aggregates', current_date)
        os.makedirs(local_gold_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_locations = []
        
        # 1. Daily post statistics
        daily_stats = df.groupby('date').agg({
            'id': 'count',
            'score': ['mean', 'max', 'sum'],
            'num_comments': ['mean', 'max', 'sum'],
            'title_length': 'mean',
            'selftext_length': 'mean',
            'upvote_ratio': 'mean'
        }).reset_index()
        
        # Flatten the MultiIndex columns
        daily_stats.columns = ['_'.join(col).strip('_') for col in daily_stats.columns.values]
        daily_stats['subreddit'] = subreddit
        
        # Save daily stats locally
        local_daily_stats_path = os.path.join(local_gold_dir, f'reddit_daily_stats_{subreddit}_{timestamp}.parquet')
        pq.write_table(pa.Table.from_pandas(daily_stats), local_daily_stats_path)
        logger.info(f"Daily stats saved locally to {local_daily_stats_path}")
        
        # Save to S3 gold layer
        s3_daily_stats_key = f"{S3_GOLD_PREFIX}/reddit_aggregates/daily_stats/{subreddit}/{current_date}/reddit_daily_stats_{subreddit}_{timestamp}.parquet"
        upload_success = upload_dataframe_to_s3_parquet(daily_stats, S3_BUCKET_NAME, s3_daily_stats_key)
        
        if upload_success:
            logger.info(f"Daily stats saved to S3: s3://{S3_BUCKET_NAME}/{s3_daily_stats_key}")
            output_locations.append((S3_BUCKET_NAME, s3_daily_stats_key))
        else:
            logger.warning(f"Failed to upload daily stats to S3, using local path instead")
            output_locations.append((None, local_daily_stats_path))
        
        # 2. Top posts by score
        top_posts = df.sort_values('score', ascending=False).head(20)
        
        # Save top posts locally
        local_top_posts_path = os.path.join(local_gold_dir, f'reddit_top_posts_{subreddit}_{timestamp}.parquet')
        pq.write_table(pa.Table.from_pandas(top_posts), local_top_posts_path)
        logger.info(f"Top posts saved locally to {local_top_posts_path}")
        
        # Save to S3
        s3_top_posts_key = f"{S3_GOLD_PREFIX}/reddit_aggregates/top_posts/{subreddit}/{current_date}/reddit_top_posts_{subreddit}_{timestamp}.parquet"
        upload_success = upload_dataframe_to_s3_parquet(top_posts, S3_BUCKET_NAME, s3_top_posts_key)
        
        if upload_success:
            logger.info(f"Top posts saved to S3: s3://{S3_BUCKET_NAME}/{s3_top_posts_key}")
            output_locations.append((S3_BUCKET_NAME, s3_top_posts_key))
        else:
            logger.warning(f"Failed to upload top posts to S3, using local path instead")
            output_locations.append((None, local_top_posts_path))
        
        # 3. Top posts by comment activity
        top_comments = df.sort_values('num_comments', ascending=False).head(20)
        
        # Save top commented posts locally
        local_top_comments_path = os.path.join(local_gold_dir, f'reddit_top_commented_{subreddit}_{timestamp}.parquet')
        pq.write_table(pa.Table.from_pandas(top_comments), local_top_comments_path)
        logger.info(f"Top commented posts saved locally to {local_top_comments_path}")
        
        # Save to S3
        s3_top_comments_key = f"{S3_GOLD_PREFIX}/reddit_aggregates/top_commented/{subreddit}/{current_date}/reddit_top_commented_{subreddit}_{timestamp}.parquet"
        upload_success = upload_dataframe_to_s3_parquet(top_comments, S3_BUCKET_NAME, s3_top_comments_key)
        
        if upload_success:
            logger.info(f"Top commented posts saved to S3: s3://{S3_BUCKET_NAME}/{s3_top_comments_key}")
            output_locations.append((S3_BUCKET_NAME, s3_top_comments_key))
        else:
            logger.warning(f"Failed to upload top commented posts to S3, using local path instead")
            output_locations.append((None, local_top_comments_path))
        
        logger.info(f"Created {len(output_locations)} aggregate views")
        
        return output_locations
        
    except Exception as e:
        logger.error(f"Error loading aggregated data: {str(e)}")
        raise