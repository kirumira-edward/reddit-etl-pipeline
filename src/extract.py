import os
import json
import logging
import praw
from datetime import datetime, timedelta
import pandas as pd
from config.settings import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, 
    BRONZE_PATH, S3_BUCKET_NAME, S3_BRONZE_PREFIX
)
from src.utils.s3_utils import upload_json_to_s3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'extract.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_reddit_data(subreddit_name, post_limit=100):
    """
    Extract posts from a specified subreddit and save to S3
    
    Args:
        subreddit_name (str): Name of the subreddit to extract posts from
        post_limit (int): Maximum number of posts to extract
        
    Returns:
        tuple: (S3 bucket, S3 object key) where data was saved
    """
    logger.info(f"Starting data extraction from r/{subreddit_name}")
    
    try:
        # Initialize Reddit API client
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        
        # Access the subreddit
        subreddit = reddit.subreddit(subreddit_name)
        
        # Extract posts
        posts = []
        for post in subreddit.hot(limit=post_limit):
            posts.append({
                'id': post.id,
                'title': post.title,
                'score': post.score,
                'num_comments': post.num_comments,
                'created_utc': post.created_utc,
                'selftext': post.selftext,
                'upvote_ratio': post.upvote_ratio,
                'url': post.url,
                'author': str(post.author),
                'is_original_content': post.is_original_content,
                'is_self': post.is_self,
                'extracted_at': datetime.now().isoformat()
            })
        
        logger.info(f"Extracted {len(posts)} posts from r/{subreddit_name}")
        
        # Prepare metadata
        current_date = datetime.now().strftime('%Y/%m/%d/%H')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Prepare JSON data
        json_data = {
            'source': 'reddit',
            'subreddit': subreddit_name,
            'extracted_at': datetime.now().isoformat(),
            'record_count': len(posts),
            'posts': posts
        }
        
        # Save to local bronze layer (backup)
        local_directory = os.path.join(BRONZE_PATH, 'reddit', subreddit_name, current_date)
        os.makedirs(local_directory, exist_ok=True)
        local_path = os.path.join(local_directory, f'reddit_{subreddit_name}_{timestamp}.json')
        with open(local_path, 'w') as f:
            json.dump(json_data, f, indent=2)
        logger.info(f"Raw data saved locally to {local_path}")
        
        # Save to S3 bronze layer
        s3_object_key = f"{S3_BRONZE_PREFIX}/reddit/{subreddit_name}/{current_date}/reddit_{subreddit_name}_{timestamp}.json"
        upload_success = upload_json_to_s3(json_data, S3_BUCKET_NAME, s3_object_key)
        
        if upload_success:
            logger.info(f"Raw data saved to S3: s3://{S3_BUCKET_NAME}/{s3_object_key}")
            return (S3_BUCKET_NAME, s3_object_key)
        else:
            logger.warning(f"Failed to upload to S3, using local path instead: {local_path}")
            return (None, local_path)
        
    except Exception as e:
        logger.error(f"Error extracting data from Reddit: {str(e)}")
        raise