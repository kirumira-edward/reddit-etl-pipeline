import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Reddit API credentials
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'MyRedditScraper v1.0')

# AWS credentials and settings
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# S3 paths
S3_BRONZE_PREFIX = 'bronze'
S3_SILVER_PREFIX = 'silver'
S3_GOLD_PREFIX = 'gold'

# Local data paths (for backup/local processing)
LOCAL_PATH = 'data'
BRONZE_PATH = os.path.join(LOCAL_PATH, 'bronze')
SILVER_PATH = os.path.join(LOCAL_PATH, 'silver')
GOLD_PATH = os.path.join(LOCAL_PATH, 'gold')
LOG_PATH = 'logs'

# Athena settings
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE', 'reddit_analytics')
ATHENA_OUTPUT_LOCATION = f"s3://{S3_BUCKET_NAME}/athena_results/"  # Changed path
ATHENA_CATALOG_NAME = os.getenv('ATHENA_CATALOG_NAME', 'AwsDataCatalog')

# Create directories if they don't exist
for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, LOG_PATH]:
    os.makedirs(path, exist_ok=True)