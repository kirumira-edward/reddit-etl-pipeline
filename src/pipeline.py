import os
import logging
from datetime import datetime
from src.extract import extract_reddit_data
from src.transform import transform_reddit_data
from src.load import load_reddit_aggregates
from config.settings import S3_BUCKET_NAME

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', f'pipeline_{datetime.now().strftime("%Y%m%d")}.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_reddit_pipeline(subreddit_name, post_limit=100):
    logger.info(f"Starting Reddit ETL pipeline for r/{subreddit_name}")
    start_time = datetime.now()
    
    try:
        # Step 1: Extract data
        logger.info("Starting extraction phase")
        bronze_location = extract_reddit_data(subreddit_name, post_limit)
        
        if bronze_location[0]:  # S3 bucket exists
            logger.info(f"Extraction phase completed. Data stored at: s3://{bronze_location[0]}/{bronze_location[1]}")
        else:
            logger.info(f"Extraction phase completed. Data stored locally at: {bronze_location[1]}")
        
        # Step 2: Transform data
        logger.info("Starting transformation phase")
        silver_location = transform_reddit_data(bronze_location)
        
        if silver_location[0]:  # S3 bucket exists
            logger.info(f"Transformation phase completed. Data stored at: s3://{silver_location[0]}/{silver_location[1]}")
        else:
            logger.info(f"Transformation phase completed. Data stored locally at: {silver_location[1]}")
        
        # Step 3: Load and aggregate data
        logger.info("Starting loading phase")
        gold_locations = load_reddit_aggregates(silver_location)
        
        # Log gold locations
        s3_count = sum(1 for loc in gold_locations if loc[0])
        local_count = len(gold_locations) - s3_count
        logger.info(f"Loading phase completed. Created {len(gold_locations)} aggregate datasets ({s3_count} in S3, {local_count} local)")
        
        # Calculate runtime
        end_time = datetime.now()
        runtime = (end_time - start_time).total_seconds()
        logger.info(f"Pipeline completed successfully in {runtime:.2f} seconds")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    # Run the pipeline for the "datascience" subreddit
    run_reddit_pipeline("datascience", post_limit=50)