import os
import sys
import logging
from datetime import datetime
from src.pipeline import run_reddit_pipeline
from src.athena_setup import setup_athena_tables
from src.athena_queries import demo_queries
from config.settings import S3_BUCKET_NAME, LOG_PATH, ATHENA_DATABASE, AWS_REGION, ATHENA_OUTPUT_LOCATION

def setup_logging():
    """Set up logging for the main script."""
    # Create logs directory if it doesn't exist
    os.makedirs(LOG_PATH, exist_ok=True)
    
    # Configure logging
    log_file = os.path.join(LOG_PATH, f'etl_main_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def print_usage():
    """Print usage information."""
    print("\nUsage:")
    print("  python main.py extract SUBREDDIT [POST_LIMIT]")
    print("  python main.py athena setup")
    print("  python main.py athena query [SUBREDDIT]")
    print("\nExamples:")
    print("  python main.py extract datascience 100")
    print("  python main.py athena setup")
    print("  python main.py athena query datascience\n")

def main():
    """Main entry point for the Reddit ETL and analytics pipeline."""
    logger = setup_logging()
    
    # Check for minimum arguments
    if len(sys.argv) < 2:
        logger.error("Missing required arguments")
        print_usage()
        sys.exit(1)
    
    # Get the command
    command = sys.argv[1].lower()
    
    # Handle extract command
    if command == "extract":
        if len(sys.argv) < 3:
            logger.error("Missing required argument: subreddit")
            print_usage()
            sys.exit(1)
            
        # Get subreddit from command line arguments
        subreddit = sys.argv[2]
        
        # Get optional post limit from command line arguments
        post_limit = int(sys.argv[3]) if len(sys.argv) > 3 else 50
        
        # Display configuration
        logger.info(f"Starting ETL process with configuration:")
        logger.info(f"- Subreddit: {subreddit}")
        logger.info(f"- Post limit: {post_limit}")
        logger.info(f"- S3 bucket: {S3_BUCKET_NAME}")
        
        print(f"Running Reddit ETL pipeline for r/{subreddit} with limit {post_limit}")
        print(f"Using S3 bucket: {S3_BUCKET_NAME}")
        
        # Execute the pipeline
        start_time = datetime.now()
        success = run_reddit_pipeline(subreddit, post_limit)
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if success:
            logger.info(f"Pipeline executed successfully in {execution_time:.2f} seconds!")
            print(f"\n✅ Pipeline executed successfully in {execution_time:.2f} seconds!")
            print(f"Data available in S3 bucket: {S3_BUCKET_NAME}")
        else:
            logger.error(f"Pipeline execution failed after {execution_time:.2f} seconds")
            print(f"\n❌ Pipeline execution failed after {execution_time:.2f} seconds")
            print("Check logs for details.")
            sys.exit(1)
            
    # Handle athena commands
    elif command == "athena":
        if len(sys.argv) < 3:
            logger.error("Missing athena subcommand")
            print_usage()
            sys.exit(1)
            
        athena_command = sys.argv[2].lower()
        
        # Handle athena setup
        if athena_command == "setup":
            print(f"Setting up Athena tables in database: {ATHENA_DATABASE}")
            print(f"Using S3 bucket: {S3_BUCKET_NAME}")
            print(f"AWS region: {AWS_REGION}")
            print(f"Athena results location: {ATHENA_OUTPUT_LOCATION}")
            
            # Execute the Athena setup
            start_time = datetime.now()
            success = setup_athena_tables()
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            if success:
                logger.info(f"Athena setup completed successfully in {execution_time:.2f} seconds!")
                print(f"\n✅ Athena setup completed successfully in {execution_time:.2f} seconds!")
                print(f"Tables created in database: {ATHENA_DATABASE}")
            else:
                logger.error(f"Athena setup failed after {execution_time:.2f} seconds")
                print(f"\n❌ Athena setup failed after {execution_time:.2f} seconds")
                print("Check logs for details.")
                sys.exit(1)
                
        # Handle athena query
        elif athena_command == "query":
            # Get optional subreddit filter
            subreddit = sys.argv[3] if len(sys.argv) > 3 else 'datascience'
            
            print(f"Running demo queries on Athena database: {ATHENA_DATABASE}")
            print(f"Filtering for subreddit: r/{subreddit}")
            
            # Execute the demo queries
            start_time = datetime.now()
            try:
                results = demo_queries(subreddit)
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                logger.info(f"Athena queries completed successfully in {execution_time:.2f} seconds!")
                print(f"\n✅ Athena queries completed successfully in {execution_time:.2f} seconds!")
            except Exception as e:
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                logger.error(f"Athena queries failed after {execution_time:.2f} seconds: {str(e)}")
                print(f"\n❌ Athena queries failed after {execution_time:.2f} seconds")
                print(f"Error: {str(e)}")
                print("Check logs for details.")
                sys.exit(1)
        
        else:
            logger.error(f"Unknown athena subcommand: {athena_command}")
            print_usage()
            sys.exit(1)
            
    else:
        logger.error(f"Unknown command: {command}")
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main()