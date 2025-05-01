from prefect import flow, task
from datetime import timedelta
import sys
import os

# Add project root to PYTHONPATH
sys.path.append('E:/DATA MODELLING')

from src.pipeline import run_reddit_pipeline
from src.athena_setup import setup_athena_tables
from src.monitoring import generate_monitoring_report

@task(retries=1, retry_delay_seconds=300, name="Extract Reddit Data")
def extract_subreddit_data(subreddit: str, post_limit: int = 100):
    """Extract data for a specific subreddit"""
    print(f"Extracting data for subreddit: {subreddit}")
    
    # Fix: Check which parameters run_reddit_pipeline actually accepts
    # Option 1: If it uses a different parameter name
    return run_reddit_pipeline(subreddit_name=subreddit, post_limit=post_limit)
    
    # Option 2: If it takes positional arguments
    # return run_reddit_pipeline(subreddit, post_limit)
    
    # Option 3: If it doesn't accept subreddit as a parameter
    # return run_reddit_pipeline(post_limit=post_limit)  # Maybe subreddit is configured elsewhere

@task(name="Set Up Athena Tables")
def setup_athena():
    """Set up Athena tables and views"""
    print("Setting up Athena tables and views")
    return setup_athena_tables()

@task(name="Generate Monitoring Report")
def monitor_data_quality():
    """Run data quality checks and generate a report"""
    print("Running data quality checks")
    return generate_monitoring_report()

@flow(name="Reddit Analytics Pipeline", 
      description="Reddit ETL pipeline with medallion architecture")
def reddit_etl_flow():
    """Main ETL flow for Reddit analytics"""
    # Define subreddits to process
    subreddits = ['datascience', 'machinelearning', 'python']
    
    # Extract data for each subreddit
    extraction_results = []
    for subreddit in subreddits:
        result = extract_subreddit_data(subreddit, post_limit=100)
        extraction_results.append(result)
    
    # Set up Athena tables
    athena_result = setup_athena()
    
    # Generate monitoring report
    monitoring_result = monitor_data_quality()
    
    return {
        "extractions": extraction_results,
        "athena_setup": athena_result,
        "monitoring": monitoring_result
    }

if __name__ == "__main__":
    # For manual testing
    reddit_etl_flow()