import sys
import subprocess
import os

# Add the parent directory to the path so we can import the flow
sys.path.append('E:/DATA MODELLING')

# Import the flow directly
sys.path.append('E:/DATA MODELLING/prefect/flows')
from reddit_etl_flow import reddit_etl_flow

if __name__ == "__main__":
    # First create a work pool if it doesn't exist
    try:
        # Create process pool
        subprocess.run(["prefect", "work-pool", "create", "reddit-pool", "--type", "process"], check=True)
        print("Created work pool: reddit-pool")
    except subprocess.CalledProcessError:
        print("Work pool may already exist, continuing...")
    
    # Use the in-process API which is more reliable
    print("Creating deployment...")
    
    # Try with minimal parameters to avoid naming conflicts in Prefect 3.3.7
    reddit_etl_flow.serve(
        name="reddit-analytics-daily",
        # Remove the pool parameter completely
        cron="0 2 * * *", 
        tags=["reddit", "etl"],
    )
    
    print("Deployment created successfully!")