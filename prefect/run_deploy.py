
import sys
sys.path.append('E:/DATA MODELLING')
from prefect.flows import reddit_etl_flow

if __name__ == "__main__":
    reddit_etl_flow.serve(
        name="reddit-analytics-daily",
        work_pool_name="reddit-pool",
        cron="0 2 * * *", 
        tags=["reddit", "etl"],
    )
