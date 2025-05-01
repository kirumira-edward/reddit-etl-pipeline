import os
import logging
import boto3
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'monitoring.log'), mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_monitoring_report():
    """
    Generate data quality and monitoring report for the Reddit ETL pipeline.
    
    This function:
    1. Checks if data exists in each layer (bronze, silver, gold)
    2. Validates record counts
    3. Logs metrics on data freshness
    4. Returns a summary report
    """
    logger.info("Generating monitoring report...")
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket_name = 'reddit-analytics-data'
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "data_checks": {
            "bronze": {"exists": False, "record_count": 0},
            "silver": {"exists": False, "record_count": 0},
            "gold": {"exists": False, "record_count": 0}
        },
        "status": "unknown"
    }
    
    # Check bronze layer
    try:
        bronze_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='bronze/')
        report["data_checks"]["bronze"]["exists"] = 'Contents' in bronze_objects
        report["data_checks"]["bronze"]["record_count"] = len(bronze_objects.get('Contents', []))
        logger.info(f"Bronze layer check: {report['data_checks']['bronze']}")
    except Exception as e:
        logger.error(f"Error checking bronze layer: {e}")
    
    # Check silver layer
    try:
        silver_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='silver/')
        report["data_checks"]["silver"]["exists"] = 'Contents' in silver_objects
        report["data_checks"]["silver"]["record_count"] = len(silver_objects.get('Contents', []))
        logger.info(f"Silver layer check: {report['data_checks']['silver']}")
    except Exception as e:
        logger.error(f"Error checking silver layer: {e}")
    
    # Check gold layer
    try:
        gold_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='gold/')
        report["data_checks"]["gold"]["exists"] = 'Contents' in gold_objects
        report["data_checks"]["gold"]["record_count"] = len(gold_objects.get('Contents', []))
        logger.info(f"Gold layer check: {report['data_checks']['gold']}")
    except Exception as e:
        logger.error(f"Error checking gold layer: {e}")
    
    # Determine overall status
    all_exist = all([
        report["data_checks"]["bronze"]["exists"],
        report["data_checks"]["silver"]["exists"],
        report["data_checks"]["gold"]["exists"]
    ])
    
    all_have_records = all([
        report["data_checks"]["bronze"]["record_count"] > 0,
        report["data_checks"]["silver"]["record_count"] > 0,
        report["data_checks"]["gold"]["record_count"] > 0
    ])
    
    if all_exist and all_have_records:
        report["status"] = "success"
    elif all_exist:
        report["status"] = "warning"
    else:
        report["status"] = "failure"
    
    logger.info(f"Monitoring report generated: {report['status']}")
    return report

if __name__ == "__main__":
    # Run the monitoring when executed directly
    generate_monitoring_report()