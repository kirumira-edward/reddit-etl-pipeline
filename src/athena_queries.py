import os
import logging
import pandas as pd
from src.utils.athena_utils import query_to_dataframe
from config.settings import ATHENA_DATABASE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'athena_queries.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def query_engagement_by_date(subreddit=None, start_date=None, end_date=None):
    """
    Query engagement metrics by date.
    
    Args:
        subreddit (str, optional): Filter for specific subreddit
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        
    Returns:
        pandas.DataFrame: Query results
    """
    query = f"""
    SELECT 
        date,
        subreddit,
        post_count,
        avg_score,
        total_score,
        avg_comments,
        total_comments,
        avg_upvote_ratio
    FROM 
        {ATHENA_DATABASE}.reddit_engagement_metrics
    WHERE 
        1=1
    """
    
    # Add filters if provided
    if subreddit:
        query += f" AND subreddit = '{subreddit}'"
    
    if start_date:
        query += f" AND date >= DATE('{start_date}')"
        
    if end_date:
        query += f" AND date <= DATE('{end_date}')"
    
    query += """
    ORDER BY 
        date DESC, subreddit
    """
    
    logger.info(f"Executing engagement query for subreddit: {subreddit or 'all'}")
    return query_to_dataframe(query)

def query_top_posts(subreddit=None, start_date=None, end_date=None, limit=10):
    """
    Query top posts by score.
    
    Args:
        subreddit (str, optional): Filter for specific subreddit
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        limit (int): Maximum number of posts to return
        
    Returns:
        pandas.DataFrame: Query results
    """
    query = f"""
    SELECT 
        id,
        title,
        score,
        num_comments,
        author,
        date,
        upvote_ratio,
        url,
        subreddit
    FROM 
        {ATHENA_DATABASE}.reddit_top_posts
    WHERE 
        1=1
    """
    
    # Add filters if provided
    if subreddit:
        query += f" AND subreddit = '{subreddit}'"
    
    if start_date:
        query += f" AND date >= DATE('{start_date}')"
        
    if end_date:
        query += f" AND date <= DATE('{end_date}')"
    
    query += f"""
    ORDER BY 
        score DESC
    LIMIT {limit}
    """
    
    logger.info(f"Executing top posts query for subreddit: {subreddit or 'all'}")
    return query_to_dataframe(query)

def query_active_discussions(subreddit=None, start_date=None, end_date=None, limit=10):
    """
    Query posts with the most comments.
    
    Args:
        subreddit (str, optional): Filter for specific subreddit
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        limit (int): Maximum number of posts to return
        
    Returns:
        pandas.DataFrame: Query results
    """
    query = f"""
    SELECT 
        id,
        title,
        score,
        num_comments,
        author,
        date,
        upvote_ratio,
        url,
        subreddit
    FROM 
        {ATHENA_DATABASE}.reddit_top_commented
    WHERE 
        1=1
    """
    
    # Add filters if provided
    if subreddit:
        query += f" AND subreddit = '{subreddit}'"
    
    if start_date:
        query += f" AND date >= DATE('{start_date}')"
        
    if end_date:
        query += f" AND date <= DATE('{end_date}')"
    
    query += f"""
    ORDER BY 
        num_comments DESC
    LIMIT {limit}
    """
    
    logger.info(f"Executing active discussions query for subreddit: {subreddit or 'all'}")
    return query_to_dataframe(query)

def query_post_length_vs_engagement(subreddit=None):
    """
    Analyze the correlation between post length and engagement.
    
    Args:
        subreddit (str, optional): Filter for specific subreddit
        
    Returns:
        pandas.DataFrame: Query results
    """
    query = f"""
    WITH posts AS (
        SELECT 
            title_length,
            selftext_length,
            score,
            num_comments,
            upvote_ratio,
            subreddit
        FROM 
            {ATHENA_DATABASE}.reddit_top_posts
        UNION ALL
        SELECT 
            title_length,
            selftext_length,
            score,
            num_comments,
            upvote_ratio,
            subreddit
        FROM 
            {ATHENA_DATABASE}.reddit_top_commented
    )
    SELECT 
        subreddit,
        AVG(title_length) as avg_title_length,
        AVG(selftext_length) as avg_selftext_length,
        AVG(score) as avg_score,
        AVG(num_comments) as avg_comments,
        AVG(upvote_ratio) as avg_upvote_ratio,
        CORR(title_length, score) as title_score_correlation,
        CORR(selftext_length, score) as content_score_correlation,
        CORR(title_length, num_comments) as title_comments_correlation,
        CORR(selftext_length, num_comments) as content_comments_correlation
    FROM 
        posts
    """
    
    # Add filter if provided
    if subreddit:
        query += f" WHERE subreddit = '{subreddit}'"
    
    query += """
    GROUP BY 
        subreddit
    ORDER BY 
        avg_score DESC
    """
    
    logger.info(f"Executing post length vs engagement analysis for subreddit: {subreddit or 'all'}")
    return query_to_dataframe(query)

def demo_queries(subreddit='datascience'):
    """Run demo queries and print results."""
    print(f"\n=== Running demo queries for r/{subreddit} ===\n")
    
    print("\n1. Engagement Metrics by Date:")
    engagement_df = query_engagement_by_date(subreddit)
    print(engagement_df.head())
    
    print("\n2. Top Posts by Score:")
    top_posts_df = query_top_posts(subreddit, limit=5)
    print(top_posts_df[['title', 'score', 'num_comments', 'author']].head())
    
    print("\n3. Most Active Discussions:")
    discussions_df = query_active_discussions(subreddit, limit=5)
    print(discussions_df[['title', 'score', 'num_comments', 'author']].head())
    
    print("\n4. Post Length vs. Engagement Analysis:")
    analysis_df = query_post_length_vs_engagement(subreddit)
    print(analysis_df)
    
    print("\n=== Demo queries complete ===\n")
    return {
        'engagement': engagement_df,
        'top_posts': top_posts_df,
        'discussions': discussions_df,
        'analysis': analysis_df
    }

if __name__ == "__main__":
    # Run demo queries when executed directly
    demo_queries()