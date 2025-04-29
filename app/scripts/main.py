import os
import pandas as pd
import openai
from dotenv import load_dotenv
from utils import *
import asyncio
import os

if __name__ == "__main__":
    load_dotenv(dotenv_path=".env")

    openai.api_key = os.getenv("OPENAI_API_KEY")
    RAW_PATH = os.getenv("RAW_PATH")
    STAGING_PATH = os.getenv("STAGING_PATH")
    SENT_PATH = os.getenv("SENT_PATH")
    APPLIED_PATH = os.getenv("APPLIED_PATH")

    datetime_now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    if not openai.api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set.")

    # create the directories if they do not exist
    os.makedirs(RAW_PATH, exist_ok=True)
    os.makedirs(STAGING_PATH, exist_ok=True)
    
    query_url = "https://www.upwork.com/nx/search/jobs/?amount=100-499,500-999,1000-4999,5000-&q=statistics&t=0,1&page=1&per_page=50"  # Replace with your query URL
    asyncio.run(get_upwork_jobs(query_url, RAW_PATH))
    
    query_url = "https://www.upwork.com/nx/search/jobs/?amount=100-499,500-999,1000-4999,5000-&per_page=50&q=data%20analyst&t=0,1"  # Replace with your query URL
    asyncio.run(get_upwork_jobs(query_url, RAW_PATH))

    query_url = "https://www.upwork.com/nx/search/jobs/?amount=100-499,500-999,1000-4999,5000-&per_page=50&q=data%20scientist&t=0,1"  # Replace with your query URL
    asyncio.run(get_upwork_jobs(query_url, RAW_PATH))

    asyncio.run(staging_jobs(RAW_PATH, STAGING_PATH))

