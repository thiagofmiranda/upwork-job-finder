import pandas as pd
import openai 
import json
from playwright.async_api import async_playwright
import re
from datetime import timedelta
from twilio.rest import Client
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from tqdm import tqdm
import random

def get_posted_datetime(timestamp, posted_text):
    """
    Parses a string like 'posted 2 weeks ago' and returns the datetime
    when the job was posted.
    """
    posted_text = posted_text.lower().strip()

    # Mapping of time units to timedelta arguments
    units = {
        "minute": "minutes",
        "minutes": "minutes",
        "hour": "hours",
        "hours": "hours",
        "day": "days",
        "days": "days",
        "week": "weeks",
        "weeks": "weeks",
        "month": "days",  # Approximate a month as 30 days
        "months": "days",
    }

    if "today" in posted_text:
        return timestamp
    elif "yesterday" in posted_text:
        return timestamp - timedelta(days=1)
    elif "last week" in posted_text:
        return timestamp - timedelta(weeks=1)
    elif "last month" in posted_text:
        return timestamp - timedelta(days=30)
    elif "last year" in posted_text:
        return timestamp - timedelta(days=365)
    
    # Regex to find expressions like "posted 3 weeks ago"
    match = re.search(r"posted\s+(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months)\s+ago", posted_text)
    if match:
        value = int(match.group(1))
        unit = match.group(2)

        if unit in ["month", "months"]:
            return timestamp - timedelta(days=value * 30)
        elif unit in units:
            return timestamp - timedelta(**{units[unit]: value})
    
    raise ValueError(f"Unrecognized format: '{posted_text}'")

async def get_upwork_jobs(query_url, RAW_PATH):
    jobs = []
    datetime_now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Scraping Upwork jobs at {datetime_now}...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Set to True for headless mode
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
            locale='en-US',
            viewport={'width': 1280, 'height': 720},
        )

        page = await context.new_page()


        # Navigate to the Upwork query URL
        await page.goto(query_url)

        # Wait for the job list to load
        await page.wait_for_selector('section.card-list-container')  # Adjust selector if needed
        job_card = await page.query_selector('section.card-list-container')

        # Extract job titles and links
        job_elements = await job_card.query_selector_all("article[class^='job-tile']")
        print(f"Raw Step: Found {len(job_elements)} job elements.")

        for job_element in job_elements:
            job_date_post_element = await job_element.query_selector("small[class^='text-light']")
            title_element = await job_element.query_selector("h2")
            link_element = await title_element.query_selector('a')

            job_description_element = await job_element.query_selector('p')

            job_type_level_element = await job_element.query_selector('li[data-test="job-type-label"]')
            job_experience_level_element = await job_element.query_selector('li[data-test="experience-level"]')
            is_fixed_price_element = await job_element.query_selector('li[data-test="is-fixed-price"]')
            duration_label_element = await job_element.query_selector('li[data-test="duration-label"]')

            if title_element and link_element:
                job_post_date = await job_date_post_element.inner_text()
                title = await title_element.inner_text()
                link = await link_element.get_attribute('href')
                job_description_element = await job_description_element.inner_text()

                job_type_level = await job_type_level_element.inner_text() if job_type_level_element else None
                job_experience_level = await job_experience_level_element.inner_text() if job_experience_level_element else None
                is_fixed_price = await is_fixed_price_element.inner_text() if is_fixed_price_element else None
                duration_label = await duration_label_element.inner_text() if duration_label_element else None

                # extract the job_id from the job_link, it is between the last '~' and '/'
                job_id = re.search(r'~([^/]+)', link).group(1) if link else None

                # print(f"Job Title: {title}, Job Link: {link}, Job Date Post: {job_date_post}")
                # Append to the list
                jobs.append({
                    "job_id": job_id, 
                    "job_title": title, 
                    "job_description": job_description_element,
                    "job_link": link, 
                    "job_post_date": job_post_date, 
                    "job_type_level": job_type_level,
                    "job_experience_level": job_experience_level,
                    "is_fixed_price": is_fixed_price,
                    "duration_label": duration_label,
                    "datetime": datetime_now})
    
        # Close the browser
        await browser.close()

    jobs_df = pd.DataFrame(jobs)
    jobs_df["datetime"] = pd.to_datetime(jobs_df["datetime"], errors='coerce')

    try:
        df_raw = duckdb.query(f"""
            SELECT * FROM read_parquet('{RAW_PATH}/*.parquet')
            ;
        """).to_df()
    except:
        df_raw = pd.DataFrame(columns=["job_id"])
    
    raw_job_ids = df_raw['job_id'].unique() if not df_raw.empty else []
    print(f"Raw Step: Found {len(raw_job_ids)} raw job IDs.")
    # Filter out jobs that have already been downloaded
    jobs_df = jobs_df[~jobs_df['job_id'].isin(raw_job_ids)]
    print(f"Raw Step: Found {len(jobs_df)} new jobs to download.")

    # Append the new jobs to the existing DataFrame
    if not jobs_df.empty:
        df_raw = pd.concat([df_raw, jobs_df], ignore_index=True)
    
        jobs_df_schema = pa.schema([
            ("job_id", pa.string()),
            ("job_title", pa.string()),
            ("job_description", pa.string()),
            ("job_link", pa.string()),
            ("job_post_date", pa.string()),
            ("job_type_level", pa.string()),
            ("job_experience_level", pa.string()),
            ("is_fixed_price", pa.string()),
            ("duration_label", pa.string()),
            ("datetime", pa.timestamp('ns'))
        ])
        
        pq.write_to_dataset(
            pa.Table.from_pandas(df_raw, schema=jobs_df_schema, preserve_index=False), 
            root_path='app/data/raw', 
            basename_template='raw_{i}.parquet'
        )

import asyncio

async def staging_jobs(RAW_PATH, STAGING_PATH):
    def sync_staging_jobs():
        try:
            df_raw = duckdb.query(f"""
                SELECT 
                    *
                FROM read_parquet('{RAW_PATH}/*.parquet')
                ;
            """).to_df()
        except:
            df_raw = pd.DataFrame(columns=["job_id"])

        try:
            df_staging = duckdb.query(f"""
                SELECT 
                    *
                FROM read_parquet('{STAGING_PATH}/*.parquet')
                ;
            """).to_df()
        except:
            df_staging = pd.DataFrame(columns=["job_id"])

        staging_job_ids = df_staging['job_id'].unique() if not df_staging.empty else []
        print(f"Staging Step: Found {len(staging_job_ids)} staging job IDs.")
        jobs_df = df_raw[~df_raw['job_id'].isin(staging_job_ids)]
        print(f"Staging Step: Found {len(jobs_df)} new jobs to evaluate.")

        if not jobs_df.empty:
            jobs_df['post_date'] = jobs_df.apply(
                lambda row: get_posted_datetime(row['datetime'], row['job_post_date']), axis=1
            )
            jobs_df['job_link'] = "http://www.upwork.com" + jobs_df['job_link']

            for i, row in tqdm(jobs_df.iterrows(), dynamic_ncols=False, total=len(jobs_df)):
                result = evaluate_job(row)
                if i % 10 == 0:
                    print(f"Staging Step: Evaluating job {i} of {len(jobs_df)}")
                for key in result.index:
                    jobs_df.loc[i, key] = result[key]

            df_staging = pd.concat([df_staging, jobs_df], ignore_index=True)

            jobs_df_schema = pa.schema([
                ("job_id", pa.string()),
                ("job_title", pa.string()),
                ("job_description", pa.string()),
                ("job_link", pa.string()),
                ("job_post_date", pa.string()),
                ("job_type_level", pa.string()),
                ("job_experience_level", pa.string()),
                ("is_fixed_price", pa.string()),
                ("duration_label", pa.string()),
                ("datetime", pa.timestamp('ns')),
                ("match_level", pa.float64()),
                ("apply", pa.bool_()),
                ("reason", pa.string()),
                ("model", pa.string())
            ])

            pq.write_to_dataset(
                pa.Table.from_pandas(df_staging, schema=jobs_df_schema, preserve_index=False), 
                root_path=STAGING_PATH, 
                basename_template='staging_{i}.parquet'
            )

            print(f"Staging Step: All {len(jobs_df)} new jobs were evaluated.")
        else:
            print("Staging Step: No new jobs need evaluation.")

    await asyncio.to_thread(sync_staging_jobs)



def json_to_table(json_data):
    """Convert JSON data to a pandas DataFrame."""
    try:
        # If the JSON data is a string, parse it
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
        return pd.json_normalize(json_data)
    except json.JSONDecodeError:
        print("Error decoding JSON:", json_data)
        return pd.DataFrame()


def build_prompt_filter(row):
    return [
        {
            "role": "system",
            "content": (
                "You are a freelance job evaluator specialized in data science, machine learning, and automation. "
                "Your job is to evaluate how well an Upwork job post aligns with the background and preferences of Thiago Miranda, "
                "an experienced data scientist. Always respond in clean, valid JSON format with no additional commentary."
            )
        },
        {
            "role": "user",
            "content": (
                "## Freelancer Profile – Thiago Miranda\n"
                "- Location: Dublin, Ireland\n"
                "- Experience: 6+ years in data science using Python, R, data analytics, statistical modeling, machine learning, and web scraping.\n"
                "- Projects: Developed a complete Python tool to collect and analyze football data from scratch using web scraping, pandas, pyarrow, duckdb, and Streamlit to extract, transform, and present historical football data.\n"
                "- Tools: Python, R, SQL, AWS (SageMaker, Lambda, EC2), Tableau, Power BI, Docker, Google Analytics, Git, APIs.\n"
                "- Domains: Business, Education, Marketplaces, Marketing, and Digital Products.\n"
                "- Preferences: Freelance jobs involving data scraping, analytics, dashboards, automations, or predictive modeling.\n"
                "- Languages: Portuguese (native), English (advanced)\n"
                "- Availability: Full-time and part-time. Prefers well-defined or recurring projects.\n\n"
                "## Job Post\n"
                f"- Title: {row['job_title']}\n"
                f"- Description: {row['job_description']}\n"
                f"- Experience Level Required: {row['job_experience_level']}\n"
                f"- Fixed Price: {row['is_fixed_price']}\n"
                f"- Duration: {row['duration_label']}\n\n"
                "## Instructions\n"
                "Evaluate how well the job post fits the freelancer profile and provide a response in valid JSON format with the following keys:\n"
                "- match_level: a float between 0.0 and 1.0 representing the compatibility score\n"
                "- apply: true or false\n"
                "- reason: a short explanation for the decision\n\n"
                "## JSON Format\n"
                "{\n"
                "  \"match_level\": float,\n"
                "  \"apply\": boolean,\n"
                "  \"reason\": string\n"
                "}\n"
            )
        }
    ]


def build_prompt_apply(row):
    impact_hooks = [
        "This project is a perfect match for my skills — my background in machine learning, web scraping, and statistical modeling ensures I can deliver top-quality results quickly and reliably.",
        "I bring exactly the expertise you're looking for — from Python automations to AWS deployments, I’ve delivered high-impact projects that align perfectly with your needs.",
        "With deep experience in data extraction, machine learning, and process automation, I'm confident I can deliver outstanding results for your project.",
        "Your project demands expertise in data science and automation — and that’s precisely what I specialize in, delivering clean, scalable solutions in Python and AWS.",
        "If you're looking for someone who combines strong technical skills in Python, R, SQL, and AWS with business-driven results, I’m your ideal partner for this project.",
    ]
    hook = random.choice(impact_hooks)
    
    perfil_usuario = """
    I am a Data Scientist with over six years of experience driving business growth through machine learning, 
    data analytics, web scraping, and automation. I specialize in developing ML models (XGBoost, Random Forest, Deep Learning), 
    automating ETL processes, and creating scalable data pipelines using Python, R, SQL, and AWS (SageMaker, Lambda, S3). 
    I have hands-on experience with Large Language Models (LLMs), Natural Language Processing (NLP), and Computerized Adaptive Testing (CAT) systems. 
    My projects always focus on delivering actionable insights and operational efficiencies. 
    I am comfortable working in fast-paced environments, adapting to new challenges, 
    and communicating effectively with both technical and non-technical stakeholders.
    """
        
    prompt = f"""
    You are a professional freelancer specialized in writing personalized Upwork cover letters.

    Generate a **customized cover letter** for the following Upwork job opportunity:

    ---
    **Job Title:** {row['job_title']}
    **Job Description:** {row['job_description']}
    **Experience Level Required:** {row['job_experience_level']}
    **Fixed Price:** {row['is_fixed_price']}
    **Duration:** {row['duration_label']}
    ---

    **Profile Information:**
    {perfil_usuario}

    **Instructions:**
    1. Start the letter with these two powerful sentences to immediately catch the client's attention:
    "{hook}"

    2. After the opening, continue with a brief paragraph that:
    - Highlights my relevant experience (based on the job description and required skills),
    - Shows confidence and a business-driven mindset,
    - Avoids generic statements (be specific).

    3. End with a **short call to action** inviting the client to discuss further.

    4. Maintain a **professional, confident, and direct tone**.

    5. Keep the entire cover letter around **150–180 words** to stay concise and impactful.

    **Important:** Focus on making the first two sentences extremely convincing, because they are the ones the client will see first.
    """

    return prompt


def evaluate_job(row):
    model = "gpt-4o-mini"
    prompt = build_prompt_filter(row)
    # calculate the number of tokens in the prompt
    
    client = openai.OpenAI()
    
    try:
        
        # Use the OpenAI API to get a response
        response = client.responses.create(
            model=model,  # ou gpt-3.5-turbo se quiser economizar
            input=prompt
        )

        content = response.output_text
        # extract everything between the first and last curly braces 
        content = content[content.find("{"):content.rfind("}") + 1]
        
        # Tentar fazer parsing do JSON de forma segura
        result = json.loads(content)
        
        
        
        # Validação mínima dos campos esperados
        match_level = float(result.get("match_level", 0))
        apply = bool(result.get("apply", False))
        reason = result.get("reason", "")

        return pd.Series({
            "match_level": match_level,
            "apply": apply,
            "reason": reason,
            "model": model
        })

    except json.JSONDecodeError:
        return pd.Series({
            "match_level": None,
            "apply": None,
            "reason": "Invalid JSON response",
            "model": model
        })
    except Exception as e:
        print(f"Error: {str(e)}")
        return pd.Series({
            "match_level": None,
            "apply": None,
            "reason": f"Error: {str(e)}",
            "model": model
        })
    

def apply_job(row):
    model = "gpt-4.1"
    prompt = build_prompt_apply(row)
    
    client = openai.OpenAI()
    
    try:
        
        # Use the OpenAI API to get a response
        response = client.responses.create(
            model=model,  # ou gpt-3.5-turbo se quiser economizar
            input=prompt
        )

        content = response.output_text
        return content

    except Exception as e:
        print(f"Error: {str(e)}")
        return ""