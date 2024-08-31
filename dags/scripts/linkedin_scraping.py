import io
import os
import time
import math
import yaml
import boto3
import requests
import urllib.parse
import pandas as pd
import pyarrow as pa
from io import BytesIO
from io import StringIO
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
from selenium import webdriver
from airflow.models import Variable
from requests.adapters import Retry
from requests.exceptions import Timeout
from datetime import datetime, timedelta
from scrapingbee import ScrapingBeeClient
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

with open('/home/manisha/airflow/dags/config.yml', encoding='utf-8') as file:
    yml = yaml.safe_load(file)

bucket_name = yml['bucket_name']
file_name = yml['file_name']
access_key_id = yml['access_key_id']
secret_access_key = yml['secret_access_key']
s3_client = boto3.client('s3', aws_access_key_id=access_key_id,
                         aws_secret_access_key= secret_access_key)

title_mapping = {
    "Data Engineer": ["Data Engineer Aws", "Data Engineer Azure", "Data Engineer Snowflake", "Data Engineer Databricks", "Data Engineer GCP",
                      "DataOPS AWS", "DataOPS Azure", "DataOPS Snowflake", "DataOPS Databricks", "DataOPS GCP", 
                      "Data Annotator", "Data Governance", "Data Quality"],
    "Data Analyst": ["Data Analyst PowerBI", "Data Analyst Tableau", "Data Analyst Looker", "Data Analytics Solution Architech"],
    "Machine Learning Engineer": ["Machine Learning Engineer", "Computer Vision Enginer", "Natural Language Engineer", "Data Scientist"],
    "AI": ["AI Engineer", "AI Researcher", "MLOps", "Generative AI"],
    "FrontEnd": ["React.js", "Angular.js", "Vue.js", "Next.js", "Nuxt.js", "Svelte", "Solid.js", "Astro", "Qwik"],
    "BackEnd": ["PHP", "Laravel", "Go", "Python Backend", "Django", "Flask", "Java", "Spring", "Ruby" "Node.js", "Nest.js", "Elixir"],
    "Mobile": ["React Native", "Flutter", "Xamarin", "Ionic", "Android", "Kotlin", "Objective-C", "Swift"],
    "UI/UX": ["Balsamiq", "Figma", "Adobe XD", "Adobe Illustrator", "Miro", "UI/UX"],
    "FrontEnd Development": ["HTML/CSS", "Bootstrap", "Tailwind", "Foundation", "Bulma"],
    "QC": ["Test Planning and Management", "Test Automation", "API Testing", "Performance Testing", "Cypress", "Selenium", "TestRail", "SonarQube"],
    "Project Management": ["Project Management"],
    "DevOps": ["DevOps AWS", "DevOps Azure", "DevOps Google Cloud", "DevOps Docker", "DevOps Kubernetes", "DevOps Terraform", "DevOps CI/CD"]
}

department_mapping = {
    "Data Engineering": ["Data Engineer", "Data Analyst"],
    "Service Engineering": ["FrontEnd", "BackEnd", "Full Stack", "Mobile"],
    "DevOps": ["DevOps"],
    "AI Services": ["Machine Learning Engineer", "AI"],
    "Design": ["UI/UX", "FrontEnd Development"],
    "PMO": ["Project Management"],
    "QC": ["QC"]
}

def find_main_title(keyword):
    for title, title_keywords in title_mapping.items():
        if keyword.lower() in [kw.lower() for kw in title_keywords]:
            return title
    return keyword


def find_department(main_title):
    for title, title_keywords in department_mapping.items():
        if main_title.lower() in [kw.lower() for kw in title_keywords]:
            return title
    return None


def create_hive_partition():

    current_date = datetime.now()
    year = current_date.strftime("%Y")          
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    hive_partition = f"year={year}/month={month}/day={day}/"

    return hive_partition


def get_request(conn, url, params, max_retries=3):
    # Construct the query parameters string from the params dictionary
    query_params = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_params}"

    retries = 0
    while retries < max_retries:
        try:
            res = conn.get(full_url,
                   params={
                       'render_js': 'False',
                   }
                   )
            res.raise_for_status()
            data = res.content
            soup = BeautifulSoup(data, "html.parser")
            return soup
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
            time.sleep(5)
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            time.sleep(5)
        except requests.exceptions.RequestException as err:
            print ("OOps: Something Else",err)

        retries += 1
        time.sleep(5)
    return None
        

def current_time_string():
    datetime_obj = datetime.now()
    date_time_today = datetime_obj.strftime("%Y-%m-%d_%H:%M:%S")
    date_today = datetime_obj.strftime("%Y-%m-%d")
    date_today = date_time_today = '2024-01-03'
    return date_today, date_time_today

def write_dataframe_to_s3_pq(bucket_name: str, s3_key: str, df) -> None:
    try:
        df = df.astype(str)
        table = pa.Table.from_pandas(df)
        # Create an in-memory buffer to store the Parquet data
        buffer = io.BytesIO()
        # Write the Parquet Table to the buffer
        pq.write_table(table, buffer)
        # Upload the buffer to S3
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key= f"{s3_key}.parquet",
            Body=buffer.getvalue()
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
    except Exception as e:
        print(f'Exception: {e}')

def write_dataframe_to_s3_csv(bucket_name: str, s3_key: str, df)->None:
    try:
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(
                Bucket = bucket_name,
                Key = f"{s3_key}.csv",
                Body = csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")    
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}") 
    except Exception as e:
        print(f'Exception: {e}')

def num_of_jobs(soup):
    try:
        label = soup.find("span", {"class": 'results-context-header__new-jobs'})
        label_text = label.get_text(strip=True)
        num_of_jobs = int(''.join(filter(str.isdigit, label_text.split()[0])))
        return num_of_jobs
    except Exception as e:
        print(e)
        return 0


def get_job_urls(list_elems, data_list, job):
    main_title = find_main_title(job)
    department = find_department(main_title)
    data_list.extend([(todays_date, department, main_title, job, item.find("a")["href"])
                     for item in list_elems])


def scrape_page(job, page_num, data_list, session, params):
    params["start"] = (page_num - 1) * 25
    target_url = f"{base_url}jobs-guest/jobs/api/seeMoreJobPostings/search"
    soup = get_request(session, target_url, params)
    if soup == None:
        pass
    jobs_in_one_page = soup.find_all("li") if soup else []
    get_job_urls(jobs_in_one_page, data_list, job)


def scrape_single_job(job, data_list):
    experience_filter = {
        "1": "Internship",
        "2": "Entry level",
        "3": "Associate",
        "4": "Mid-Senior level",
        "5": "Director"
    }
    work_type_filter = {
        "1": "On-site",
        "2": "Remote",
        "3": "Hybrid"
    }

    params = {
        "keywords": job,
        "location": "United States",
        "f_TPR": "r86400",
    }

    soup = get_request(session, f"{base_url}jobs/search", params)
    if soup == None:
        pass
    total_jobs = int(num_of_jobs(soup))
    # print(total_jobs)
    if total_jobs > 1000:
        for exp in experience_filter.keys():
            params["f_E"] = exp
            params["f_WT"] = ""
            soup = get_request(session, f"{base_url}jobs/search", params)
            if soup == None:
                continue
            total_jobs = int(num_of_jobs(soup))
            if total_jobs > 1000:
                for wt in work_type_filter.keys():
                    params["f_WT"] = wt
                    soup = get_request(
                        session, f"{base_url}jobs/search", params)
                    if soup == None:
                        continue
                    total_jobs = int(num_of_jobs(soup))
                    total_jobs = 1000 if total_jobs > 1000 else total_jobs
                    print("Here", total_jobs)
                    num_pages = math.ceil(total_jobs / 25)
                    for page_num in range(1, num_pages + 1):
                        scrape_page(job, page_num, data_list, session, params)
            else:
                num_pages = math.ceil(total_jobs / 25)
                for page_num in range(1, num_pages + 1):
                    scrape_page(job, page_num, data_list, session, params)
    else:
        num_pages = math.ceil(total_jobs / 25)
        for page_num in range(1, num_pages + 1):
            scrape_page(job, page_num, data_list, session, params)

def scrape_multiple_jobs(job_list, url_path):
    ''' Scrape for multiple jobs'''
    data_list = []
    for job in job_list:
        scrape_single_job(job, data_list)
        df = pd.DataFrame(data_list, columns=["extract_date", "department", "main_title", "job_title", "url"])
        data_list = []
        if not os.path.exists(url_path):
            df.to_csv(url_path, mode='a', index=False)
        else:
            df.to_csv(url_path, mode='a', header=False, index=False)
    # return data_list


def linkedin(BASE_URL, JOB_TITLES, url_path):
    global base_url, session, todays_date
    base_url = BASE_URL
    # Personal API
    # session = ScrapingBeeClient(
    #     api_key='RFFUDHBRZ851GAG1J9L6X4A6DJCQFFRH1LEICG5ZRIUPDDFW82K78UBRGHWP7D4BGOOL0WL9J6DGLZP1')
    # Office API
    session = ScrapingBeeClient(
        api_key='PTVT5ISCQMR9XG81QUPQ7M1ZH49P3ST05Q29TUN32J6EUKUU860UFZZUMPQ1BONMS0KJ8PBISY64R6J2')
    todays_date  = datetime.date.today().strftime('%Y-%m-%d')
    # logging.basicConfig(filename=f"logs/log_{todays_date}.txt", level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
    scrape_multiple_jobs(JOB_TITLES, url_path)
    # df = pd.DataFrame(df, columns=["extract_date", "department", "main_title", "job_title", "url"])
    # df = df.drop_duplicates(subset= "url")
    # return df

def start_linkedin_scraping():
    print("*****Starting Scraping Linkedin*******")
    df = pd.read_parquet("/home/manisha/airflow/data/linkedin_data_2024-02-29.parquet")

    itertation = 0
    for index, row in df.iloc[0:100].iterrows():
        title = row['title']
        job_url = row['job_url']
        itertation = itertation+1
        print(f"{itertation}:Scraping complete for {title}: {job_url}")
        time.sleep(1)

if __name__ == '__main__':
        start_linkedin_scraping()