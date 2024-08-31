import time
import math
import yaml
import boto3
import requests
import pandas as pd
import pyarrow as pa
from io import BytesIO
import pyarrow.parquet as pq
from io import StringIO
from bs4 import BeautifulSoup
from selenium import webdriver
from airflow.models import Variable
from requests.adapters import Retry
from requests.exceptions import Timeout
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException


# chrome_options = Options()
# chrome_options.headless = True

# driver = webdriver.Chrome(options = chrome_options)


with open('/home/manisha/airflow/dags/config.yml', encoding='utf-8') as file:
    yml = yaml.safe_load(file)

bucket_name = yml['bucket_name']
file_name = yml['file_name']
access_key_id = yml['access_key_id']
secret_access_key = yml['secret_access_key']

def create_hive_partition():
    current_date = datetime.now()
    year = current_date.strftime("%Y")          
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    hive_partition = f"year={year}/month={month}/day={day}/"

    return hive_partition

def scrape_page(url):
    driver.get(url)
    selenium_response = driver.page_source
    response = BeautifulSoup(selenium_response,"html.parser")
    return response

def scrape_page(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            driver.get(url)
            selenium_response = driver.page_source
            soup = BeautifulSoup(selenium_response, "html.parser")
            return soup
        except Exception as e:
            attempt = attempt + 1
            print("Exception occurred on attempt", attempt + 1)
            print("Exception: ",e)
            if attempt + 1 < max_retries:
                print("Retrying...")
                time.sleep(2)  # Add a delay before retrying
                continue
            else:
                print("Max retries reached")
                return None

def scrape_page(url):
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        # response = session.get(url, timeout=5)
        driver.get(url)
        selenium_response = driver.page_source
    except Timeout:
        print("Timeout occurred")
        return None

    soup = BeautifulSoup(selenium_response, "html.parser")
    return soup


def get_cursor(soup, next_page_num):
    cursor = soup.find('a', {'class':'chakra-link css-1wxsdwr', 'aria-label': f'page {next_page_num}'})
    if cursor:
        return cursor.get('href')
    else:
        return None

def get_job_links(url, list_elems):
    job_links = []
    for item in list_elems:
        job_key = item.div.get('data-jobkey')
        job_link = f'{url}&job={job_key}'
        job_links.append(job_link)
    return job_links


# def scrape_one_page(url, soup,search_keyword_for_job):
#     ul = soup.find_all('ul', id = 'job-list')
#     li = ul[0].find_all('li', {'class':'css-0'})
#     job_links = get_job_links(url, li)
#     data_list = []
#     for job_link in job_links:
#         print(job_link)
#         job = scrape_page(job_link)
#         job_title = job.h2.text.strip() if job.h2.text.strip() else ""
#         company = job.find('span', {'data-testid': 'viewJobCompanyName'}).text if job.find('span', {'data-testid': 'viewJobCompanyName'}) else ""
#         location = job.find('span', {'data-testid': 'viewJobCompanyLocation'}).text if job.find('span', {'data-testid': 'viewJobCompanyLocation'}) else ""
#         job_type = job.find('span', {'data-testid': 'viewJobBodyJobDetailsJobType'}).text if job.find('span', {'data-testid': 'viewJobBodyJobDetailsJobType'}) else ""
#         salary = job.find('span', {'data-testid': 'viewJobBodyJobCompensation'}).text if job.find('span', {'data-testid': 'viewJobBodyJobCompensation'}) else ""
#         posted_on = job.find('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'}).text if job.find('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'}) else ""
#         # job_qualification = job.find('div', {'data-testid': 'viewJobQualificationsContainer'})\
#         #                         .find('ul')
#         try:
#             job_qualification = job.find('div', {'data-testid': 'viewJobQualificationsContainer'}).find('ul')
#             if job_qualification:
#                 job_qualification = list(job_qualification.strings)
#             else:
#                 job_qualification = []
#         except AttributeError:
#             job_qualification = []

#         if job_qualification:
#             job_qualification = list(job.find('div', {'data-testid': 'viewJobQualificationsContainer'})\
#                                     .find('ul').strings)
#             job_qualification = [x.strip() for x in job_qualification]
#             # job_qualification = "\n".join(job_qualification)
#         else:
#             job_qualification = ""

#         job_description = job.find('div', {'data-testid':'viewJobBodyJobFullDescriptionContent'})
#         if job_description:
#             job_description = list(job.find('div', {'data-testid':'viewJobBodyJobFullDescriptionContent'}).strings)
#             job_description = [x.strip() for x in job_description]
#             # job_description = "\n".join(job_description)
#         else:
#             job_description = ""

#         job_posted_time = soup.find_all('span',{'data-testid':'viewJobBodyJobPostingTimestamp'})
#         job_posted_time = job_posted_time[0].find('span',{'data-testid':'detailText'}).text.strip()
#         # date_format = "%Y-%m-%d"
#         date_format = "%Y-%m-%d"
#         if "days" in  job_posted_time :
#             job_posted_date = datetime.now() - timedelta(days=int(job_posted_time.split()[0]))
#             job_posted_dates = job_posted_date.strftime(date_format)
#         elif "hours" in job_posted_time:
#             job_posted_date = datetime.now() - timedelta(hours=int(job_posted_time.split()[0]))
#             job_posted_dates = job_posted_date.strftime(date_format)
#         else:
#             job_posted_dates = datetime.now()

#         data = {
#                 'search_keyword_for_job':search_keyword_for_job,
#                 'job_title':job_title,
#                 'job_url':job_link,
#                 'company':company,
#                 'location': location, 
#                 'job_type': job_type, 
#                 'salary':salary, 
#                 'posted_on':job_posted_dates, 
#                 'job_qualification':job_qualification, 
#                 'job_description': job_description
#                 }
#         data_list.append(data)
#         # break
#     return data_list

def scrape_one_page(url, soup, search_keyword_for_job):
    ul = soup.find_all('ul', id='job-list')
    li = ul[0].find_all('li', {'class': 'css-0'})
    job_links = get_job_links(url, li)
    data_list = []
    for job_link in job_links:
        print(job_link)
        job = scrape_page(job_link)
        job_title = job.h2.text.strip() if job.h2.text.strip() else ""
        company = job.find('span', {'data-testid': 'viewJobCompanyName'}).text if job.find('span', {'data-testid': 'viewJobCompanyName'}) else ""
        location = job.find('span', {'data-testid': 'viewJobCompanyLocation'}).text if job.find('span', {'data-testid': 'viewJobCompanyLocation'}) else ""
        job_type = job.find('span', {'data-testid': 'viewJobBodyJobDetailsJobType'}).text if job.find('span', {'data-testid': 'viewJobBodyJobDetailsJobType'}) else ""
        salary = job.find('span', {'data-testid': 'viewJobBodyJobCompensation'}).text if job.find('span', {'data-testid': 'viewJobBodyJobCompensation'}) else ""
        posted_on = job.find('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'}).text if job.find('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'}) else ""
        
        try:
            job_qualification = job.find('div', {'data-testid': 'viewJobQualificationsContainer'}).find('ul')
            if job_qualification:
                job_qualification = list(job_qualification.strings)
            else:
                job_qualification = []
        except AttributeError:
            job_qualification = []

        job_description = job.find('div', {'data-testid': 'viewJobBodyJobFullDescriptionContent'})
        if job_description:
            job_description = list(job_description.strings)
        else:
            job_description = []

        job_posted_time = soup.find_all('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'})
        job_posted_time = job_posted_time[0].find('span', {'data-testid': 'detailText'}).text.strip()
        date_format = "%Y-%m-%d"
        if "days" in job_posted_time:
            job_posted_date = datetime.now() - timedelta(days=int(job_posted_time.split()[0]))
            job_posted_dates = job_posted_date.strftime(date_format)
        elif "hours" in job_posted_time:
            job_posted_date = datetime.now() - timedelta(hours=int(job_posted_time.split()[0]))
            job_posted_dates = job_posted_date.strftime(date_format)
        else:
            job_posted_dates = datetime.now()

        data = {
            'search_keyword_for_job': search_keyword_for_job,
            'job_title': job_title,
            'job_url': job_link,
            'company': company,
            'location': location,
            'job_type': job_type,
            'salary': salary,
            'posted_on': job_posted_dates,
            'job_qualification': job_qualification,
            'job_description': job_description
        }
        data_list.append(data)
    return data_list


def start_scraping():
    global chrome_options
    global driver
    chrome_options = Options()
    chrome_options.headless = True

    driver = webdriver.Chrome(options = chrome_options)
    # job_collection = ['Data Engineer','Frontend Developer', 'Backend Developer','Fullstack Developer', 'Business Analyst', 'DevOps Engineer', 'UI/UX', 'IT Manager']
    job_collection = ['Data Engineer']
    # Create a list to store the DataFrames
    df_list = []

    # Navigate to URL
    for item in job_collection:
        url = f'https://www.simplyhired.com/search?q={item}&l=united+states&t=1'
        next_page = url
        driver.get(url)
        # time.sleep(1)
        number_of_jobs = driver.find_element(By.XPATH, '//*[@id="__next"]/div/main/div/div[1]/div[4]/p')
        number_of_pages = math.ceil(int(number_of_jobs.text)/int(20))
        print(f'Number of jobs for {item}:{number_of_jobs.text}   total_page:{number_of_pages}')
        

        i=1
        df = pd.DataFrame(columns=['search_keyword_for_job','job_title','job_url', 'company', 'location', 'job_type', 'salary', 'posted_on', 'job_qualification', 'job_description'])
        df['job_qualification'] = df['job_qualification'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        while next_page != None:
            print('Page Number:', i, ' Page Link: ', next_page)
            soup = scrape_page(next_page)
            page_data = scrape_one_page(next_page, soup,item)
            print(type(page_data))
            df = df.append(page_data)   
            # df = pd.concat([df, page_data], ignore_index=True)
            df['company'] = df['company'].str.replace(',', '-')
            df['location'] = df['location'].str.replace(',', '-')
            df['salary'] = df['salary'].str.replace(',', '') 
            # i = i+1
            next_page = get_cursor(soup, i+1)
            time.sleep(3)   
            i = i+1
            

        # Append the DataFrame to the list
        df_list.append(df)

    df = pd.concat(df_list)

    parquet_buffer = BytesIO()
    df.to_parquet('/home/manisha/airflow/data/simply_hired.parquet', index=False)
    df_v2 = pd.read_parquet('/home/manisha/airflow/data/simply_hired.parquet')
    df_v2.to_parquet(parquet_buffer,engine='pyarrow',index=False)
   

    s3_client = boto3.client('s3',
                              aws_access_key_id = access_key_id,
                            aws_secret_access_key = secret_access_key)   
    hive_path = create_hive_partition()

    print(hive_path)

    s3_client.put_object(Bucket= bucket_name, Key= f"{file_name}{hive_path}simply_hired_data.parquet",  Body = parquet_buffer.getvalue())

    driver.close()

if __name__ == '__main__':
    # start_scraping()
     final_df = start_scraping()