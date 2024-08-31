import yaml
import time
import boto3
import botocore
import pandas as pd
from typing import List
from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql.functions import to_date
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import col, explode, regexp_replace, split,monotonically_increasing_id,when,size,trim,concat_ws,array_distinct


# glue_client = boto3.client('glue')
s3_client = boto3.client('s3')
val_dict = {}


# spark.setConf("spark.sql.shuffle.partitions", "100")

## functions
def create_df_from_catalog(database: str, table: str, year: str, month: str, day: str) :
    """
    Creates a Spark DataFrame from a Glue catalog using the specified parameters.

    Parameters:
    database (str): The name of the Glue catalog database.
    table (str): The name of the table in the Glue catalog.
    year (str): The year value as a string.
    month (str): The month value as a string.
    day (str): The day value as a string.

    Returns:
    DataFrame: A Spark DataFrame created from the Glue catalog.
    """
    df = pd.read
    return df


def get_partitions(database: str, table: str) -> List[List[str]]:
    """
    Retrieves and returns a sorted list of partitions for the specified database and table.

    Parameters:
    database (str): The name of the Glue catalog database.
    table (str): The name of the table in the Glue catalog.

    Returns:
    List[List[str]]: A sorted list of partitions, where each partition is represented as a list of strings.
    """
    partitions = {'tableName':'raw_linkedin'}
    
    partitions_list = partitions['Partitions']
    partitions_sorted = sorted(
        list(map(lambda x: x['Values'], partitions_list))
    )
    return partitions_sorted

def remaining_dates_to_sync(src_database: str, dest_database: str, src_table: str, dest_table: str) -> List[List[str]]:
    """
    Determines the remaining dates to synchronize between the source and destination tables in the Glue catalog.

    Parameters:
    src_database (str): The name of the source Glue catalog database.
    dest_database (str): The name of the destination Glue catalog database.
    src_table (str): The name of the source table in the Glue catalog.
    dest_table (str): The name of the destination table in the Glue catalog.

    Returns:
    List[List[str]]: A list of remaining dates to synchronize, where each date is represented as a list of strings.
    If there are no remaining dates, a list containing the latest destination partition is returned.
    If the destination database or table does not exist, the function returns all source partitions.
    """
    src_partitions = get_partitions(
        database=src_database,
        table=src_table
    )
    try:
        # Try to get destination partitions
        dest_partitions = get_partitions(
            database=dest_database,
            table=dest_table
        )
    except:
        # If destination database and table do not exist, return all source partitions
        return src_partitions


    remaining_dates = [
        date for date in src_partitions if date not in dest_partitions]

    if not remaining_dates:
        # If no remaining dates, return the latest destination partition
        return [dest_partitions[-1]]
    return remaining_dates



def get_config_file(bucket: str, key: str) -> dict:
    """
    Retrieves and returns the contents of a YAML config file stored in an S3 bucket.

    Parameters:
    bucket (str): The name of the S3 bucket.
    key (str): The key or path of the config file in the S3 bucket.

    Returns:
    dict: The contents of the YAML config file, represented as a dictionary.

    Raises:
    botocore.exceptions.ClientError: If there is an error reading the config file from S3.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read().decode('utf-8')
        yaml_data = yaml.safe_load(contents)
        return yaml_data
    except botocore.exceptions.ClientError as e:
        print(
            f"Could not read config file s3://{bucket}/{key}. Exception: {e}.")
        raise


def write_df_to_s3(df, dest_bucket: str, dest_prefix: str, dest_database: str, dest_table: str):
    """
    Writes a DataFrame to S3 and updates the data catalog with partition information.

    Parameters:
    df (DataFrame): The input DataFrame to write.
    dest_bucket (str): The destination S3 bucket.
    dest_prefix (str): The prefix within the S3 bucket where the DataFrame will be written.
    dest_database (str): The destination Glue catalog database.
    dest_table (str): The destination Glue catalog table.

    Returns:
    None
    """
    # Get the latest source partition
    lastest_src_partition = df.select('year', 'month', 'day').first().asDict()
    year = lastest_src_partition["year"]
    month = lastest_src_partition["month"]
    day = lastest_src_partition["day"]

    # Destination table path to write to
    dest_write_path = f's3://{dest_bucket}/{dest_prefix}{dest_table}'


# get tables list sorted
def get_tables_list(database: str) -> List[str]:
    """
    Retrieves a sorted list of tables from the specified Glue catalog database.

    Parameters:
    database (str): The name of the Glue catalog database.

    Returns:
    List[str]: A sorted list of table names.
    """
    tables =  ['simplyhired', 'linkedin']
    tables_sorted = sorted(tables, reverse=False)
    return tables_sorted


def check_database_exists(database: str) -> bool:
    """
    Checks if a database exists in the Glue catalog.

    Parameters:
    database (str): The name of the database to check.

    Returns:
    bool: True if the database exists, False otherwise.
    """
    try:
        # glue_client.get_database(Name=database)
        return True
    except:
        return False


PR_pattern = r'(?i)(Product|Resources|Energy|Utilities|Logistics|Retail|Consumer|Goods|Travel|Hospitality|Oil|Gas|Transportation|Machinery|Semiconductor|Plastics)'
HC_INCS_LS_pattern = r'(?i)(Insurance|Life Science|Healthcare|Hospitals|Pharmaceutical|Biotechnology|Medical)'
CMT_pattern = r'(?i)(Communication|Entertainment|Media|Telecommunications|Information|Internet|Software|IT Services|Technology|Computer|Network|Wireless|Hospitals|Health Care)'
BFS_EDU_GOV_pattern = r'(?i)(Education|Government|Banking|Financial|Accounting|Book|Publishing|E-Learning|Business|Investment|Capital|Finance)'

def add_verticals(df):
    df = df.withColumn(
        "verticals",
        f.expr(
            f"CASE " +
            f"WHEN company_industry REGEXP '{PR_pattern}' THEN 'PR' " +
            f"WHEN company_industry REGEXP '{HC_INCS_LS_pattern}' THEN 'HC_INCS_LS' " +
            f"WHEN company_industry REGEXP '{CMT_pattern}' THEN 'CMT' " +
            f"WHEN company_industry REGEXP '{BFS_EDU_GOV_pattern}' THEN 'BFS_EDU_GOV' " +
            "ELSE 'None' END"
        )
    )
    return df
    
def filter_managers(df):
    manager_keywords_to_ignore = ['vp', 'director', 'graphic', 'multimedia', 'vehicle', 'digital', 'business','manager']
    filter_condition = ~col('title').rlike('|'.join(map(lambda x: f"(?i){x}", manager_keywords_to_ignore)))
    df = df.filter(filter_condition)
    return df

    

def filter_company_industries():
    keywords_to_ignore = ["space", "defense", "aviation", "aerospace", "government", "human resources", "non-profit", "robotics", 
    "public relation", "political", "chemical", "chemistry", "outsourcing", "offshoring"]
    
    # filtered_df = df.filter(~col('company_industry').rlike('|'.join(keywords_to_ignore)))
    # return filtered_df
    for i in keywords_to_ignore:
        print(f"Ingnoring indusrty: {i}")
        time.sleep(3)
    
    
def join_companies(df,src_database,companies_table):
    jyear,jmonth,jday = max(get_partitions(src_database,companies_table))
    companies = create_df_from_catalog(
        database = src_database,
        table=companies_table,
        year=jyear,
        month=jmonth,
        day=jday
        )
    global val_dict
    columns_to_drop = ['year','month','day', 'company_url']
    companies = companies.drop(*columns_to_drop)
    val_dict['total_number_of_companies'] = companies.count()
    companies = filter_company_industries(companies)
    val_dict['number_of_company_after_company_industry_filtered'] = companies.count()

    

    joined_df = df.join(companies,df.company==companies.company_name,'left')
    joined_df = joined_df.withColumnRenamed('company size','company_size')
    # print(joined_df.columns)
    
    # print(partition)
    return joined_df


def clean_tables():
    """
    Cleans the mr table DataFrame by implementing drop duplicates,split location.

    Parameters:
    df (DataFrame): The input hires table DataFrame.

    Returns:
    DataFrame: The cleaned hires table DataFrame.
    """
    
    cols_to_trim = ['extract_date','department','domains','search_title','title','company','salary','location','job_type','posted_on','job_url','description']
    for column in cols_to_trim:
        df = df.withColumn(column, trim(col(column)))
    clean_df = df.dropDuplicates(['company','department','domains','title','extract_date','location','job_type','description','ORG','ROLE','DOMAIN'])

    if "HARD SKILLS" in df.columns:
        clean_df = clean_df.withColumnRenamed("HARD SKILLS","hard_skills").withColumnRenamed("SOFT SKILLS","soft_skills")
        print("Column name is HARD SKILLS")
    else:
        clean_df = clean_df.withColumnRenamed("HARD_SKILLS#0","hard_skills").withColumnRenamed("SOFT_SKILLS#1","soft_skills")
        print("Column name is HARD_SKILLS#0")  
        
    clean_df = clean_df.withColumn("location",split(col("location"), ", ")).withColumn("state",when((col("location").isNotNull()) & (size(col("location")) == 2), col("location")[1]).otherwise("")).withColumn("place",when((col("location").isNotNull()) & (size(col("location")) == 2), col("location")[0]).otherwise(concat_ws(",", "location")))
    clean_df = clean_df.withColumn('location', concat_ws('-', 'location'))
    # clean_df = clean_df.filter((col("extract_date") != "extract_date") | (col("extract_date") != 'nan'))
    #clean_df = clean_df.dropna(subset=["extract_date"])
    clean_df = clean_df.withColumn('extract_date', to_date('extract_date'))
    clean_df = clean_df.filter(col("extract_date").isNotNull())
    clean_df = clean_df.withColumn("title", f.lower(f.col("title")))


# Removing unnecesary terms from title column --------------------------------------------------------
    clean_df = clean_df.withColumn("title", f.regexp_replace("title","\((\$[\d, ]+.*?)\)|\$\d+(,\d+)*", ""))
    clean_df = clean_df.withColumn("title", f.regexp_replace("title","\|*remote\|*",""))
    clean_df = clean_df.withColumn("title",f.regexp_replace("title","\|*hybrid\|*",""))
    clean_df = clean_df.withColumn("title",f.regexp_replace("title","\|*\(\)\|*",""))
    
    datatype = clean_df.schema["hard_skills"].dataType
    if isinstance(datatype, ArrayType):
      array_columns = ["DATE", "DEG", "DOMAIN", "EMAIL", "EXP", "LOC", "ORG", "PER", "PHONE", "PROJECT", "ROLE", "soft_skills", "UNI"]
      for column in array_columns:
        clean_df = clean_df.withColumn(column, col(column).cast("string"))
      print("hard_skills is Array type")
      clean_df = clean_df.withColumn("hard_skills",array_distinct(col("hard_skills")))
    
    elif isinstance(datatype, StringType):
      print("hard_skills is StringType")
      df_single_record = clean_df.sort("hard_skills").limit(1)
    #   df_single_record.select('company','title','hard_skills').show()
      value_to_check = "' '"
      condition = col("hard_skills").contains(value_to_check)
      contains_value = df_single_record.filter(condition).count()
      print('count 0 or 1 ?', contains_value)

      if contains_value == 0:  
        print('Separated by comma')
        clean_df = clean_df.withColumn("hard_skills",array_distinct(split(regexp_replace(col("hard_skills"), "(^\[')|(\']$)", ""),"', '")))

      elif contains_value == 1:
        print('Separated by space')
        clean_df = clean_df.withColumn("hard_skills",regexp_replace(col("hard_skills"), "(^\[')|(\']$)", ""))\
                    .withColumn("hard_skills",regexp_replace(col("hard_skills"), "\n", ""))\
                    .withColumn('hard_skills', split(col('hard_skills'), "' '"))
    else:
        print("datatype mismatched")
        
    print("Before creating temp view")
    clean_df.createOrReplaceGlobalTempView('cleaned_temp_df')
    print("creating temp view")
    """clean_df_sql = spark.sql(
    ''' 
         SELECT 
         extract_date,
         department,
         domains,
         search_title,
          CASE
             WHEN title LIKE '%back%end%' or title LIKE '%backend%' THEN 'Backend Developer'
             WHEN title LIKE '%ui%ux%' THEN 'UI/UX Developer'
             WHEN title LIKE '%business%analyst%' THEN 'Business Analyst'
             WHEN title LIKE '%business%intelligence%' THEN 'Business Intelligence'
             WHEN title LIKE '%data%analyst%' THEN 'Data Analyst'
             WHEN title LIKE '%web%designer' THEN 'Web Designer'
             WHEN title LIKE '%network%engineer' THEN 'Network Engineer'
             WHEN title LIKE '%.net%' THEN '.Net Developer'
             WHEN title LIKE '%machine learning%' THEN 'Machine Learning Engineer'
             WHEN title LIKE '%azure%data engineer%' THEN 'Azure Data Engineer'
             WHEN title LIKE '%aws%data engineer%' THEN 'AWS Data Engineer'
             WHEN title LIKE '%data engineer%' THEN 'Data Engineer'
             WHEN title LIKE '%devops%' THEN 'DevOps Engineer'
             WHEN title LIKE '%application support%' THEN 'Application Support'
             WHEN title LIKE '%quality%assurance%' or title LIKE '%quality engineer%' or title LIKE '%qa%' THEN 'Quality Assurance'
             WHEN title LIKE '%react%js%' THEN 'React JS Developer'
             WHEN title LIKE '%front%end%' THEN 'Frontend Developer'
             WHEN title LIKE '%full%stack%' THEN 'Full Stack Developer'
             WHEN title LIKE '%software%engineer%' THEN 'Software Engineer'
             WHEN title LIKE '%product%manager%' THEN 'Product Manager'
             WHEN title LIKE '%system%engineer%' THEN 'System Engineer'
             WHEN title LIKE '%azure%cloud%engineer' THEN 'Azure Cloud Engineer'
             WHEN title LIKE '%azure%cloud%architect' THEN 'Azure Cloud Architect'
             WHEN title LIKE '%data%scientist%' THEN 'Data Scientist'
             WHEN title LIKE '%it%analyst%' THEN 'IT Analyst'
             WHEN title LIKE '%business%development%' THEN 'Business Development'
             WHEN title LIKE '%blockchain%' THEN 'Blockchain Developer'
             WHEN title LIKE '%java%developer%' THEN 'Java Developer'
             WHEN title LIKE '%data warehouse%' THEN 'Data Warehousing'
             WHEN title LIKE '%support engineer%' THEN 'Support Engineer'
             WHEN title LIKE '%digital marketing%' THEN 'Digital Marketing'
             WHEN title LIKE '%marketing%' THEN 'Marketing'
             WHEN title LIKE '%etl%' THEN 'ETL Developer'
             WHEN title LIKE '%scrum master%' THEN 'Scrum Master'
             WHEN title LIKE '%system administrator%' THEN 'System Administrator'
             WHEN title LIKE '%ios developer%' THEN 'iOS Developer'
             WHEN title LIKE '%graphics designer%' THEN 'Graphics Designer'
             WHEN title LIKE '%professor%' THEN 'Professor'
             WHEN title LIKE '%data entry%' THEN 'Data Entry'
             WHEN title LIKE '%cyber security%' THEN 'Cyber Security'
             WHEN title LIKE '%python developer%' THEN 'Python Developer'
             WHEN title LIKE '%principal scientist%' THEN 'Principal Scientist'
             WHEN title LIKE '%php%developer%' THEN 'PHP Developer'
             WHEN title LIKE '%data architect%' THEN 'Data Architect'
             WHEN title LIKE '%mechanical engineer%' THEN 'Mechanical Engineer'
             WHEN title LIKE '%civil engineer%' THEN 'Civil Engineer'
             WHEN title LIKE '%product analyst%' THEN 'Product Analyst'
             WHEN title LIKE '%ai prompt engineer%' THEN 'AI Prompt Engineer'
             WHEN title LIKE '%nurse%' THEN 'Nurse'
             WHEN title LIKE '%ai engineer%' THEN 'AI Engineer'
             WHEN title LIKE '%angular developer%' THEN 'Angular Developer'
             WHEN title LIKE '%project manager%' THEN 'Project Manager'
             WHEN title LIKE '%c++%' THEN 'C++ Developer'
            ELSE title
           END AS title_cleaned,
          title,
          company,
          salary,
          location,
          seniority_level,
          job_type,
          industries,
          job_function,
          posted_on,
          job_url,
          DATE,
          DEG,
          DOMAIN,
          TRANSFORM(hard_skills, skill -> 
              CASE
                WHEN lower(skill) LIKE '%ai chatbot tuning%' THEN 'AI Chatbot Tuning and Training'
                WHEN lower(skill) LIKE '%2d and%' THEN '2D and 3D vision technology'
                WHEN lower(skill) LIKE 'ab%test%' or lower(skill) LIKE 'ab%experiment%' THEN 'A/B Testing'
                WHEN lower(skill) LIKE 'aiaS' THEN 'AIaaS'
                WHEN lower(skill) LIKE '%arima%' THEN 'Arima Modeling'
                WHEN lower(skill) LIKE 'apismicroservices' THEN 'APIs Microservices'
                WHEN lower(skill) LIKE '%arcgis%' THEN 'ARCGIS Notebook'
                WHEN lower(skill) LIKE '%arvr%' THEN 'AR/VR Systems'
                WHEN lower(skill) LIKE 'asr' THEN 'ASR systems'
                WHEN lower(skill) LIKE '%sagemaker%' THEN 'AWS Sagemaker'
                WHEN lower(skill) LIKE '%amazon%web service' THEN 'AWS Platform'
                WHEN lower(skill) LIKE '%postgre%' THEN 'PostgreSQL'
                WHEN lower(skill) LIKE '%recomnmend%' THEN 'Recommender system'
                WHEN lower(skill) LIKE '%reinforce%' THEN 'Reinforcement Learning'
                WHEN lower(skill) LIKE '%no%sql%' THEN 'NoSQL Database'
                WHEN lower(skill) LIKE '%deep%neural%net%' THEN 'Deep Neural Network'
                WHEN lower(skill) LIKE '%networking%' THEN 'Networking'
                WHEN lower(skill) LIKE '%stat%model%' THEN 'Statistical Modeling Technique'
                WHEN lower(skill) LIKE '%workflow%' THEN 'Workflow Orchestration Tool'
                WHEN lower(skill) LIKE '%azure%data%facto%' THEN 'Azure Data Factory'
                WHEN lower(skill) LIKE 'adobe acrobat%' THEN 'Adobe Acrobat'
                WHEN lower(skill) LIKE 'autocad%' THEN 'Autocad'
                WHEN lower(skill) LIKE 'dsl%' THEN 'Dslr Camera Photography/Videography'
                WHEN lower(skill) LIKE 'geographic%' THEN 'Geographic Information System'
                WHEN lower(skill) LIKE '%google%analytics%' THEN 'Google Analytics'
                WHEN lower(skill) LIKE '%spark%' THEN 'Apache Spark'
                WHEN lower(skill) LIKE '%python%' THEN 'Python'
                WHEN lower(skill) LIKE '%mysql%' THEN 'MySQL Database'
                WHEN lower(skill) LIKE '%airflow%' THEN 'Apache Airflow'
                WHEN lower(skill) LIKE '%agile%' THEN 'Agile Software Development'
                WHEN lower(skill) LIKE '%java%script%' THEN 'Javascript'
                WHEN lower(skill) LIKE '%tensorflow%' THEN 'Tensorflow'
                WHEN lower(skill) LIKE '%java%' THEN 'Java'
                WHEN lower(skill) LIKE '%react%' THEN 'ReactJS'
                WHEN lower(skill) LIKE '%angular%' THEN 'AngularJS'
                WHEN lower(skill) LIKE '%node%' THEN 'Node'
                WHEN lower(skill) LIKE '%tableau%' THEN 'Tableau'
                WHEN lower(skill) LIKE '%next%js%' THEN 'NextJS'
                WHEN lower(skill) LIKE '%docker%' THEN 'Docker'
                WHEN lower(skill) LIKE '%power%bi%' THEN 'Power BI'
                WHEN lower(skill) LIKE 'net' THEN '.NET'
                WHEN lower(skill) LIKE 'data warehous%' THEN 'Data Warehousing'
                WHEN lower(skill) LIKE 'data integration' THEN 'Data Integration'
                WHEN lower(skill) LIKE 'data model%' THEN 'Data Modeling'
                WHEN skill LIKE 'API%' THEN 'API'
                WHEN lower(skill) LIKE 'data clean%' THEN 'Data Cleaning'
                WHEN lower(skill) LIKE '%linux%' THEN 'Linux'
                WHEN lower(skill) LIKE 'scala' THEN 'scala'
                WHEN lower(skill) LIKE 'google cloud%' THEN 'Google cloud platform'
                WHEN lower(skill) LIKE 'git' THEN 'GIT'
                WHEN lower(skill) LIKE '%.net%' THEN '.NET'
                WHEN lower(skill) LIKE '%aws%cloud%' OR lower(skill) LIKE 'aws' OR lower(skill) LIKE '%aws%service%' THEN 'AWS Services'
                WHEN lower(skill) LIKE '%aws%glue%' THEN 'AWS Glue'
                WHEN lower(skill) LIKE '%lambda%' THEN 'AWS Lambda'
                WHEN lower(skill) LIKE '%azure%cloud%' THEN 'Azure Cloud Services'
                WHEN lower(skill) LIKE '%kubernetes%' THEN 'Kubernetes'
                WHEN lower(skill) LIKE '%etl%' THEN 'ETL'
                WHEN lower(skill) LIKE 'integration' THEN 'Integration'
                WHEN lower(skill) LIKE '%large%language%model%' or lower(skill) LIKE 'llm%' THEN 'Large Language Model (LLM)'
              ELSE skill
              END
            ) AS hard_skills,
          EMAIL,
          EXP,
          LOC,
          ORG,
          PER,
          PHONE,
          PROJECT,
          ROLE,
          soft_skills,
          UNI,
          state,
          place,
          company_url,
          company_name,
          website,
          company_industry,
          company_size,
          headquarters,
          type,
          founded,
          specialties,
          year,
          month,
          day
           
        FROM global_temp.cleaned_temp_df
        '''
        ) """
    # clean_df.printSchema()
    global val_dict
    clean_df = add_verticals(clean_df)
    clean_df = filter_managers(clean_df)
    val_dict['scrapped_data_count_after_removing_managerial_post'] = clean_df.count()
    # print(clean_df.show(truncate=False))
        
    return clean_df


def explode_mr_table():
    """
    Cleans the mr table DataFrame by implementing explode,trim and adding new id column the data.

    Parameters:
    df (DataFrame): The input hires table DataFrame.

    Returns:
    DataFrame: The cleaned hires table DataFrame.
    """
    
    #Previous Code END----------------
    df = df.withColumn("hard_skills_array", col("hard_skills"))
    df = df.withColumn("hard_skills", explode((col("hard_skills"))))
    df = df.sort('extract_date').withColumn("id", monotonically_increasing_id() + 1).cache()
    return df
    
def validate_department_count():
    time.sleep(3)
    query = """
            SELECT department, extract_date,year ,month, day, COUNT(*) AS record_count
            FROM global_temp.scrapped_data
            GROUP BY department, extract_date, year, month, day
        """
    
    print(f"Validating DEPartment \n {query}")
        
    
def start_cleaning_data():
        print("*** STARTING CLEANING SCRIPT")
        validate_department_count()
        filter_company_industries()
        
        
        

    
def write_val_df_to_s3(df, dest_database:str, dest_table:str, dest_bucket:str, latest_year:str, latest_month:str, latest_day:str):
    year, month, day = latest_year, latest_month, latest_day

    # Destination table path to write to
    dest_write_path = f's3://{dest_bucket}/{dest_table}'

    # Create DynamicFrame from the DataFram
    

    print(f'Completed Writing: {dest_table} for date: {latest_year}-{latest_month}-{latest_day}')
    
def write_to_silver_bucket_single_table(src_database: str, dest_database: str, dest_bucket: str, dest_prefix: str, table: str, remaining_dates: List[List[str]]):
    """
    Writes data from a single table to the silver bucket for the specified remaining dates.

    Parameters:
    src_database (str): The name of the source Glue catalog database.
    dest_database (str): The name of the destination Glue catalog database.
    dest_bucket (str): The destination S3 bucket.
    dest_prefix (str): The prefix within the S3 bucket where the DataFrame will be written.
    table (str): The name of the table in the Glue catalog.
    remaining_dates (List[List[str]]): A list of remaining dates to synchronize.

    Returns:
    None
    """
    
    if table == "linkedin_scrapped_data":
        for date in remaining_dates:
            print(f"For date: {date}")
            year, month, day = date
            print(f" db,table,year,month,day is {src_database}, {table}, {year}, {month} ,{day}")
            df = create_df_from_catalog(
                database=src_database,
                table=table,
                year=year,
                month=month,
                day=day
            )
            global val_dict
            val_dict['initial_scrapped_data_count'] = df.count()
           
            
            
            linkedin_companies_table = 'linkedin_companies'
            df = join_companies(df,src_database,linkedin_companies_table)
            df.show(2)
    
            # print(f"dataframe is {df.printSchema()}")
            # if table == 'linkedin_scrapped_data_clean':
            
           
            count_validation_df = count_validation_df.withColumn("year",f.lit(year)).withColumn("month",f.lit(month)).withColumn("day",f.lit(day))  
            count_validation_df = count_validation_df.withColumn("Date", f.concat_ws("-", "year", "month", "day"))
            count_validation_df = count_validation_df.withColumn("Date", f.col("Date").cast("date"))
            
            val_dict.clear()
            
            
            write_df_to_s3(
                dest_bucket=dest_bucket,
                dest_prefix=dest_prefix,
                dest_database=dest_database,
                dest_table='linkedin_scrapped_data_clean'
            )
            print("Unexploded Data written to S3")
            
            write_df_to_s3(
                dest_bucket=dest_bucket,
                dest_prefix=dest_prefix,
                dest_database=dest_database,
                dest_table='linkedin_scrapped_data'
              )
            print("Exploded Data written to S3")
        else:
            print("Related to hubspot")
        
        

def write_to_silver_bucket_all_tables(src_database: str, dest_database: str, dest_bucket: str, dest_prefix: str):
    """
    Writes data from all tables in the source database to the silver bucket.

    Parameters:
    src_database (str): The name of the source Glue catalog database.
    dest_database (str): The name of the destination Glue catalog database.
    dest_bucket (str): The destination S3 bucket.
    dest_prefix (str): The prefix within the S3 bucket where the DataFrame will be written.

    Returns:
    None
    """
    src_tables = get_tables_list(database=src_database)
    if not check_database_exists(database=dest_database):
       
         print(f"src_tables is {src_tables}")
    
    for table in src_tables:
        remaining_dates = remaining_dates_to_sync(
            src_database=src_database,
            dest_database=dest_database,
            src_table=table,
            dest_table=table
        )
        print(remaining_dates)
        write_to_silver_bucket_single_table(
            src_database=src_database,
            dest_database=dest_database,
            dest_bucket=dest_bucket,
            dest_prefix=dest_prefix,
            table=table,
            remaining_dates=remaining_dates
        )




if __name__ == '__main__' :
    start_cleaning_data()
    