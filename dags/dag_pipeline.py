from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
import pendulum
import io
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import json
import logging

BUCKET_NAME = "aws-awesomeapi-data-lake-072"
logger = logging.getLogger(__name__)

# --------------------- Default arguments for the DAG ---------------------
default_args = {
    'owner': 'hadassa',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 5, 14),
    'end_date': pendulum.datetime(2026, 5, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------- EXTRACTION (BRONZE LAYER) ---------------------
def extract_quotation(**kwargs):
    
    
    url = "https://economia.awesomeapi.com.br/last/GBP-BRL,BRL-GBP,EUR-BRL,BRL-EUR,USD-BRL,BRL-USD"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw_data = response.json()
    
    # Using UTC for global standardization
    now = pendulum.now('UTC')
    data_extraction = now.strftime("%Y-%m-%d_%H-%M-%S")
    archive_name = f"quotation_{data_extraction}.json"
    path_s3 = (
        f"bronze/"
        f"year={now.year}/"
        f"month={now.month}/"
        f"day={now.day}/"
        f"{archive_name}"
    )
    bucket_name=BUCKET_NAME
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=json.dumps(raw_data),
        key=path_s3,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info(f"Bronze Layer: Data extracted and uploaded to S3 at {path_s3}")
    
    # Returning the path so the Silver task can pull it via XCom
    return path_s3

# -------------------- CLEANING (SILVER LAYER) ---------------------
def clean_quotation(ti, **kwargs):
    
    
    # Pulling the exact file path from the Bronze task
    bronze_file_path = ti.xcom_pull(task_ids='extract_quotation')
    
    if not bronze_file_path:
        raise ValueError("No file path found in XCom for 'extract_quotation'")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name=BUCKET_NAME
    
    logger.info(f"Cleaning file from S3 at {bronze_file_path}")
    file_content = s3_hook.read_key(key=bronze_file_path, bucket_name=bucket_name)
    raw_data = json.loads(file_content)
    
    clean_records = []
    numeric_columns = ['high', 'low', 'varBid', 'pctChange', 'bid', 'ask']
    ingestion_time = pendulum.now('UTC').strftime("%Y-%m-%d %H:%M:%S")
    
    for key_coin, info_dict in raw_data.items():
        info_dict['coin'] = key_coin
        info_dict['ingestion_time'] = ingestion_time
        
        for col in numeric_columns:
            if col in info_dict:
                info_dict[col] = float(info_dict[col])
                
        clean_records.append(info_dict)
        
    table = pa.Table.from_pylist(clean_records)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer,compression='snappy')
    
    silver_path = bronze_file_path.replace("bronze/", "silver/").replace(".json", ".parquet")
    
    s3_hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        key=silver_path,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info(f"Silver Layer: Data cleaned and uploaded to S3 at {silver_path}")
    
    return silver_path
    
# -------------------- BUSINESS RULES (GOLD LAYER) ---------------------
def apply_business_rules(ti, **kwargs):
    
    # Pulling the clean Parquet file path from the Silver task
    silver_path = ti.xcom_pull(task_ids='clean_quotation')
    
    if not silver_path:
        raise ValueError("No file path found in XCom for 'clean_quotation'")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name=BUCKET_NAME
    
    file_obj = s3_hook.get_key(key=silver_path, bucket_name=bucket_name)
    file_content = file_obj.get()['Body'].read()
    
    table = pq.read_table(io.BytesIO(file_content))
    silver_records = table.to_pylist()
    
    gold_records = []
    
    for row in silver_records:
        spread = row['ask'] - row['bid']
        volatility = 'HIGH' if abs(row['pctChange']) > 1.0 else 'NORMAL'

        gold_records.append({
            'coin': row['coin'],
            'purchase_price': row['bid'],
            'sale_price': row['ask'],
            'spread': round(spread, 4),
            'volatility': volatility,
            'ingestion_time': row['ingestion_time']
        })
    
    gold_table = pa.Table.from_pylist(gold_records)
    gold_buffer = io.BytesIO()
    pq.write_table(gold_table, gold_buffer, compression='snappy')
    
    gold_path = silver_path.replace("silver/", "gold/")
    
    s3_hook.load_bytes(
        bytes_data=gold_buffer.getvalue(),
        key=gold_path,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info(f"Gold Layer: Business rules applied and data uploaded to S3 at {gold_path}")
        
# ------------------ Orchestration ------------------
with DAG(
    dag_id='dag_pipeline',
    default_args=default_args,
    description='End-to-End Medallion Architecture Pipeline',
    schedule='0 8,12,18 * 5 1-5',
    catchup=False,
) as dag:
    
    task_extract = PythonOperator(
        task_id='extract_quotation',
        python_callable=extract_quotation,
    )
    
    task_crawler_bronze = GlueCrawlerOperator(
        task_id='crawler_bronze',
        config={'Name': 'aws-awesomeapi-bronze-crawler'},
        aws_conn_id='aws_default',
    )
    
    task_clean = PythonOperator(
        task_id='clean_quotation',
        python_callable=clean_quotation,
    )

    task_crawler_silver = GlueCrawlerOperator(
        task_id='crawler_silver',
        config={'Name': 'aws-awesomeapi-silver-crawler'},
        aws_conn_id='aws_default',
    )
    
    task_gold = PythonOperator(
        task_id='apply_business_rules',
        python_callable=apply_business_rules,
    )

    task_crawler_gold = GlueCrawlerOperator(
        task_id='crawler_gold',
        config={'Name': 'aws-awesomeapi-gold-crawler'},
        aws_conn_id='aws_default',
    )
        

    task_extract >> task_crawler_bronze >> task_clean >> task_crawler_silver >> task_gold >> task_crawler_gold
