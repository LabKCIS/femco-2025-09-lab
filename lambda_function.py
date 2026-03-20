import os
import re
import boto3
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

# ============================
# 環境變數設定
# ============================
# 原有的
TARGET_BUCKET = os.getenv('TARGET_BUCKET')
TARGET_PREFIX = os.getenv('TARGET_PREFIX', 'gz')

# 新增的 - Athena Cleaning
DATABASE = os.getenv('DATABASE', 'your_database')
RAW_TABLE = os.getenv('RAW_TABLE', 'iot_data')
CLEANED_TABLE = os.getenv('CLEANED_TABLE', 'iot_data_cleaned')
CLEANED_S3_PATH = os.getenv('CLEANED_S3_PATH', 's3://your-bucket/cleaned/')
ATHENA_OUTPUT = os.getenv('ATHENA_OUTPUT', 's3://your-bucket/athena-results/')

# 檔名格式檢查與抓日期的正規
FILENAME_REGEX = re.compile(r'^(\d{4}-\d{2}-\d{2})\.csv\.gz$')


def lambda_handler(event, context):
    """
    1. 解析 S3 event
    2. 取出檔名，驗證格式
    3. 組出新路徑：TARGET_PREFIX/t_open_date={date}/{filename}
    4. copy_object & delete_object
    5. [新增] 觸發 Athena Data Cleaning → 寫入 cleaned table
    """
    logger.info(f"event: {event}")
    logger.info(f"TARGET_BUCKET: {TARGET_BUCKET}")

    processed_dates = []

    for rec in event['Records']:
        src_bucket = rec['s3']['bucket']['name']
        src_key = rec['s3']['object']['key']
        filename = src_key.split('/')[-1]

        # 驗證檔名
        m = FILENAME_REGEX.match(filename)
        if not m:
            logger.warning(f"檔名不符合預期格式，跳過: {filename}")
            continue

        date_str = m.group(1)
        dest_key = f"{TARGET_PREFIX}/t_open_date={date_str}/{filename}"

        try:
            # ============================
            # Step 1: Copy 到目標路徑（原有邏輯）
            # ============================
            copy_source = {'Bucket': src_bucket, 'Key': src_key}
            logger.info(f"Copy s3://{src_bucket}/{src_key} → s3://{TARGET_BUCKET}/{dest_key}")
            s3_client.copy_object(
                Bucket=TARGET_BUCKET,
                Key=dest_key,
                CopySource=copy_source
            )

            # ============================
            # Step 2: 刪除原始檔（原有邏輯）
            # ============================
            logger.info(f"刪除原檔 s3://{src_bucket}/{src_key}")
            s3_client.delete_object(Bucket=src_bucket, Key=src_key)

            # 記錄已處理的日期
            if date_str not in processed_dates:
                processed_dates.append(date_str)

        except Exception as e:
            logger.error(f"檔案處理失敗: {e}", exc_info=True)
            raise

    # ============================
    # Step 3: [新增] 對每個日期執行 Athena Data Cleaning
    # ============================
    cleaning_results = []
    for date_str in processed_dates:
        try:
            result = run_data_cleaning(date_str)
            cleaning_results.append(result)
        except Exception as e:
            logger.error(f"Data Cleaning 失敗 ({date_str}): {e}", exc_info=True)
            raise

    return {
        'statusCode': 200,
        'body': {
            'file_process': 'OK',
            'cleaning_results': cleaning_results
        }
    }


# ============================================================
# 以下為新增的 Data Cleaning 函數
# ============================================================

def run_data_cleaning(t_open_date):
    """對指定日期執行 Data Cleaning 並寫入 cleaned table"""

    logger.info(f"========== 開始 Data Cleaning: {t_open_date} ==========")

    # 判斷 Table 是否存在
    table_exists = check_table_exists(DATABASE, CLEANED_TABLE)

    if not table_exists:
        logger.info(f"Table {CLEANED_TABLE} 不存在，執行 CTAS...")
        query = build_ctas_query(t_open_date)
    else:
        logger.info(f"Table {CLEANED_TABLE} 已存在，清除舊分區後 INSERT INTO...")
        delete_partition_data(t_open_date)
        query = build_insert_query(t_open_date)

    # 執行 Athena 查詢
    query_id = execute_athena_query(query)
    status = wait_for_query(query_id)

    if status == 'SUCCEEDED':
        logger.info(f"✅ Data Cleaning 完成: {t_open_date}")
        return {
            'date': t_open_date,
            'action': 'CTAS' if not table_exists else 'INSERT_INTO',
            'status': 'SUCCEEDED',
            'query_execution_id': query_id
        }
    else:
        error_msg = get_query_error(query_id)
        logger.error(f"❌ Data Cleaning 失敗 ({t_open_date}): {error_msg}")
        raise Exception(f'Athena cleaning failed for {t_open_date}: {error_msg}')


def build_cleaning_select(t_open_date):
    """建立 Data Cleaning 的 SELECT 語句"""

    return f"""
    SELECT
        equipment_name,
        sensor_name,
        CASE
            WHEN sensor_value IS NOT NULL THEN sensor_value
            WHEN prev_val IS NOT NULL AND next_val IS NOT NULL THEN
                prev_val + (next_val - prev_val)
                * ( CAST(to_unixtime(log_time) - to_unixtime(prev_time) AS DOUBLE)
                  / NULLIF(CAST(to_unixtime(next_time) - to_unixtime(prev_time) AS DOUBLE), 0) )
            WHEN prev_val IS NOT NULL THEN prev_val
            WHEN next_val IS NOT NULL THEN next_val
            ELSE NULL
        END AS sensor_value,
        sensor_value AS sensor_value_original,
        log_time,
        anomaly_flag,
        relative_humidity,
        production_status,
        shift,
        coil_start_time,
        coil_end_time,
        product_specification,
        pipe_outer_diameter,
        pipe_thickness,
        pipe_type,
        pipe_standard,
        coil_thickness,
        coil_width,
        coil_material,
        CASE WHEN sensor_value IS NULL THEN TRUE ELSE FALSE END AS is_missing,
        CASE
            WHEN sensor_value IS NOT NULL THEN 'ORIGINAL'
            WHEN prev_val IS NOT NULL AND next_val IS NOT NULL THEN 'LINEAR_INTERPOLATION'
            WHEN prev_val IS NOT NULL THEN 'FORWARD_FILL'
            WHEN next_val IS NOT NULL THEN 'BACKWARD_FILL'
            ELSE 'UNFILLED'
        END AS imputation_method,
        CASE
            WHEN sensor_value IS NULL AND prev_val IS NULL AND next_val IS NULL THEN 'CRITICAL'
            WHEN sensor_value IS NULL THEN 'IMPUTED'
            WHEN anomaly_flag IS NOT NULL
                 AND TRIM(anomaly_flag) != ''
                 AND LOWER(TRIM(anomaly_flag)) NOT IN ('normal', 'n', '0')
                THEN 'ANOMALY'
            ELSE 'CLEAN'
        END AS data_quality_flag,
        CASE WHEN sensor_value IS NULL THEN prev_val ELSE NULL END AS prev_valid_value,
        CASE WHEN sensor_value IS NULL THEN next_val ELSE NULL END AS next_valid_value,
        t_open_date
    FROM (
        SELECT
            *,
            LAST_VALUE(
                CASE WHEN sensor_value IS NOT NULL THEN sensor_value END
            ) IGNORE NULLS OVER (
                PARTITION BY equipment_name, sensor_name
                ORDER BY log_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_val,
            LAST_VALUE(
                CASE WHEN sensor_value IS NOT NULL THEN log_time END
            ) IGNORE NULLS OVER (
                PARTITION BY equipment_name, sensor_name
                ORDER BY log_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_time,
            FIRST_VALUE(
                CASE WHEN sensor_value IS NOT NULL THEN sensor_value END
            ) IGNORE NULLS OVER (
                PARTITION BY equipment_name, sensor_name
                ORDER BY log_time
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) AS next_val,
            FIRST_VALUE(
                CASE WHEN sensor_value IS NOT NULL THEN log_time END
            ) IGNORE NULLS OVER (
                PARTITION BY equipment_name, sensor_name
                ORDER BY log_time
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) AS next_time
        FROM {RAW_TABLE}
        WHERE t_open_date = DATE '{t_open_date}'
    )
    """


def build_ctas_query(t_open_date):
    """首次建表：CTAS"""

    select_sql = build_cleaning_select(t_open_date)

    return f"""
    CREATE TABLE {CLEANED_TABLE}
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        partitioned_by = ARRAY['t_open_date'],
        external_location = '{CLEANED_S3_PATH}'
    ) AS
    {select_sql}
    """


def build_insert_query(t_open_date):
    """後續增量：INSERT INTO"""

    select_sql = build_cleaning_select(t_open_date)

    return f"""
    INSERT INTO {CLEANED_TABLE}
    {select_sql}
    """


def check_table_exists(database, table_name):
    """透過 Glue Catalog 檢查 table 是否存在"""
    try:
        glue_client.get_table(DatabaseName=database, Name=table_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False


def delete_partition_data(t_open_date):
    """刪除 S3 上該日期分區的舊資料，確保冪等"""

    # 解析 CLEANED_S3_PATH → bucket + prefix
    path = CLEANED_S3_PATH.replace('s3://', '')
    parts = path.split('/', 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''

    partition_prefix = f"{prefix}t_open_date={t_open_date}/"

    # 列出該分區下的所有物件
    paginator = s3_client.get_paginator('list_objects_v2')
    objects_to_delete = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=partition_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                objects_to_delete.append({'Key': obj['Key']})

    if objects_to_delete:
        # 批次刪除（每次最多 1000 個）
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i + 1000]
            logger.info(f"刪除舊分區資料: {partition_prefix} ({len(batch)} 個檔案)")
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': batch}
            )
    else:
        logger.info(f"分區 {partition_prefix} 無舊資料，跳過刪除")


def execute_athena_query(query):
    """提交 Athena 查詢"""

    logger.info(f"提交 Athena 查詢 (前200字):\n{query[:200]}...")

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    query_id = response['QueryExecutionId']
    logger.info(f"Query Execution ID: {query_id}")
    return query_id


def wait_for_query(query_execution_id, max_wait=600):
    """輪詢等待 Athena 查詢完成"""

    elapsed = 0
    interval = 5

    while elapsed < max_wait:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            # 取得掃描量資訊
            stats = response['QueryExecution'].get('Statistics', {})
            scanned = stats.get('DataScannedInBytes', 0)
            exec_time = stats.get('TotalExecutionTimeInMillis', 0)
            logger.info(
                f"查詢 {status} | "
                f"耗時: {exec_time/1000:.1f}s | "
                f"掃描: {scanned/1024/1024:.2f} MB"
            )
            return status

        logger.info(f"等待中... {status} ({elapsed}s)")
        time.sleep(interval)
        elapsed += interval

    raise TimeoutError(f'Query {query_execution_id} timed out after {max_wait}s')


def get_query_error(query_execution_id):
    """取得失敗查詢的錯誤原因"""

    response = athena_client.get_query_execution(
        QueryExecutionId=query_execution_id
    )
    return response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')