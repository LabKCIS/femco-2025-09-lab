import os
import re
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# 環境變數設定
TARGET_BUCKET = os.getenv('TARGET_BUCKET')         # e.g. "your-bucket"
TARGET_PREFIX = os.getenv('TARGET_PREFIX', 'gz') # e.g. "iot"

# 檔名格式檢查與抓日期的正規
FILENAME_REGEX = re.compile(r'^(\d{4}-\d{2}-\d{2})\.csv\.gz$')

def lambda_handler(event, context):
    """
    1. 解析 S3 event
    2. 取出檔名，驗證格式
    3. 組出新路徑：TARGET_PREFIX/t_open_date={date}/{filename}
    4. copy_object & delete_object
    """
    # print event on log
    logger.info(f"event: {event}")
    logger.info(f"TARGET_BUCKET: {TARGET_BUCKET}")
        
    for rec in event['Records']:
        src_bucket = rec['s3']['bucket']['name']
        src_key    = rec['s3']['object']['key']
        filename   = src_key.split('/')[-1]

        # 驗證檔名
        m = FILENAME_REGEX.match(filename)
        if not m:
            logger.warning(f"檔名不符合預期格式，跳過: {filename}")
            continue

        date_str = m.group(1)  # e.g. "2025-07-30"
        dest_key = f"{TARGET_PREFIX}/t_open_date={date_str}/{filename}"

        try:
            # 1. Copy 到目標路徑
            copy_source = {'Bucket': src_bucket, 'Key': src_key}
            logger.info(f"Copy s3://{src_bucket}/{src_key} → s3://{TARGET_BUCKET}/{dest_key}")
            s3.copy_object(
                Bucket=TARGET_BUCKET,
                Key=dest_key,
                CopySource=copy_source
            )
            # 2. (選擇性) 刪除原始檔
            logger.info(f"刪除原檔 s3://{src_bucket}/{src_key}")
            s3.delete_object(Bucket=src_bucket, Key=src_key)

        except Exception as e:
            logger.error(f"檔案處理失敗: {e}", exc_info=True)
            raise

    return {
        'statusCode': 200,
        'body': 'OK'
    }
