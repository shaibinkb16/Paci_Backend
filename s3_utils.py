import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, BotoCoreError
from mimetypes import guess_type
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")  # Default to Mumbai

# Initialize S3 client
def get_s3_client():
    try:
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
            raise ValueError("AWS credentials missing in .env")
        
        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        return s3_client
    except Exception as e:
        logger.error(f"Error initializing S3 client: {e}")
        return None

def upload_to_s3(file_data: bytes, bucket: str, s3_key: str, content_type: str = None) -> bool:
    try:
        s3_client = get_s3_client()
        if not s3_client:
            logger.error("S3 client initialization failed.")
            return False

        # Auto-detect content type if not provided
        if content_type is None:
            guessed_type, _ = guess_type(s3_key)
            content_type = guessed_type or "application/octet-stream"

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_data,
            ContentType=content_type
        )

        logger.info(f"Uploaded to S3: s3://{bucket}/{s3_key}")
        return True

    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        return False
    except BotoCoreError as e:
        logger.error(f"S3 error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def list_s3_files(bucket: str, prefix: str = "") -> list:
    """List all files in S3 under a given prefix"""
    s3_client = get_s3_client()
    if not s3_client:
        return []

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        return [obj["Key"] for obj in contents if not obj["Key"].endswith("/")]
    except Exception as e:
        logger.error(f"Failed to list S3 files: {e}")
        return []

def download_s3_file(bucket: str, key: str) -> bytes:
    try:
        s3_client = get_s3_client()
        if not s3_client:
            logger.error("S3 client initialization failed.")
            return None

        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found: s3://{bucket}/{key}")
        return None
    except Exception as e:
        logger.error(f"Error downloading from S3: {e}")
        return None