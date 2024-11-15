import os, boto3
from dotenv import load_dotenv


def download_file_from_s3(bucket_name, s3_file_path, local_file_path):
    try:
        load_dotenv()
        
        s3_client = boto3.client(
            "s3",
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name = os.getenv("REGION"),
        )

        print(bucket_name, s3_file_path, local_file_path)

        s3_client.download_file(bucket_name, s3_file_path, local_file_path)
        print(f"File downloaded successfully to {local_file_path}")
    except Exception as e:
        print(f"Error downloading file: {str(e)}")

download_file_from_s3(
    "animal-center",
    "curated/austin-animal-center-curated.csv",
    "preprocessed/austin-animal-center-curated.csv",
)

