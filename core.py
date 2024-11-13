import os, boto3
from dotenv import load_dotenv

load_dotenv()

KEY = os.getenv('KEY')
REGION = os.getenv('REGION')
LOCAL_PATH = os.getenv('LOCAL_PATH')
BUCKET_NAME = os.getenv('BUCKET_NAME') 
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

def load_file():
    s3_resource = boto3.resource(
        's3', 
        region_name = REGION, 
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY
    ) 
    
    s3_resource.Bucket(BUCKET_NAME).put_object(
        Key = KEY, 
        Body = open(LOCAL_PATH, 'rb')
    )
    
    print('Uploaded succesfully!')
    
if __name__ == '__main__': load_file()
