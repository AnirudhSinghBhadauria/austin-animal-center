import boto3
import json

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    
    try:
        bucket = 'animal-center'
        key = 'stage/animal-center-raw.csv'
        
        print(f"File uploaded - Bucket: {bucket}, Key: {key}")
        
        response = glue_client.start_job_run(
            JobName='austin-animal-center-processed',
            Arguments={
                '--bucket': bucket,
                '--key': key
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue job started successfully. Job ID: {response['JobRunId']}")
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error starting Glue job: {str(e)}")
        }
