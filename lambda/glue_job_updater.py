import json
import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        if 's3_to_parquet_job.py' in object_key:
            response = glue.update_job(
                JobName='S3ToParquetGlueJob',
                JobUpdate={'Command': {'ScriptLocation': f"s3://{bucket_name}/{object_key}"}}
            )
            print("Glue Job Updated:", response)
    
    return {'statusCode': 200, 'body': json.dumps('Glue Job Updated')}