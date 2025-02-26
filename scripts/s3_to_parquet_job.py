import sys
import os
import boto3
import pandas as pd
from io import BytesIO
from awsglue.utils import getResolvedOptions
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'inbound_path', 'outbound_path'])

# Initialize S3 client
s3 = boto3.client('s3')

def split_s3_path(s3_path):
    """
    Splits an S3 path into bucket name and prefix (key).
    """
    path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = path_parts[0]
    key = path_parts[1] if len(path_parts) > 1 else ''
    return bucket, key

def list_s3_objects(bucket, prefix):
    """
    List all objects under a given S3 bucket and prefix.
    """
    objects = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            objects.extend(page['Contents'])
    return objects

def process_csv_to_parquet(in_bucket, in_key, out_bucket, out_key):
    """
    Reads a CSV file from S3, converts it to Parquet, and writes it back to S3.
    """
    try:
        logging.info(f"Processing file: s3://{in_bucket}/{in_key}")

        # Read CSV from S3
        response = s3.get_object(Bucket=in_bucket, Key=in_key)
        df = pd.read_csv(response['Body'])

        # Convert to Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)

        # Upload to S3
        s3.put_object(Bucket=out_bucket, Key=out_key, Body=parquet_buffer.getvalue())

        logging.info(f"Successfully processed {in_key} -> {out_key}")

    except Exception as e:
        logging.error(f"Error processing {in_key}: {str(e)}", exc_info=True)

def main():
    """
    Main function that handles listing, processing, and uploading files.
    """
    in_bucket, in_prefix = split_s3_path(args['inbound_path'])
    out_bucket, out_prefix = split_s3_path(args['outbound_path'])

    # Ensure prefixes end with '/'
    in_prefix = in_prefix.rstrip('/') + '/'
    out_prefix = out_prefix.rstrip('/') + '/'

    logging.info(f"Starting processing from {args['inbound_path']} to {args['outbound_path']}")

    # List CSV files in the inbound S3 path
    all_objects = list_s3_objects(in_bucket, in_prefix)

    for obj in all_objects:
        key = obj['Key']
        if key == in_prefix or not key.lower().endswith('.csv'):
            logging.info(f"Skipping non-CSV file: {key}")
            continue

        # Generate new Parquet file path
        relative_key = key[len(in_prefix):]
        new_file_name = os.path.splitext(os.path.basename(relative_key))[0] + ".parquet"
        output_key = f"{out_prefix}{new_file_name}"

        # Convert CSV to Parquet
        process_csv_to_parquet(in_bucket, key, out_bucket, output_key)

    logging.info("Processing completed successfully.")

if __name__ == "__main__":
    main()