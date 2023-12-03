import boto3
import csv
import io
from collections import defaultdict

BUCKET_NAME = 'test-bucket-pewpew'
INTERACTIONS_FILE_NAME = 'interactions_with_recom.csv'

s3 = boto3.client('s3')
def lambda_handler(event, context):
    response_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }

    # Extract user_id from the event
    user_id =event['queryStringParameters']['userId']

    # Fetch interactions from S3
    try:
        s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=INTERACTIONS_FILE_NAME)
        csv_content = s3_object['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching S3 object: {e}")
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': 'Failed to fetch interactions'
        }
        
    reader = csv.DictReader(io.StringIO(csv_content))
    interactions = list(reader)

    # Filter interactions for the given user ID
    user_interactions = [row for row in interactions if row['USER_ID'] == user_id]
    event_counts = defaultdict(int)
    for row in user_interactions:
        event_counts[row['EVENT_TYPE']] += 1
        
    return {
        'statusCode': 200,
        'headers': response_headers,
        'body': {
            'event_counts': dict(event_counts)
        }
    }

