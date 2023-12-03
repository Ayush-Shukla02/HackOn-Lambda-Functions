import boto3
import csv
from io import StringIO
import json

def get_sentiment_from_comprehend(text):
    comprehend = boto3.client('comprehend')
    response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    sentiment_score = response['SentimentScore']
    return sentiment_score

def append_to_s3_csv(bucket_name, file_key, row_data):
    """Append a row of data to a CSV file stored in S3."""
    s3 = boto3.client('s3')
    
    # Fetch the existing content of the CSV
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')
    
    # Append the new data
    csv_content = StringIO()
    writer = csv.writer(csv_content)
    
    # Split the existing content into rows and add the new row_data
    existing_rows = file_content.strip().split('\n')
    for row in existing_rows:
        writer.writerow(row.split(','))  # Add existing rows
    writer.writerow(row_data)  # Add the new row
    
    # Write the updated content back to S3
    s3.put_object(Body=csv_content.getvalue(), Bucket=bucket_name, Key=file_key)


def lambda_handler(event, context):
    userId = event['queryStringParameters']['userId']
    comment = event['queryStringParameters']['comment']
    movieId = event['queryStringParameters']['movieId']
    
    # userId = event['userId']
    # comment = event['comment']
    # movieId = event['movieId']
    
    sentiment_score = get_sentiment_from_comprehend(comment)
    
    bucket_name = 'test-bucket-pewpew'
    file_key = 'user_comments.csv'
    
    row_data = [userId, movieId, comment] + list(sentiment_score.values())
    
    append_to_s3_csv(bucket_name, file_key, row_data)
    
    appended_data = {
        'userId': userId,
        'movieId': movieId,
        'comment': comment,
        'SentimentScore': sentiment_score
    }
    return {
        'statusCode': 200,
        "headers": {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        },
        'body': json.dumps(appended_data)
    }

