import boto3
import csv
import io
from collections import defaultdict

BUCKET_NAME = 'test-bucket-pewpew'
INTERACTIONS_FILE_NAME = 'ratings.csv'
MOVIE_DETAILS_FILE_NAME = 'final2.csv'

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Enable CORS
    response_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }

    # Extract user_id from the event
    user_id = event['queryStringParameters']['userId']

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

    # Convert CSV content to a list of dictionaries
    reader = csv.DictReader(io.StringIO(csv_content))
    interactions = list(reader)

    # Filter interactions for the given user ID
    user_interactions = [row for row in interactions if row['USER_ID'] == user_id]

    # Fetch movie details from S3
    try:
        s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=MOVIE_DETAILS_FILE_NAME)
        csv_content_movies = s3_object['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching movie details from S3: {e}")
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': 'Failed to fetch movie details'
        }

    # Convert movie details CSV content to a list of dictionaries
    reader_movies = csv.DictReader(io.StringIO(csv_content_movies))
    movies = {row['movieId']: row for row in reader_movies}

    # Sort interactions based on timestamp to get 25 most recent movies watched
    recent_movies = sorted([row for row in user_interactions if row['EVENT_TYPE'] == 'watch'],
                           key=lambda x: x['TIMESTAMP'], reverse=True)[:25]

    # Map movie_ids from recent movies to their details
    recent_movie_details = [movies[movie['ITEM_ID']] for movie in recent_movies if movie['ITEM_ID'] in movies]

    # Count the number of interactions for each EVENT_TYPE
    event_counts = defaultdict(int)
    for row in user_interactions:
        event_counts[row['EVENT_TYPE']] += 1

    return {
        'statusCode': 200,
        'headers': response_headers,
        'body': {
            'recent_movies': recent_movie_details,
            'event_counts': dict(event_counts)
        }
    }
