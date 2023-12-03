import json
import boto3
import csv
from io import StringIO

def lambda_handler(event, context):
    
    # Input: event contains the userId
    user_id = int(event['queryStringParameters']['userId'])

    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Load the ratings.csv from S3
    response = s3.get_object(Bucket='test-bucket-pewpew', Key='ratings.csv')
    interaction_data = response['Body'].read().decode('utf-8').splitlines()

    # Filter interactions for the given userId and 'watch' event type
    watched_movie_ids = []
    for line in interaction_data[1:]:
        data = line.split(',')
        if len(data) > 3 and data[0] != "null" and data[1] != "null":
            uid, movie_id, _, event_type = data
            if int(uid) == user_id and event_type.strip() == 'watch':
                watched_movie_ids.append(int(movie_id))
    
    print(f"DEBUG: Found {len(watched_movie_ids)} watched movie IDs for user {user_id}")

    # Load the final2.csv from S3
    response = s3.get_object(Bucket='test-bucket-pewpew', Key='final2.csv')
    final2_data = csv.reader(StringIO(response['Body'].read().decode('utf-8')))

    # Create a mapping of movieId to genres using the parsed CSV data
    movie_to_genres = {int(row[0]): row[2] for row in list(final2_data)[1:]}

    # Count the genres
    genre_count = {}
    for movie_id in watched_movie_ids:
        genres = movie_to_genres.get(movie_id, '').split('|')
        for genre in genres:
            genre_count[genre] = genre_count.get(genre, 0) + 1
    
    print(f"DEBUG: Generated genre count: {genre_count}")

    response = {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        },
        "body": json.dumps(genre_count)
    }
    
    return response
