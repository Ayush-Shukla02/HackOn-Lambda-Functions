import boto3
import csv
import io
import json

# Initialize AWS clients
s3 = boto3.client('s3')
personalize = boto3.client('personalize-runtime')  # Initialize Personalize client
BUCKET_NAME = 'test-bucket-pewpew'
CAMPAIGN_ARN = 'your-campaign-arn-here'  # Replace with your campaign ARN

def personalize_recomm(user_id, user_mood):
    # Get recommendations from Amazon Personalize
    response = personalize.get_recommendations(
        campaignArn=CAMPAIGN_ARN,
        userId=str(user_id),
        context={
            'MOOD': user_mood
        }
    )
    # Extract item IDs from the response
    item_ids = [item['itemId'] for item in response['itemList']]
    return item_ids

def lambda_handler(event, context):
    # Check if queryStringParameters is in event and if userId is provided
    if 'queryStringParameters' not in event or 'userId' not in event['queryStringParameters']:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'userId is required'})
        }

    user_id = event['queryStringParameters']['userId']
    user_mood = event['queryStringParameters'].get('mood', 'neutral')  # Extract mood, default to 'neutral' if not provided

    # Get recommended item IDs
    # recommended_movie_ids = personalize_recomm(user_id, user_mood)
    recommended_movie_ids = ["5","42","346","779","1","2","6","7","8","9","10","11","12","13","14","22","79","110","216","87"]

    # Fetch movie details from the CSV file in S3
    s3_response = s3.get_object(Bucket=BUCKET_NAME, Key='final2.csv')
    csv_file = s3_response['Body'].read().decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(csv_file))

    # Filter the CSV data to get details of the recommended movies
    movies = []
    for row in csv_reader:
        if row['movieId'] in recommended_movie_ids:
            movies.append(row)
    
    # Construct the response
    response = {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        },
        "body": json.dumps(movies)
    }
    
    return response
