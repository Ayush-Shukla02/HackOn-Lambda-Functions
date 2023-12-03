import boto3
import csv
import math
import json

# Initialize AWS clients
s3 = boto3.client('s3')

def calculate_euclidean_distance(u, p):
    """Calculate Euclidean distance between two vectors u and p."""
    distance = math.sqrt(sum((u[i] - p[i]) ** 2 for i in range(len(u))))
    return distance

def lambda_handler(event, context):
    # Extract parameters from the Lambda event
    userId = event['queryStringParameters']['userId']
    threshold = event['queryStringParameters'].get('threshold', 0.1)  # Default threshold is 0.1
    
    # Retrieve the user's last 10 comments from S3
    user_comments_s3_bucket = "test-bucket-pewpew"
    user_comments_s3_key = "user_comments.csv"
    
    # Retrieve movie sentiment scores from S3
    movie_scores_s3_bucket = "test-bucket-pewpew"
    movie_scores_s3_key = "movie_scores.csv"
    
    # Get the user's last 10 comments from S3
    user_last_10_comments = []
    try:
        response = s3.get_object(Bucket=user_comments_s3_bucket, Key=user_comments_s3_key)
        lines = response['Body'].read().decode('utf-8').split('\n')
        reader = csv.DictReader(lines)
        for row in reader:
            if row['userId'] == userId:
                user_last_10_comments.append(row)
                if len(user_last_10_comments) >= 2:  # Changed to 2 for testing
                    break
    except Exception as e:
        return {"error": str(e)}
    
    # Initialize sentiment vectors for user and movie
    user_sentiment_vector = [0, 0, 0]  # [Positive, Negative, Neutral]
    movie_sentiment_vector = [0, 0, 0]  # [Positive, Negative, Neutral]
    
    # Iterate through the user's last 10 comments
    for row in user_last_10_comments:
        # Extract user sentiment scores
        user_sentiment = [float(row['Positive']), float(row['Negative']), float(row['Neutral'])]
        
        # Find the corresponding movie's public sentiment scores from movie_scores.csv
        movie_public_sentiment = []
        try:
            response = s3.get_object(Bucket=movie_scores_s3_bucket, Key=movie_scores_s3_key)
            lines = response['Body'].read().decode('utf-8').split('\n')
            reader = csv.DictReader(lines)
            for movie_row in reader:
                if int(movie_row['movieId']) == int(row['movieId']):
                    movie_public_sentiment = [
                        float(movie_row['Positive_Score']),
                        float(movie_row['Negative_Score']),
                        float(movie_row['Neutral_Score'])
                    ]
                    break
        except Exception as e:
            return {"error": str(e)}
        
        # Update sentiment vectors
        user_sentiment_vector = [user_sentiment_vector[i] + user_sentiment[i] for i in range(3)]
        movie_sentiment_vector = [movie_sentiment_vector[i] + movie_public_sentiment[i] for i in range(3)]
    
    # Calculate Euclidean distance between user and movie sentiment vectors
    distance = calculate_euclidean_distance(user_sentiment_vector, movie_sentiment_vector)
    
    # Check if the Euclidean distance exceeds the threshold
    if distance > threshold:
        # Determine the sentiment direction based on user sentiment scores
        if user_sentiment_vector[0] > user_sentiment_vector[1]:
            mood = "positive"  # User is more positive
        else:
            mood = "negative"  # User is more negative
    else:
        mood = "neutral"  # User sentiment is within the threshold
    
    # Return the mood as a response
    return {
        'statusCode': 200,
        "headers": {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        },
        'body': json.dumps(mood)
    }
