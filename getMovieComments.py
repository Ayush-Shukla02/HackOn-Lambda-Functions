import boto3
import csv
from io import StringIO
import json

def fetch_comments_from_s3_csv(bucket_name, file_key, movie_id):
    """Fetch all comments for a specific movieId from a CSV file stored in S3."""
    s3 = boto3.client('s3')
    
    # Fetch the content of the CSV
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')
    
    # Parse the CSV content
    csv_content = StringIO(file_content)
    reader = csv.reader(csv_content)
    
    # Extract header and rows
    header = next(reader)
    rows = list(reader)
    
    # Filter rows based on movieId
    filtered_rows = [row for row in rows if row[1] == str(movie_id)]  # Assuming movieId is the second column
    comments_list = [dict(zip(header, row)) for row in filtered_rows]
    
    return comments_list

def get_movieId_from_tmdbId(tmdb_id, mapping_data):
    """Fetch the corresponding movieId for a given tmdbId."""
    for mapping in mapping_data:
        if mapping['tmdbId'] == str(tmdb_id):
            return mapping['movieId']
    return None

def lambda_handler(event, context):
    tmdbId = event['tmdbId']
    
    # Load the mapping data from final2.csv
    mapping_data = load_dataframe_from_s3('test-bucket-pewpew', 'final2.csv')
    
    if mapping_data is not None:
        # Fetch the corresponding movieId using the mapping
        movieId = get_movieId_from_tmdbId(tmdbId, mapping_data)
        
        if movieId is not None:
            # Fetch comments for the movieId
            bucket_name = 'test-bucket-pewpew'
            file_key = 'user_comments.csv'
            comments_list = fetch_comments_from_s3_csv(bucket_name, file_key, movieId)
            
            return {
                'statusCode': 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                },
                'body': json.dumps(comments_list)
            }
    
    return {
        'statusCode': 404,
        'body': json.dumps({"error": "Comments not found for provided tmdbId"})
    }

def load_dataframe_from_s3(bucket_name, file_key):
    """Load a list of dictionaries from a CSV file stored in S3."""
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            file_content = response['Body'].read().decode('utf-8')
            csv_content = StringIO(file_content)
            reader = csv.DictReader(csv_content)
            data = [row for row in reader]
            return data
        else:
            print(f"Error: Received non-OK status code: {response['ResponseMetadata']['HTTPStatusCode']}")
            return None
    except Exception as e:
        print(f"Error loading data from S3: {str(e)}")
        return None
