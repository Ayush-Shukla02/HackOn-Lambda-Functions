import json
import boto3
import csv

s3 = boto3.client('s3')
personalize_events = boto3.client('personalize-events')

BUCKET_NAME = 'test-bucket-pewpew'

INTERACTION_WEIGHTS = {
    'watch': 5,
    'click': 3,
    'like': 5,
    'dislike': -5
}

def get_movieId_from_tmdbId(tmdbId, csv_content):
    reader = csv.DictReader(csv_content.splitlines())
    for row in reader:
        if str(row['tmdbId']) == str(tmdbId):
            return row['movieId']
    return None


def lambda_handler(event, context):
    response_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    try:
        userId = event['queryStringParameters']['userId']
        tmdbId = float(event['queryStringParameters']['tmdbId'])
        event_type = event['queryStringParameters']['event_type']
        timestamp = event['queryStringParameters']['timestamp']
        
        # Fetch the movieId corresponding to the provided tmdbId
        response = s3.get_object(Bucket=BUCKET_NAME, Key="final2.csv")
        csv_content = response['Body'].read().decode('utf-8')
        movieId = get_movieId_from_tmdbId(tmdbId, csv_content)
        
        event_value = INTERACTION_WEIGHTS.get(event_type, 1)  
        modified_csv_data = f"{userId},{movieId},{timestamp},{event_type},{event_value}\n"
        
        csv_data = f"{userId},{movieId},{timestamp},{event_type}\n"
        
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key="modified_full_interaction.csv")
            current_content = response['Body'].read().decode('utf-8')
            modified_csv_data = current_content + modified_csv_data
        except s3.exceptions.NoSuchKey:
            pass
        
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key="ratings.csv")
            current_content = response['Body'].read().decode('utf-8')
            csv_data = current_content + csv_data
        except s3.exceptions.NoSuchKey:
            pass 
        
        s3.put_object(Bucket=BUCKET_NAME, Key="modified_full_interaction.csv", Body=modified_csv_data)
        s3.put_object(Bucket=BUCKET_NAME, Key="ratings.csv", Body=csv_data)
        
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key="interactions_with_recom.csv")
            current_content = response['Body'].read().decode('utf-8')
            recom_csv_data = current_content + f"{userId},{movieId},{timestamp},{event_type},{event_value}\n"
        except s3.exceptions.NoSuchKey:
            recom_csv_data = f"{userId},{movieId},{timestamp},{event_type},{event_value}\n"
        s3.put_object(Bucket=BUCKET_NAME, Key="interactions_with_recom.csv", Body=recom_csv_data)
        
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps('User interaction recorded in CSVs and Personalize!')
        }
        
        # # Record the interaction in AWS Personalize
        # personalize_events.put_events(
        #     trackingId=TRACKING_ID,
        #     userId=str(userId),
        #     sessionId='some-session-id',
        #     eventList=[{
        #         'eventId': 'some-event-id',
        #         'eventType': event_type,
        #         'properties': json.dumps({
        #             'itemId': str(movieId),
        #             'eventValue': str(event_value)
        #         }),
        #         'sentAt': int(timestamp)
        #     }]
        # )
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

