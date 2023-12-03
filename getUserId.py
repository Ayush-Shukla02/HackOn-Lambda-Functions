import boto3
import csv
import io

# Define your S3 bucket and CSV file
BUCKET_NAME = "test-bucket-pewpew"
CSV_FILE_NAME = "users.csv"
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Extract UUID from the event
    uuid = event['queryStringParameters']['userId']

    # Get the current mapping from S3
    try:
        s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=CSV_FILE_NAME)
        csv_content = s3_object['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching S3 object: {e}")
        return {
            'statusCode': 500,
            "headers": {
              "Access-Control-Allow-Origin": "*",
              "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
              "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': 'Failed to fetch user mapping'
        }

    # Convert CSV content to a list of dictionaries
    reader = csv.DictReader(io.StringIO(csv_content))
    mapping = list(reader)

    # Check if UUID is already mapped
    for row in mapping:
        if row['uuid'] == uuid:
            return {
                'statusCode': 200,
                "headers": {
                  "Access-Control-Allow-Origin": "*",
                  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                  "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
                },
                'body': f"User ID: {row['user_id']}"
            }

    # If not, assign the next available user ID
    used_ids = [int(row['user_id']) for row in mapping]
    for i in range(1, 501):
        if i not in used_ids:
            new_mapping = {
                'uuid': uuid,
                'user_id': str(i)
            }
            mapping.append(new_mapping)
            break
    else:
        return {
            'statusCode': 500,
            "headers": {
              "Access-Control-Allow-Origin": "*",
              "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
              "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': 'All user IDs are taken'
        }

    # Write the updated mapping back to S3
    writer = io.StringIO()
    csv_writer = csv.DictWriter(writer, fieldnames=['uuid', 'user_id'])
    csv_writer.writeheader()
    csv_writer.writerows(mapping)

    try:
        s3.put_object(Body=writer.getvalue(), Bucket=BUCKET_NAME, Key=CSV_FILE_NAME)
    except Exception as e:
        print(f"Error writing to S3: {e}")
        return {
            'statusCode': 500,
            "headers": {
              "Access-Control-Allow-Origin": "*",
              "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
              "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': 'Failed to update user mapping'
        }

    return {
        'statusCode': 200,
        "headers": {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
        },
        'body': f"User ID: {new_mapping['user_id']}"
    }
