import json
import boto3
import os

s3 = boto3.client("s3")
sns = boto3.client("sns")

# sns topics mapping
TOPIC_MAP = {
    "products/": os.environ["PRODUCT_TOPIC"],
    "documents/": os.environ["DOCUMENT_TOPIC"],
    "receipts/": os.environ["RECEIPT_TOPIC"],
    "profilepics/": os.environ["PROFILE_TOPIC"],
}

DEFAULT_TOPIC = os.environ["GENERAL_TOPIC"]


def lambda_s3_handler(event, context):
    
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    file_ext = key.lower().split(".")[-1]
    if file_ext not in ["jpg", "jpeg", "png"]:
        move_to_invalid(bucket, key, "Invalid file type")
        return {"statusCode": 400, "msg": "Invalid file type"}

    head = s3.head_object(Bucket=bucket, Key=key)
    file_size = head["ContentLength"] 

    metadata = {
        "bucket": bucket,
        "file": key,
        "file_extension": file_ext,
        "file_size_bytes": file_size,
        "status": "processed"
    }

    processed_key = f"processed/{key.split('/')[-1]}.json"
    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=json.dumps(metadata),
        ContentType="application/json"
    )

    send_sns_notification(key, metadata)
    return {"statusCode": 200, "body": "Processed successfully"}


def to_invalid(bucket, key, reason):
    invalid_key = f"invalid/{key.split('/')[-1]}"

    s3.copy_object(
        Bucket=bucket,
        CopySource=f"{bucket}/{key}",
        Key=invalid_key
    )
    s3.delete_object(Bucket=bucket, Key=key)


    sns.publish(
        TopicArn=DEFAULT_TOPIC,
        Message=f"File moved to invalid: {key}\nReason: {reason}",
        Subject="Invalid Image Upload"
    )


def send_sns_notification(key, metadata):
    topic_arn = DEFAULT_TOPIC

    for prefix, topic in TOPIC_MAP.items():
        if key.startswith(f"raw/{prefix}"):
            topic_arn = topic
            break

    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(metadata, indent=4),
        Subject="Image Upload Processed"
    )
