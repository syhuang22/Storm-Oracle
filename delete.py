import boto3

# Initialize a session using your credentials
s3 = boto3.resource('s3')
bucket = s3.Bucket('my-storm-oracle-data')

# Delete all objects in the bucket
bucket.objects.delete()

# If the bucket must be completely deleted, uncomment the following line
# bucket.delete()

print("All objects in the bucket have been deleted.")