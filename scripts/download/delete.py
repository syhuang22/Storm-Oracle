import os
import boto3

# Initialize a session using your credentials
s3 = boto3.resource('s3')
bucket = s3.Bucket('my-storm-oracle-data')

# Delete all objects in the bucket
bucket.objects.delete()

# If the bucket must be completely deleted, uncomment the following line
# bucket.delete()

print("All objects in the bucket have been deleted.")

# 清除日志文件
log_file = '/home/azureuser/Storm-Oracle/logs/download_sst.log'
if os.path.exists(log_file):
    with open(log_file, 'w') as file:
        file.write("")
    print(f"Log file {log_file} has been cleared.")
else:
    print(f"Log file {log_file} does not exist.")
