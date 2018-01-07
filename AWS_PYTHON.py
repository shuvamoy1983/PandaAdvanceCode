import boto3
s3 = boto3.resource('s3')
upld = boto3.client('s3')
s3.create_bucket(Bucket='sumittest555', CreateBucketConfiguration={
    'LocationConstraint': 'us-west-2'})

bucket_name='sumittest555'
filename='/Users/shuvamoymondal/Downloads/Emp.txt'
upld.upload_file(filename, bucket_name, filename)