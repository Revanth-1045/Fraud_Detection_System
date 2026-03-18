import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minio',
    aws_secret_access_key='minio123'
)

for b in ['datasets', 'models']:
    try:
        s3.create_bucket(Bucket=b)
        print(f'Created bucket: {b}')
    except Exception as e:
        print(f'Bucket [{b}]: {e}')

print('Done.')
