import boto3


class S3Helper:

    def __init__(self):
        self.__s3 = boto3.client('s3')

    def write_data_to_s3(self, data, bucket_name, s3_key):
        self.__s3.put_object(Body=data, Bucket=bucket_name, Key=s3_key)

    def delete_file_from_s3(self, bucket_name, s3_key):
        self.__s3.delete_object(Bucket=bucket_name, Key=s3_key)
