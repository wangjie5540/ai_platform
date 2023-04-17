import boto3
import digitforce.aip.common.utils.config_helper as config_helper


class S3Client(object):
    _client = None

    def __init__(self, endpoint_url=None, access_key=None, secret_key=None):
        if endpoint_url is None or access_key is None or secret_key is None:
            s3_config = config_helper.get_module_config('s3')
            endpoint_url = s3_config['endpoint_url']
            access_key = s3_config['aws_access_key_id']
            secret_key = s3_config['aws_secret_access_key']
        self.client = boto3.client(
            's3',
            region_name='ap-beijing',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def list_buckets(self) -> list:
        """
        List all buckets
        """
        response = self.client.list_buckets()
        if not response:
            return []
        return [bucket['Name'] for bucket in response['Buckets']]

    def list_objects(self, bucket_name, prefix='', max_keys=20):
        """
        List all objects in a bucket
        :param bucket_name: 桶名
        :param prefix: 前缀
        :param max_keys: 最大返回数量
        :return:
        """
        response = self.client.list_objects_v2(Bucket=bucket_name, MaxKeys=max_keys, Prefix=prefix)
        return [content['Key'] for content in response.get('Contents', [])]

    def upload_file(self, bucket_name, local_file_path, output_file_name):
        self.client.upload_file(
            local_file_path,
            bucket_name,
            output_file_name,
        )

    def download_file(self, bucket_name, key, local_file_path):
        self.client.download_file(Bucket=bucket_name, Key=key, Filename=local_file_path)

    def delete_object(self, bucket_name, key):
        self.client.delete_object(Bucket=bucket_name, Key=key)

    def delete_objects(self, bucket_name, keys):
        self.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {
                        'Key': key
                    } for key in keys
                ]
            }
        )

    def delete_bucket(self, bucket_name):
        self.client.delete_bucket(Bucket=bucket_name)

    def create_bucket(self, bucket_name):
        self.client.create_bucket(Bucket=bucket_name)

    def get_object_str(self, bucket_name, key):
        return self.client.get_object(Bucket=bucket_name, Key=key)['Body'].read().decode('utf-8')

    def get_object_url_with_expire(self, bucket_name, key, expire=3600):
        return self.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': key,
            },
            ExpiresIn=expire,
        )

    @staticmethod
    def get():
        if S3Client._client is None:
            S3Client._client = S3Client()
        return S3Client._client
