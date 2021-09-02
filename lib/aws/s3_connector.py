from boto3 import client, resource

class S3Connector ():
    def __init__(self,sec_token, region):
        self.client = client('s3', region)
        self.resource = resource('s3')

    def parseObjectDict(self, location):
        location = location.replace('s3://','').replace('arn:aws:s3:::','').replace('s3a://','')
        return {
            'Bucket' : location.split('/')[0],
            'Key' : '/'.join(location.split('/')[1:])
        }

    def copyObject(self, source_location, dest_location):
        cpy_src = self.parseObjectDict(source_location)
        parsed_dest = self.parseObjectDict(dest_location)

        return self.client.copy(cpy_src, parsed_dest['Bucket'], parsed_dest['Key'])
    
    def listBuckets(self):
        return self.client.list_buckets()['Buckets']

    def createBucket(self, bucket_name):
        return self.client.create_bucket(Bucket=bucket_name)

    def deleteBucket(self, bucket_name):
        return self.client.delete_bucket(Bucket=bucket_name)

    def listObjects(self,bucket_name):
        return self.client.list_objects_v2(Bucket=bucket_name)['Contents']

    def retreiveObject(self,location):
        parsed_loc = self.parseObjectDict(location)
        r = self.client.get_object(Bucket=parsed_loc['Bucket'],Key=parsed_loc['Key'])
        return r['Body'].read().decode()

    def deleteObject(self,location):
        parsed_loc = self.parseObjectDict(location)
        return self.client.delete_object(Bucket=parsed_loc['Bucket'],Key=parsed_loc['Key'])
