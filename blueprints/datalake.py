from flask import Blueprint, Response, request, json
from lib.aws.s3_connector import S3Connector

datalake = Blueprint('datalake', __name__, url_prefix='/datalake')

@datalake.route("/list_buckets", methods=['GET'])
def list_buckets():
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.listBuckets()
    return Response(resp.__str__(), 200)

@datalake.route("/create_bucket/<bucket_name>", methods=['GET'])
def create_bucket(bucket_name):
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.createBucket(bucket_name)['Location']
    return Response(resp.__str__(), 200)

@datalake.route("/delete_bucket/<bucket_name>", methods=['GET'])
def delete_bucket(bucket_name):
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.deleteBucket(bucket_name)
    return Response(resp.__str__(), 200)

@datalake.route("/list_objects/<bucket_name>", methods=['GET'])
def list_objects(bucket_name):
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.listObjects(bucket_name)
    return Response(resp.__str__(), 200)

@datalake.route("/copy_object", methods=['POST'])
def copy_object():
    params = json.loads(request.data)
    source_location = params['source_location']
    dest_location = params['dest_location']
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.copyObject(source_location, dest_location)
    return Response(resp.__str__(), 200)

@datalake.route("/delete_object", methods=['POST'])
def delete_object():
    params = json.loads(request.data)
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.deleteObject(params['location'])
    return Response(resp.__str__(), 200)

@datalake.route("/retreive_object", methods=['POST'])
def retreive_object():
    params = json.loads(request.data)
    s3 = S3Connector('token_chamgeme_later', 'us-east-1')
    resp = s3.retreiveObject(params['location'])
    return Response(resp.__str__(), 200)
