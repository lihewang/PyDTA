import boto3
import yaml as ym

def read_control():
    bucket_name = 'cf-model-poc'
    object_name = 'control.yaml'

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, object_name)
    par = ym.safe_load(obj.get()['Body'])
    
    return par