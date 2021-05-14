import boto3
import pprint as p

client = boto3.client('batch')
response = client.submit_job(
    jobName='pydta-job-py',
    jobQueue='model-job-queue',
    
    jobDefinition='pydta:9',

    containerOverrides={
        'vcpus': 48,
        'memory': 64000,
        'command': [
            'python', 
            '/app/main.py',
        ]     
    }
)

p.pprint(response)
