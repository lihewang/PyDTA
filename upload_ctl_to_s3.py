# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 12:36:08 2021
Utility program, not used in the model
@author: lihe.wang
"""
import boto3

if __name__ == "__main__":

    s3 = boto3.resource('s3')
    data = open('control.yaml', 'rb')
    s3.Bucket('cf-model-poc').put_object(Key='control.yaml', Body=data) 

    