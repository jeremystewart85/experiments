#!/usr/bin/python
import os
import boto3

s3 = boto3.client('s3')
response = s3.select_object_content(
    Bucket="testbucket",
    Key="sample_data/100k_people.csv",
    ExpressionType='SQL',
    Expression="SELECT s.id FROM S3Object s",
    InputSerialization={
        'CompressionType': 'NONE',
        'CSV': {
            'FileHeaderInfo': 'Use',
            'RecordDelimiter': '\n',
            'FieldDelimiter': ',',
        }
    },
    OutputSerialization={
        'CSV': {
            'RecordDelimiter': '\n',
            'FieldDelimiter': ',',
        }
    }
)

event_stream = response['Payload']
with open('output', 'wb') as f:
    # Iterate over events in the event stream as they come
    for event in event_stream:
        # If we received a records event, write the data to a file
        if 'Records' in event:
            data = event['Records']['Payload']
            f.write(data)
        # If we received a progress event, print the details
        elif 'Progress' in event:
            print(event['Progress']['Details'])
