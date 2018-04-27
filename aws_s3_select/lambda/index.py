import json
from urllib.parse import parse_qs
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('s3')

#For better logging of the context object as json since it is not serializable by default
#source: https://gist.github.com/gene1wood/24e431859c7590c8c834
class PythonObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,
            (list, dict, str, bytes,
            int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)
        elif hasattr(obj, '__repr__'):
            return obj.__repr__()
        else:
            return json.JSONEncoder.default(self, obj.__repr__())

def handler(event, context):
    logger.info('Event: %s' % json.dumps(event))
    logger.info('Context: %s' % json.dumps(vars(context), cls=PythonObjectEncoder))

    query_params = parse_qs(event['body-json'])
    logger.info('Form input: {0}'.format(query_params))
    bucket = query_params['bucket'][0]
    key = query_params['key'][0]
    expression = query_params['query'][0]
    #header checkbox only sent if checked
    has_header = "false"
    if 'header' in query_params:
        has_header = query_params['header'][0]
    #assume no header by default
    inputheader = "IGNORE"
    if has_header == "true":
        inputheader = "USE"
    logger.info('Bucket: {0}\nKey: {1}\nQuery: {2}'.format(bucket,key,expression))

    if bucket is None or key is None or expression is None:
        raise Exception('bucket, key and expression must all be present')

    response = client.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization={
            'CompressionType': 'NONE',
            'CSV': {
                'FileHeaderInfo': inputheader,
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

    results = []

    event_stream = response['Payload']
    # Iterate over events in the event stream as they come
    for item in event_stream:
        # If we received a records event, add it to results
        # These are bytestrings so we have to decode them
        if 'Records' in item:
            logger.info("Record data: {0}".format(item['Records']['Payload']))
            data = item['Records']['Payload'].decode("utf-8")
            results.append(data)
        # If we received a progress event, log the details
        elif 'Progress' in item:
            logger.info(item['Progress']['Details'])
    
    #create plain string to return and remove trailing whitespace
    results = ''.join(results).rstrip()
    
    logger.info('Results: {0}'.format(results))
    return results
