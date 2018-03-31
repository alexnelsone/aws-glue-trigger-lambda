#boto3 is the AWS python library
import boto3
import json
import logging, inspect
import datetime, time, ast, os




# for local testing set profile
boto3.setup_default_session(profile_name='default')
current_session = boto3.session.Session()
current_region = current_session.region_name

dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')

'''This function is used for logging
   To use it, in your function/catch, pass in your Exception and the Logging Level
   
   Logging Levels are:
       CRITICAL
       ERROR
       WARNING
       DEBUG
       INFO
       NOTSET
       

'''
def log(e, logging_level):
    func_name=inspect.currentframe().f_back.f_code
    logging_level = logging_level.upper()
    print(logging_level+":"+func_name.co_name+':'+str(e))
    
    
''' used to evaluate if any returned structures are empty'''
def is_empty(any_structure):
    if any_structure:
        return False
    else:
        return True
    

'''#This is the main handler for lambda function'''
def lambda_handler(event, context):
    
    # for local testing
    event = "{u'Records': [{u'eventVersion': u'2.0', u'eventTime': u'2018-02-22T21:05:17.065Z', u'requestParameters': {u'sourceIPAddress': u'10.17.84.114'}, u's3': {u'configurationId': u'ae6af604-78ce-4892-bf88-4c4ab9656bdf', u'object': {u'eTag': u'37f87410402060bc06d72abbbc5c4e64', u'sequencer': u'005A8F308CF49526A7', u'key': u'sftp-incoming/np/testuser1/good_25_csv_pipe.csv', u'size': 6831}, u'bucket': {u'arn': u'arn:aws:s3:::np-medda-root-us-east-1', u'name': u'np-medda-root-us-east-1', u'ownerIdentity': {u'principalId': u'A29QZDIGW6XZOP'}}, u's3SchemaVersion': u'1.0'}, u'responseElements': {u'x-amz-id-2': u'ni1vmbeAGOzw3kSPDXt6CkD9BgsYbmxKuxs5z9tAcBGLijD77uNxh8+kx5JmKnu0V6AfzVJ2DFc=', u'x-amz-request-id': u'F63B4DB48BD1BF61'}, u'awsRegion': u'us-east-1', u'eventName': u'ObjectCreated:Put', u'userIdentity': {u'principalId': u'AWS:AROAJE472CMROXCZMLGPU:i-00721247b6bca1809'}, u'eventSource': u'aws:s3'}]}"
    event = ast.literal_eval(event)
    
    sourceBucketName = event['Records'][0]['s3']['bucket']['name']
    sourceKeyName = event['Records'][0]['s3']['object']['key']
    
    fullKey = sourceBucketName + "/" + sourceKeyName
    fullKey = fullKey.rsplit('/',1)[0]
    
    print("Received event on " + fullKey)
    
    if os.environ['GLUE_DYNAMO_TABLE'] != None:
        GLUE_DYNAMO_TABLE = os.environ['GLUE_DYNAMO_TABLE']
    
    table = dynamodb.Table(GLUE_DYNAMO_TABLE)
    
    try:
        dynamoDBResponse = table.get_item(
            Key={ 
                "sourcebucket" : fullKey 
                }
            )
        
        
        
        glueJob = dynamoDBResponse['Item']['glueJob']
        
        
        glueClient = boto3.client('glue', region_name='eu-west-1')
        glueResponse = glueClient.start_job_run(JobName=glueJob)
        
        
        print('Start job with id: ' + glueResponse['JobRunId'])
        
        
        
    except Exception as e:
        print (str(e))


# this calls the main lambda function when developing and debugging locally.
if __name__ == '__main__':
    lambda_handler(None, None)