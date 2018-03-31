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