import boto3
import requests
import json
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key
import random


region = 'us-east-1' # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-res')

def lambda_handler(event, context):
    print("Triggered!!!!")
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')

    queue_url = "https://sqs.us-east-1.amazonaws.com/407866641031/NoshSQS"
    response = sqs.receive_message(
        QueueUrl = queue_url,
        AttributeNames = ['All'],
        MaxNumberOfMessages = 10,
        MessageAttributeNames = ['All'],
        VisibilityTimeout=60
    )
    
    if 'Messages' in response:  
        for msg in response['Messages']:
            print(msg)
            details = json.loads(msg['Body'])
            print(details)
        
            phone = "+1" + details['phone']
            cuisine = details['cuisine']
            recieptHandle = msg['ReceiptHandle']
            date = details['date']
            time = details['time']
            count = details['count']
             
            # Get restaurants from Dynamo an ES
            elasticSearchUrl = "https://search-restaurant-domain-q4u6riswo2flyel2qwbgezrlsq.us-east-1.es.amazonaws.com/restaurants/_search"
            query = {
                "size": 1000,
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match_phrase": {
                                    "Cuisine": cuisine
                                }
                            }
                        ]
                    }
                }
            }
            
            headers = { "Content-Type": "application/json" }
            r = requests.get(elasticSearchUrl, auth=("admin","Admin@1234"), headers=headers, data = json.dumps(query)).json()
            
            totalValues = r['hits']['total']['value']
            print(totalValues)
            print("Line 61")
            print(r['hits']['hits'][0]['_source']['RestaurantID'])
            
            print("Length returned is"+str(len(r['hits']['hits'])))
            # for t in r['hits']['hits']:
            #     print(str(t))
            
            Results = "Hello! Here are my "+cuisine+" restaurant suggestions for " + count + " people for " +date+" at " +time+": "
            nums = set()
            for i in range(0,3):
                x = random.randint(0, totalValues-1)
                if x not in nums:
                    nums.add(x)
                    print(x)
                    res = r["hits"]["hits"][x]["_source"]["RestaurantID"]
                    print("Res:" + str(res))
                    item = table.query(KeyConditionExpression=Key('insertedAtTimestamp').eq(str(res)))
                    Address = ""
                    for z in item['Items'][0]['address']:
                        Address += z+" "
                    Results += str(i+1)+". "+item['Items'][0]['name'] +", located at " + z
                    if i != 2 :
                        Results += ", "
                    else: Results += ". Enjoy your meal!"
                    
            print(Results)
            
            
            sns_response = sns.publish(PhoneNumber=str(phone), Message=Results)
            print("Message sent successfully")
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=recieptHandle)
            