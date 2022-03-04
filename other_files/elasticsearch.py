import json
from requests_aws4auth import AWS4Auth
import boto3
import requests

dynamodb = boto3.resource('dynamodb', aws_access_key_id="AKIAV55V7BKD7IRN57GY", aws_secret_access_key="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", region_name='us-east-1')
table = dynamodb.Table('yelp-res')

host = 'https://search-restaurant-domain-q4u6riswo2flyel2qwbgezrlsq.us-east-1.es.amazonaws.com'
path = '/restaurants/Restaurant/'
region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def start():
    response = table.scan()
    i = 0
    url = host + path

    headers = {"Content-Type": "application/json"}

    for r in response['Items']:
        payload = {"RestaurantID": r['insertedAtTimestamp'], "Cuisine": r['cuisine']}
        res = requests.post(url, auth=("admin", "Admin@1234"), data=json.dumps(payload).encode("utf-8"), headers=headers)
        i += 1
        print(i)
        print(res.text)


if __name__ == '__main__':
    start()
