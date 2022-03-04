import requests
import boto3
import datetime
from time import sleep
from decimal import *

dynamodb = boto3.resource('dynamodb', aws_access_key_id="AKIAV55V7BKD7IRN57GY", aws_secret_access_key="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", region_name='us-east-1')
table = dynamodb.Table('yelp-res')

api_key = 'sXPUTTkDDD_gkK0xPOpVCG6m7TZLRY0Zuw_RLXqppfOR5ftQHivUwQyPrR1aVdJWBfJNpyK-0uT7NwN0_SbozZw5f5aA139Tl5_Ig9SWqX0QlTh0wB008z-J19YeYnYx'
url = 'https://api.yelp.com/v3/businesses/search'
restaurants = set()


def find(cuisine, offset):
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    params = {'term': cuisine + ' restaurants',
              'location': 'Manhattan',
              'limit': 50,
              'offset': offset}
    response = requests.get(url, headers=headers, params=params)
    data_dict = response.json()
    return data_dict


def start():
    cuisines = ['indian', 'chinese', 'mexican', 'italian', 'american', 'korean']

    for cuisine in cuisines:
        offset = 0
        while offset < 1000:
            res = find(cuisine, offset)
            add_items(res["businesses"], cuisine)
            offset += 50


def add_items(data, cuisine):
    for r in data:
        if r['name'] in restaurants:
            continue
        restaurants.add(r['name'])
        r['cuisine'] = cuisine
        r['insertedAtTimestamp'] = str(datetime.datetime.now())
        r['address'] = r['location']['display_address']
        r['rating'] = Decimal(str(r['rating']))
        r['coordinates']['latitude'] = Decimal(str(r['coordinates']['latitude']))
        r['coordinates']['longitude'] = Decimal(str(r['coordinates']['longitude']))
        r.pop("alias", None)
        r.pop("image_url", None)
        r.pop("url", None)
        r.pop("categories", None)
        r.pop("transactions", None)
        r.pop("distance", None)
        r.pop("location", None)
        r.pop("display_phone", None)
        if r["phone"] == "":
            r.pop("phone", None)

        table.put_item(Item=r)
        sleep(0.001)


if __name__ == '__main__':
    start()
