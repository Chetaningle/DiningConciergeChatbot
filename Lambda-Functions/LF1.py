import boto3
import math
import dateutil.parser
import datetime
import time
import os
import logging
import json
from boto3.dynamodb.conditions import Key
import requests
import random

region = 'us-east-1' # For example, us-west-1
service = 'es'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
users_table = dynamodb.Table('recommendations_table')
table = dynamodb.Table('yelp-res')
user_name = ""

def lambda_handler(event, context):
	os.environ['TZ'] = 'America/New_York'
	time.tzset()
	return dispatch(event)
	
def getSuggestions(cuisine):
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
            
	Results = ""
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
			else: Results += "."
                    
	# print(Results)
	return Results

def dispatch(intentRequest):
	print("Intent Request"+ str(intentRequest))
	print("Request received for userId=" + intentRequest['userId'] + ", intentName=" + intentRequest['currentIntent']['name'])
	intent_name = intentRequest['currentIntent']['name']
	
	if intent_name == "GreetingIntent":
		global user_name
		user_name = intentRequest['currentIntent']['slots']['Name']
		print(intentRequest['sessionAttributes'])
		
		return greetingIntent(intentRequest)
	if intent_name == "DiningSuggestionsIntent":
		return diningSuggestionsIntent(intentRequest)
	if intent_name == "ThankYouIntent":
		return thankYouIntent(intentRequest)


def greetingIntent(intentRequest):
	print("Inside Greet :" + user_name)
	session_attributes = intentRequest['sessionAttributes']
	lower_user_name = user_name
	item = users_table.query(KeyConditionExpression=Key('name').eq(lower_user_name.lower()))
	print("Item "+str(item['Items']))
	if len(item['Items'])>0:
		res = getSuggestions(item['Items'][0]['cuisine'])
		return close(
		session_attributes,
		{'contentType': 'PlainText',
		'content': "Welcome back {0}. Here are the top 3 suggestions from your previous search: {1}".format(user_name, res)},
		'Fulfilled'
	)
	else:
		return close(
		session_attributes,
		{'contentType': 'PlainText',
		'content': "Hi {0}. How can I help you today?".format(user_name)},
		'Fulfilled'
	)
	
def parse_int(n):
	try:
		return int(n)
	except ValueError:
		return float('nan')
	
def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def get_slots(intentRequest):
	return intentRequest['currentIntent']['slots']
	
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def build_validation_result(is_valid, violated_slot, message_content):
	if message_content is None:
		return {
			"isValid": is_valid,
			"violatedSlot": violated_slot,
		}

	return {
		'isValid': is_valid,
		'violatedSlot': violated_slot,
		'message': {'contentType': 'PlainText', 'content': message_content}
	}
	
	
def isvalid_date(date):
	try:
		dateutil.parser.parse(date)
		return True
	except ValueError:
		return False

    
def validateInput(city, cuisine, count, date, time, phone, intentRequest):
	sameDate = False
	if city is not None and city.lower() != "manhattan":
		return build_validation_result(False, 'City', 'Sorry! We only serve in Manhattan at the moment. Please try again!')
        
	cuisines = ["chinese", "indian", "mexican", "italian", "american", "korean"]
	if cuisine is not None and cuisine.lower() not in cuisines:
		return build_validation_result(False, 'Cuisine', 'This cuisine is currently unavailable.  Please try again with another cuisine!')
        
	if count is not None: 
		count = int(count)
		if count < 1 or count>20:
			return build_validation_result(False, 'Count', 'Cannot accomodate the specifed number of people. Please try again!')
			
	if date is not None:
		if not isvalid_date(date):
			return build_validation_result(False, 'Date',
										   'Please try entering your preferred time again.')
		elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
			return build_validation_result(False, 'Date', 'Please enter a date that has not passed.')
		
		if datetime.datetime.strptime(date, '%Y-%m-%d').date() == datetime.date.today():
			sameDate = True

	if time is not None and sameDate:
		hour, minute = time.split(':')
		hour = parse_int(hour)
		minute = parse_int(minute)
		curr_hour = int(datetime.datetime.now().hour)
		curr_min = int(datetime.datetime.now().minute)
		
		print("Curr Hour:"+str(curr_hour))
		print("Curr Minute:"+str(curr_min))
		print("Given Hour:"+str(hour))
		print("Given Minute:"+str(minute))
		if math.isnan(hour) or math.isnan(minute):
			return build_validation_result(False, 'Time', 'Invalid value entered')
	
		
		if curr_hour>hour:
			return build_validation_result(False, 'Time', 'Invalid value entered')
		elif curr_hour==hour and curr_min>=minute:
			return build_validation_result(False, 'Time', 'Invalid value entered')
	
	if phone is not None and len(phone) != 10:
		return build_validation_result(False, 'PhoneNum',
										   'Please enter a valid phone number.')
		
	return build_validation_result(True, None, None)
	
	
def diningSuggestionsIntent(intentRequest):
	print("Dining")
	global user_name
	
	temp = user_name
	print("User Name is: "+ temp)
	session_attributes = intentRequest['sessionAttributes']
	
	city = intentRequest['currentIntent']['slots']['City']
	cuisine = intentRequest['currentIntent']['slots']['Cuisine']
	count = intentRequest['currentIntent']['slots']['Count']
	date = intentRequest['currentIntent']['slots']['Date']
	time = intentRequest['currentIntent']['slots']['Time']
	phone = intentRequest['currentIntent']['slots']['PhoneNum']
	source = intentRequest['invocationSource']

	print("Source:"+source)
	if source == 'DialogCodeHook':
		slots = get_slots(intentRequest)
		validation_result = validateInput(city, cuisine, count, date, time, phone, intentRequest)
		if not validation_result['isValid']:
			slots[validation_result['violatedSlot']] = None
			return elicit_slot(intentRequest['sessionAttributes'],
                               intentRequest['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
                               
		output_session_attributes = intentRequest['sessionAttributes'] if intentRequest['sessionAttributes'] is not None else {}
        
		return delegate(output_session_attributes, get_slots(intentRequest))
		
	if 1:
		print("NAme at line 241"+temp.lower())
		# print("SessionVariable"+intentRequest['sessionAttributes']['name'])
		user_preference = {"name":temp.lower(), "cuisine":cuisine}
		response = users_table.put_item(Item=user_preference)
		print("Response---------"+str(response))
		sqs = boto3.client('sqs')
        
		queueUrl = "https://sqs.us-east-1.amazonaws.com/407866641031/NoshSQS"
		msg = {"cuisine": cuisine, "phone": phone, "date": date, "time": time, "count": count}
		print("Sending to SQS.....")
        
		response = sqs.send_message(
			QueueUrl=queueUrl,
			MessageBody=json.dumps(msg)
			)
       
		print(response)
		
		return close(
		session_attributes,
		{'contentType': 'PlainText',
		  'content': 'You’re all set. Expect my suggestions shortly! Have a good day.'},
		'Fulfilled'
	)
	

def thankYouIntent(intentRequest):
	print("ThankYou")
	session_attributes = intentRequest['sessionAttributes']
	return close(
		session_attributes,
		{'contentType': 'PlainText',
		  'content': 'You\'re welcome'},
		'Fulfilled'
	)


def close(session_attributes, message, fulfillment_state='Fulfilled'):
	print("Inside close")
	response = {
		'sessionAttributes': session_attributes,
		'dialogAction': {
			'type': 'Close',
			'fulfillmentState': fulfillment_state,
			'message': message
        }
    }
	print("Response: " + str(response))
	
	return response