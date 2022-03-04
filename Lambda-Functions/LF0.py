import json
import boto3
import time
import os

def lambda_handler(event, context):
    user_id, text = get_info_from_request(event)
    print("UID: "+str(user_id))
    user_id="USER"
    if user_id is None or text is None:
        return get_error_response("Unable to get user ID and text from request")
    chatbot_text = get_chatbot_response(user_id, text)
    if chatbot_text is None:
        return get_error_response("Fail to connect with lex")
    else:
        return get_success_response(chatbot_text, user_id)
    

def get_info_from_request(event):
    if "messages" in event:
        messages = event["messages"]
        message = messages[0]
        
        text = None
        user_id = None
        
        if("unstructured" in message):
            if ("text" in message["unstructured"]):
                text = message["unstructured"]["text"]
            if ("user_id" in message["unstructured"]):
                user_id = message["unstructured"]["user_id"]
        
        return user_id,text

def get_error_response(text):
    print("Line 34")
    response = {
        "status code": 200,
        "body": {},
        "messages":[
            {
                "type":"unstructured",
                "unstructured": {
                    "user_id": None,
                    "text": text,
                    "time": time.time(),
                }
            }]
    }
    return response
    
def get_success_response(text,user_id):
    print("Line 53")
    response = {
        "status code": 200,
        "body": {},
        "messages":[
            {
                "type":"unstructured",
                "unstructured": {
                    "user_id": user_id,
                    "text": text,
                    "time": time.time()
                }
            }]
    }
    return response
    
def get_chatbot_response(user_id,text):
    print("Line 75")
    message = ''
    client = boto3.client('lex-runtime')
    lex_response = client.post_text(
        botName ='NoshBot',
        botAlias = 'NoshBot',
        userId = user_id,
        inputText = text,
        sessionAttributes={
        'name': 'string'
        }
    )
    
    if not isinstance(lex_response, dict):
        return None
    
    if 'message' not in lex_response:
        return None
        
    message = lex_response['message']
    return message