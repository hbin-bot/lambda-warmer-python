import datetime
import random
import time
import os
import json
from boto3 import client as boto3_client
import asyncio

instanceID = datetime.datetime.now().strftime("%Y/%m/%d-%I:%M%p") + '-' + ('0000' + str(int(random.random()*1000)))
warm = False
funcName = os.environ["AWS_LAMBDA_FUNCTION_NAME"]

async def invoke_lambda(lambda_client, funcName, InvocationTypeString, Payload):

	lambda_client.invoke(
		FunctionName=funcName,
		InvocationType=InvocationTypeString,
		LogType='None',
		Payload=json.dumps(Payload)
	)

def handler(event, context):

	global instanceID
	global warm
	global funcName

	config = {
		"flag": 'warmer', #default test flag
		"concurrency": 1, #default concurrency field
		"test": True, #default test flag
		"log": True, #default logging to true
		"correlationId": instanceID, #default the correlationId
		"delay": 75 #default the delay to 75ms
	}

	# handle the default arguments
	for tempKey in config.keys():
		if tempKey in event.keys():
			config[tempKey] = event[tempKey]

	if (event and event["flag"]):
		
		if config["concurrency"] > 1:
			concurrency = config["concurrency"]
		else:
			concurrency = 1

		if '__WARMER_INVOCATION__' in event.keys():
			invokeCount = event['__WARMER_INVOCATION__']
		else:
			invokeCount = 1

		if '__WARMER_CONCURRENCY__' in event.keys():
			invokeTotal = event['__WARMER_CONCURRENCY__']
		else:
			invokeTotal = concurrency

		if '__WARMER_CORRELATIONID__' in event.keys():
			correlationId = event['__WARMER_CORRELATIONID__'] 
		else:
			correlationId = config["correlationId"]

		# flag as warm
		warm = True

		# Create log record
		log = {
			"action": 'warmer',
			"function": funcName,
			"instanceID": instanceID,
			"correlationId": correlationId,
			"count": invokeCount,
			"concurrency": invokeTotal,
			"isWarmed": warm
		}

		# Log it
		if config['log']:
			print(log)

		if concurrency > 1 and not config['test']:

			# invoke
			lambda_client = boto3_client('lambda', region_name="us-east-1")

			loop = asyncio.new_event_loop()

			for i in range(1, concurrency):

				if i == (concurrency-1):
					InvocationTypeString = "RequestResponse"
				else:
					InvocationTypeString = "Event"

				Payload = {
					"flag": config["flag"], #send warmer flag
					"__WARMER_INVOCATION__": i+1, #send invocation number
					"__WARMER_CONCURRENCY__": concurrency, #send total concurrency
					"__WARMER_CORRELATIONID__": correlationId #send correlation id
				}

				future = loop.create_task(invoke_lambda(lambda_client, funcName, InvocationTypeString, Payload))

			loop.run_until_complete(future)
			loop.close()

			return True

		elif invokeCount > 1:
			time.sleep(config['delay']/1000.0)
			return True

		return True
	else:
		warm = True
		return False
