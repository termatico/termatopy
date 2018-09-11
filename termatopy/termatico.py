import requests
import pandas as pd
import boto3
import io
from io import StringIO
import json
import re
from .outputs import Logger

import pytz
from datetime import datetime as dt
ausTZ = pytz.timezone("Australia/Melbourne")

loginUrl = "https://trdgpg5gd6.execute-api.ap-southeast-2.amazonaws.com/dev/signIn"
tcoApi = "https://p9009m33d1.execute-api.ap-southeast-2.amazonaws.com/prod"

def getToken(username, password):
    payload = {"username": str(username), "password": str(password)}
    headers = {'content-type': 'application/json'}

    response = requests.request("POST", loginUrl, data = json.dumps(payload), headers = headers)
    token = json.loads(response.content)['idToken']
    return token

def runTemplate(username, password, templateId, payload = {}):
    logData = Logger()
    logData.add_log("INFO", "Getting token...")
    token = getToken(username, password)

    payload = {
        'operationName': 'runTemplate',
        'query': 'mutation runTemplate($templateId : UUID!, $input : JSON!){\n\texecuteTemplate(id : $templateId, input : $input) {\n\t\tid\n\t}\n}',
        'variables': {'input': payload,
        'templateId': templateId}}

    headers = {
        'authorization': "JWT " + token,
        'content-type': "application/json"
        }

    logData.add_log("INFO", "Triggering template...")
    response = requests.request("POST", tcoApi, data = json.dumps(payload), headers=headers)

    if response.status_code == 200:
        executionId = json.loads(response.content)['data']['executeTemplate']['id']
        logData.add_log("INFO", "Template has been triggered")
        output = {"executionId" : executionId, "logs" : logData.logs}
        return output
    else:
        raise Exception(str(response.status_code) + " Error sending request: ", str(response.content))
