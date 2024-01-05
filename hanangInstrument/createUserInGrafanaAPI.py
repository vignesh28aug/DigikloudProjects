import requests
import json
import boto3
import os
from botocore.config import Config
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UserCreation:
  def __init__(self, uname, email, pword):
    self.grafana_url = os.environ['grafana_url']
    self.username = os.environ['username']
    self.password = os.environ['password']
    self.AWS_ACCESS_KEY = os.environ['ACCESS_KEY']
    self.AWS_SECRET_KEY = os.environ['SECRET_KEY']
    self.AWS_TIMESTREAM_DB = os.environ['AWS_TIMESTREAM_DB']
    self.AWS_TABLE_NAME = os.environ['AWS_TABLE_NAME']
    self.region_name = os.environ['region_name']
    self.uname = uname
    self.email = email
    self.pword = pword

  def constructBaseUrl(self):
    base_url = "https://{}:{}@{}".format(self.username, self.password, self.grafana_url)
    return base_url

  def constructPayload(self):
    data = {
      "name": self.uname,
      "email": self.email,
      "login": self.uname,
      "password": self.pword,
      "OrgId": 1
    }
    return data

  def prepare_record(self):
    current_time = str(int(round(time.time() * 1000)))
    record = {
        'Dimensions': [{'Name': 'username', 'Value': self.uname}],
        'Time': current_time,
        'MeasureName': 'user',
        'MeasureValueType': 'VARCHAR',
        'MeasureValue': str(self.uname),
    }
    print("Record to insert into Timestream: %s" % str(record))
    return [record]

  def write_timestream_client(self):
    try:
      write_client = boto3.client(
        'timestream-write',
        aws_access_key_id=self.AWS_ACCESS_KEY,
        aws_secret_access_key=self.AWS_SECRET_KEY,
        config=Config(region_name=self.region_name, read_timeout=20, max_pool_connections=5000,
                      retries={'max_attempts': 10}))
      logger.info("Connection to Timestream Established Successfully !")
      return write_client
    except Exception as e:
      logger.error('Error occured during : %s', str(e))
      return None

  def writeTimestream(self):
    try:
      write_client = self.write_timestream_client()
      response = write_client.write_records(DatabaseName=self.AWS_TIMESTREAM_DB, TableName=self.AWS_TABLE_NAME, Records=self.prepare_record())
      return response
    except write_client.exceptions.RejectedRecordsException as err:
      logger.error(err.response)

  def createUser(self):
    try:
      resp = requests.post(self.constructBaseUrl() + "/api/admin/users", json=self.constructPayload(), verify=False)
      data = resp.json()
      if data['message'] == 'User created':
        self.writeTimestream()
        logger.info("WriteTimestream Initiated")
      return data
    except Exception as e:
      logger.error("Error occurred: %s", str(e))
      return {"error": str(e)}


def lambda_handler(event, context):
  try:
    data = json.loads(event['body'])
    uname = data['username']
    emailid = data['emailid']
    pword = data['password']
  except KeyError as e:
    return {
      'statusCode': 400,
      'body': json.dumps({"error": f"Missing required field: {str(e)}"}),
      'headers': {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST',
        'Access-Control-Allow-Headers': 'Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With'
      },
    }
  userCreation = UserCreation(uname, emailid, pword)
  result = userCreation.createUser()

  return {
    'statusCode': 200,
    'body': json.dumps(result),
    'headers': {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST',
      'Access-Control-Allow-Headers': 'Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With'
    },
  }
