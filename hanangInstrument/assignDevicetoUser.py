import json
import os
import time
import logging
import boto3
from botocore.config import Config
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class UserDeviceMapper:
    def __init__(self, user, device):
        self.user = user
        self.device = device
        self.AWS_ACCESS_KEY = os.environ['ACCESS_KEY']
        self.AWS_SECRET_KEY = os.environ['SECRET_KEY']
        self.AWS_TIMESTREAM_DB = os.environ['AWS_TIMESTREAM_DB']
        self.AWS_TABLE_NAME = os.environ['AWS_TABLE_NAME']
        self.region_name = 'eu-west-1'

    def prepare_common_attributes(self):
        common_attributes = {
            'Dimensions': [
                {'Name': 'username', 'Value': self.user},
            ],
            'MeasureName': 'user',
            'MeasureValueType': 'MULTI'
        }
        print(common_attributes)
        return common_attributes

    def prepare_records(self, timestamp):
        version = int(round(time.time() * 1000))
        record = {
            'Time': str(timestamp),
            'MeasureValues': [],
            'Version': version
        }
        return record

    def prepare_measure(self, measure_name, measure_value):
        measure = {
            'Name': measure_name,
            'Value': str(measure_value),
            'Type': 'VARCHAR',
        }
        return measure

    def prepare_record(self, timestamp):
        records = []
        record = self.prepare_records(timestamp)
        record['MeasureValues'].append(self.prepare_measure('devicename', self.device))
        record['MeasureValues'].append(self.prepare_measure('status', 'assigned'))
        records.append(record)
        return records

    def prepare_recordUnAssign(self, timestamp):
        records = []
        record = self.prepare_records(timestamp)
        record['MeasureValues'].append(self.prepare_measure('devicename', self.device))
        record['MeasureValues'].append(self.prepare_measure('status', 'unassigned'))
        records.append(record)
        return records

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

    def query_timestream_client(self):
        try:
            query_client = boto3.client(
                'timestream-query',
                aws_access_key_id=self.AWS_ACCESS_KEY,
                aws_secret_access_key=self.AWS_SECRET_KEY,
                config=Config(region_name='eu-west-1'))
            logger.info("Connection Established Succesfully")
            return query_client
        except Exception as e:
            logger.error('Error occured during : %s', str(e))
            return None

    def convert_to_timestamp(self, timestamp):
        timestamp = timestamp[:-3]
        formatted_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
        epoch = formatted_time.timestamp()
        time_milliseconds = int(epoch * 1000)
        return time_milliseconds

    def get_time_from_timestream(self):
        try:
            query_client = self.query_timestream_client()
            paginator = query_client.get_paginator("query")
            query = f""" SELECT time FROM "{self.AWS_TIMESTREAM_DB}"."{self.AWS_TABLE_NAME}" WHERE username = '{self.user}' LIMIT 1 """
            pag_response = paginator.paginate(QueryString=query)
            time_str = ""
            for page in pag_response:
                column_info = page['Rows']
                for row in column_info:
                    ans = row['Data']
                    time_str = ans[0]['ScalarValue']
            time_milliseconds = self.convert_to_timestamp(time_str)
            return time_milliseconds
        except Exception as e:
            logger.error('Error occured during : %s', str(e))
            return None

    def writeTimestream(self):
        try:
            time_milliseconds = self.get_time_from_timestream()
            write_client = self.write_timestream_client()
            response = write_client.write_records(DatabaseName=self.AWS_TIMESTREAM_DB,
                                                  TableName=self.AWS_TABLE_NAME,
                                                  Records=self.prepare_record(time_milliseconds),
                                                  CommonAttributes=self.prepare_common_attributes())
            status = response['ResponseMetadata']['HTTPStatusCode']
            return status
        except write_client.exceptions.RejectedRecordsException as err:
            logger.error(err.response)

    def writeTimestreamUnAssign(self):
        try:
            time_milliseconds = self.get_time_from_timestream()
            write_client = self.write_timestream_client()
            response = write_client.write_records(DatabaseName=self.AWS_TIMESTREAM_DB,
                                                  TableName=self.AWS_TABLE_NAME,
                                                  Records=self.prepare_recordUnAssign(time_milliseconds),
                                                  CommonAttributes=self.prepare_common_attributes())
            status = response['ResponseMetadata']['HTTPStatusCode']
            return status
        except write_client.exceptions.RejectedRecordsException as err:
            logger.error(err.response)

def lambda_handler(event, context):
    data = json.loads(event['body'])
    user = data['username']
    device = data['devicname']
    status = data['status']
    u1 = UserDeviceMapper(user, device)
    if status == 'assign':
        result = u1.writeTimestream()
    else:
        result = u1.writeTimestreamUnAssign()

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
