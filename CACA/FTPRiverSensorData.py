import json
import pandas as pd
import boto3
import datetime
import ftplib
from botocore.client import Config
import os
import logging

AWS_ACCESS_KEY = os.environ['ACCESS_KEY']
AWS_SECRET_KEY = os.environ['SECRET_KEY']
AWS_TIMESTREAM_DB = os.environ['AWS_TIMESTREAM_DB']
AWS_TABLE_NAME = os.environ['AWS_TABLE_NAME']
IP_ADDRESS = os.environ['IP_ADDRESS']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_timestream_client():
    try:
        write_client = boto3.client(
            'timestream-write',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            config=Config(region_name='eu-west-1', read_timeout=20, max_pool_connections=5000,
                          retries={'max_attempts': 10})
        )
        return write_client
    except Exception as e:
        logger.error('Error occured during : %s', str(e))
        return None


def write_records(records,common_attributes):
    try:
        write_client = write_timestream_client()
        result = write_client.write_records(DatabaseName=AWS_TIMESTREAM_DB,
                                            TableName=AWS_TABLE_NAME,
                                            Records=records,
                                            CommonAttributes=common_attributes)
        status = result['ResponseMetadata']['HTTPStatusCode']
        logger.info("Processed %d records. WriteRecords Status: %s" % (len(records), status))
        logger.info("WriteRecords Result:", status)
        return status
    except write_client.exceptions.RejectedRecordsException as e:
        logger.error("Error in write_records: %s" % e.response)


def prepare_common_attributes(dimensions):
    common_attributes = {
        'Dimensions': dimensions,
        'MeasureName': 'riversensor',
        'MeasureValueType': 'MULTI'
    }
    return common_attributes

def prepare_record(time):
    record = {
        'Time': str(time),
        'MeasureValues': []
    }
    return record

def prepare_measure(measure_name, measure_value):
    measure = {
        'Name': measure_name,
        'Value': str(measure_value),
        'Type': 'DOUBLE'
    }
    return measure


def construct_timestream(df, file_name):
    device_name = file_name.split('_')[0]
    records = []
    response = None  # Initialize response variable
    if device_name.startswith('UCL'):
        for _, row in df.iterrows():
            timestamp = datetime.datetime.strptime(row[df.columns[1]], '%d.%m.%Y %H:%M:%S')
            dimensions = [
                    {'Name': 'deviceName', 'Value': device_name},
                    {'Name': 'DatumZeit', 'Value': str(row['Datum Zeit'])},
                    {'Name': 'CH01[m]', 'Value': str(row['CH01[m]'])}
                ]
            value = str(row['CH32[V]'])
            common_attributes = prepare_common_attributes(dimensions)
            timestamp = str(int(timestamp.timestamp() * 1000))
            record = prepare_record(timestamp)
            record['MeasureValues'].append(prepare_measure('CH32[V]', value))
            records.append(record)
    elif device_name.startswith('MCCA'):
        for index, row in df.iterrows():
            try:
                timestamp = datetime.datetime.strptime(row['date/time'], "%m/%d/%Y %I:%M %p")
                value = str(row['32 Power'])
            except KeyError:
                continue
            dimensions = [
                    {'Name': 'deviceName', 'Value': device_name},
                    {'Name': '01Rain', 'Value': str(row['01 Rain'])},
                    {'Name': '02Rainintensitz', 'Value': str(row['02 Rain intensitz'])},
                    {'Name': '03Totalrain24h', 'Value': str(row['03 Total rain 24h'])},
                    {'Name': '04Totalrain10m', 'Value': str(row['04 Total rain 10m'])},
                    {'Name': '05Totalrain5m', 'Value': str(row['05 Total rain 5m'])}
                ]
            common_attributes = prepare_common_attributes(dimensions)
            timestamp = str(int(timestamp.timestamp() * 1000))
            record = prepare_record(timestamp)
            record['MeasureValues'].append(prepare_measure('32Power', value))
            records.append(record)
    response = write_records(records,common_attributes)
    
    return response


def ftp_connection():
    try:
        ftp = ftplib.FTP(IP_ADDRESS)
        ftp.login(USERNAME, PASSWORD)
        ftp.cwd('/ftp/rs')
        files = ftp.nlst()
        logger.info("FTP server successfully connected")
        for file in files:
            ftp.retrbinary("RETR " + file, open("/tmp/" + file, 'wb').write)
            device_partition = file.split('_')[0]
            if device_partition.startswith('MCCA'):
                df = pd.read_csv('/tmp/' + file, encoding='latin1')
            elif device_partition.startswith('UCL'):
                df = pd.read_csv('/tmp/' + file, encoding='latin1', sep=';')
            df = df.fillna(0)
            df = df.reset_index(drop=True)
            construct_timestream(df, file)
    except Exception as e:
        logger.error("Error connecting to FTP: {}".format(str(e)))


def lambda_handler(event,context):
    response = ftp_connection()
    return {
        'statusCode': '200',
        'body' : response
        }
