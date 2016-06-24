#!/usr/bin/env python
# Author: Peter Mueller, CTO, ATS, Inc. www.atso.com
# Credits: Christopher Corus - ATS, Chris Brokes - ATS, Abhijit Chanda - Gamma, Felipe Hoffa - Google, Graham Polley - Shine 
# License: GPL

#  Note: Requires pandas 0.18 or higher : pip install git+https://github.com/pydata/pandas.git 
import argparse
import ast
import json
import uuid
import pandas as pd

# Google service account authentication is tricky.  It took us waaay too long to figure out how service-accounts could work in Lambda.
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
goog_service_account='./<my_service_account_file.json>'  # Get one at https://console.developers.google.com/iam-admin/serviceaccounts/

# Using argument parsing doesn't make a lot of sense in Lambda context, but we'll leave the defaults in anyway.
# It would make sense if you land up using this locally since you could run ./stream_to_bq.py --project_id etc... for testing purposes pre-Lambda.
parser = argparse.ArgumentParser()
parser.add_argument('--project_id', help='Google Cloud project ID.', default='<project_id>')
parser.add_argument('--dataset_id', help='BigQuery dataset ID.', default='<dataset_name>')
parser.add_argument('--table_name', help='Name of the table to load csv file into.', default='<table_name>_'+pd.datetime.now().strftime('%Y%m%d'))
args = parser.parse_args()

fields=['field1', 'field2', 'field3']

fieldtypes={'field1':'int64','field2':'int64','field3':'object'}

# the "event" here is what gets passed from S3-notification.  In this case, all we're interested in is the S3 key name.
# You'll need to make sure your Lambda function runs with a 'role' that allows it to read from your S3 bucket.
# Your Lambda "Handler" should like stream_to_bq.handler, which means that the Lambda 'event' will be passed to our function called 'handler'.
def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        # having isolated bucket and key, we can use pandas 'read_csv' to load it up in a dataframe, then pipe it out to BigQuery's streaming service.
        # Remember, it's your Lambda function trying to read an S3 key, so your Lambda role is important.  Alternately, you could use boto and include your AWS Secret Keys in this file.
        df=pd.read_csv('s3://'+bucket+'/'+key, names=fields, dtype=fieldtypes)
        df.to_gbq(args.dataset_id+'.'+args.table_name, project_id=args.project_id, verbose=1, if_exists='append', private_key=goog_service_account)
        # The next two lines will print to your Lambda CloudWatch logs, which you should be monitoring anyhow...
        # If you skimped on time or RAM, you'll see errors showing up in these logs.  Notably, too, you won't see anything showing up in BQ!
        print "loaded " +str(len(df)) + " records"
        print vars(args)

