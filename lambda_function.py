# AWS Lambda Function

import json
import boto3

#  function to query data in CSV in Amazon S3
def query_csv_s3(s3, bucket_name, filename, sql_exp, use_header):

    #  should we search by column name or column index
    if use_header:
        header = "Use"
    else:
        header = "None"
        
    #  query and create response
    resp = s3.select_object_content(
        Bucket=bucket_name,
        Key=filename,
        ExpressionType='SQL',
        Expression=sql_exp,
        InputSerialization = {'CSV': {"FileHeaderInfo": header}},
        OutputSerialization = {'CSV': {}},
    )
    
    #  upack query response
    records = []
    for event in resp['Payload']:
        if 'Records' in event:
            records.append(event['Records']['Payload'])  
        
    #  store unpacked data as a CSV format
    file_str = ''.join(req.decode('utf-8') for req in records)
    
    return file_str
    
    

#  start client with s3
s3 = boto3.client('s3', region_name='ap-south-1')

#  define file location and name
bucket_name = 'india-mart'
filename = 'dummyCSV.csv'

#  create SQL expression to query by date using column names
#sql_exp = ("SELECT s.\"PC_ITEM_ID\", s.\"Type\", s.\"CITY\"" 
 #         "FROM s3object s " )
          #"WHERE s.\"Date/Time\" BETWEEN '2012-06-20 12:00' AND '2012-06-20 16:00'")

#  should we use header names to filter
use_header = True
def lambda_handler(event, context):
    #  return CSV of unpacked data
    # print(event['queryStringParameters']['sql'])
    sql_exp = (str(event['queryStringParameters']['sql']))
    # print(sql_exp)
    file_str = query_csv_s3(s3, bucket_name, filename, sql_exp, use_header)

    #print(file_str)
    #  read CSV to dataframe
    #df = pd.read_csv(StringIO(file_str), names=['Date/Time', 'Wind Speed', 'Wind Direction'])
    return file_str