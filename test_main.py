import os
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = r"C:\Flask\s3DataSampler\creds\aws_credentials.csv"
from s3Synergy import S3Synergy

args={
    'key': r's3://s3synergy/parquet/emp_data.snappy.parquet',
    'format': 'parquet',
    'query': "select count(*) from s3object where city='mumbai'"
}
# self, key: str,
#                  format: str,
#                  header_flag: str = 'Y',
#                  headers: str = None,
#                  sep: str = ',',
#                  query: str = '',
#                  lines: int=0,
#                  compression: str = '',
#                  quoteChar: str
args_csv={
    'key': r's3://s3synergy/csv/emp_data.csv.bz2',
    'format': 'csv',
    'query': "select `city` from s3object where `city`='mumbai' limit 5",
    'compression': 'bzip2',
    'header_flag':'N',
    'headers':'id,name,dept,salary,city,country,remarks'
}
# a,b,c=S3Synergy().readData(**args)
# print(a,b,c)
a,b,c=S3Synergy().readData(**args_csv)
print(a,b,c)
# import boto3
# s3=boto3.client('s3')
# response = s3.select_object_content(
#         Bucket = 's3synergy',
#         Key = 'parquet/emp_data.parquet',
#         ExpressionType = 'SQL',
#         Expression = "select * from s3object where city='mumbai'",
#         InputSerialization = {'Parquet':{}},
#         OutputSerialization = {'JSON': {'RecordDelimiter': ','}},
#         )
# for event in response['Payload']:
#     print(event)
#     if 'Records' in event:
#         records = event['Records']['Payload'].decode('utf-8')

# import pandas as pd 
# from io import StringIO
# data='''{"id":"1","name":"a","dept":"IT","salary":"10000","city":"mumbai","country":"india"}
# {"id":"2","name":"b","dept":"HR","salary":"100000","city":"pune","country":"india","remarks":"Work\\nWork\\nWork"}
# {"id":"3","name":"c","dept":"Finance","salary":"100000","city":"Hyd","country":"india","remarks":"Work,Money"}
# {"id":"4","name":"d","dept":"Admin","salary":"100000","city":"Delhi","country":"india","remarks":"Work,\\nFalicitate"}
# {"id":"1","name":"a","dept":"IT","salary":"10000","city":"mumbai","country":"india"}'''

# print('['+records+']')
# df=pd.read_csv(StringIO(records))
# df
# import json
# json.loads('['+records.strip(',')+']')
# df=pd.DataFrame(json.loads('['+records[:-1]+']'))