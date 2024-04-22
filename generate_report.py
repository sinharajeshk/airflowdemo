import boto3
import os
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    #bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    #s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    
    bucket_name = event["bucket_name"]
    s3_file_name = event["s3_file_name"]
    
    # Query DynamoDB
    table = dynamodb.Table('people')
    response = table.scan()
    items = response['Items']
    
    # Generate HTML
    
    html = "<html><body>"
    html+="<br>"
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_date = datetime.now().strftime('%Y%m%d')
    report_heading ="Report - "+current_date
    html+=f"<h1><b>{report_heading}</b></h1>"
    for item in items:
        html +="<hr>"
        for key, value in item.items():
            html += f"<b>{key}</b>:{value}"
            html += "<br>"
    html +="<hr><br></body></html>"
    
    #response_key = 'output/'+s3_file_name.removesuffix('.txt') + "_" +file_date  + '.html'
    response_key = 'output/result_' +file_date  + '.html'
    print(response_key)
    print(bucket_name)
    s3 = boto3.resource('s3')
    obj_exists = list(s3.Bucket(bucket_name).objects.filter(Prefix=response_key))
    if len(obj_exists) > 0 and obj_exists[0].key == response_key:
        s3.Object(bucket_name, response_key).delete()
    
    s3_client.put_object(Body=html, Bucket=bucket_name, Key=response_key)
    
    return {
        'statusCode': 200,
        'body': 'HTML page created!'
    }