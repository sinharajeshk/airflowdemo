import json
import boto3
import datetime
import uuid
import random
import string

s3_client = boto3.client('s3')
lambda_client=boto3.client('lambda')
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table('people')

def lambda_handler(event, context):
    # TODO implement
    try:
        timestamp=int(datetime.datetime.utcnow().timestamp())
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_name)
        response=s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
        data=response["Body"].read().decode('utf_8')
        peoples= data.split("\n")
        peoples.pop(0)
        rowcount=0
        for people in peoples:
            if not people.strip():  # Check if the line is empty
                continue  # Skip this iteration and move to the next line
            rowcount +=1
            #print(rowcount)
            people=people.split(",")
            firstName = str(people[2])
            lastName = str(people[3])
            table.put_item(
                Item={
                    "UserId": str(uuid.uuid4()),
                    "UserName": (firstName[0:1]+lastName[0:3]).lower(),
                    "FirstName": firstName,
                    "LastName": lastName,
                    "Gender": str(people[4]),
                    "Email": str(people[5]),
                    "Phone": str(people[6]),
                    "Dateofbirth": str(people[7]),
                    "JobTitle": str(people[8]),
                    "Password": ''.join(random.choices(string.ascii_uppercase + string.digits, k=8)),
                    "timestamp": str(timestamp)
                }
                )
            
        response_content='Records Processed:' + str(rowcount)
        response_key = 'output/'+s3_file_name.removesuffix('.csv') + '.dat'
        s3_client.put_object(Body=response_content, Bucket=bucket_name, Key=response_key)
        
        
        
        input = {
            "bucket_name":bucket_name,
            "s3_file_name" : s3_file_name
        }
        print(input)
        response = lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-east-1:381492116045:function:generate_report",
            InvocationType="RequestResponse",
            Payload=json.dumps(input)
            )
        responsePayload=json.load(response["Payload"])
        print(responsePayload)
        
    except Exception as err:
        print(err)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
