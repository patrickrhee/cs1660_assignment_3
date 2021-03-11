#!/usr/bin/env python3

import boto3

s3 = boto3.resource('s3', 
	aws_access_key_id='', 
	aws_secret_access_key=''
)

try:
	s3.create_bucket(Bucket='datacont-cs1660-hw3', 
		CreateBucketConfiguration={
		'LocationConstraint': 'us-east-2'}) 
except:
	print("this may already exist")


bucket = s3.Bucket("datacont-cs1660-hw3")

bucket.Acl().put(ACL='public-read')




#upload a new object into the bucket
body = open('/Users/Patrick/Desktop/CS1660 - Intro to Cloud Computing/Assignment 3/exp2.csv', 'rb')

o = s3.Object('datacont-cs1660-hw3', 'exp2.csv').put(Body=body )

s3.Object('datacont-cs1660-hw3', 'exp2.csv').Acl().put(ACL='public-read')





dyndb = boto3.resource('dynamodb', 
	region_name='us-east-2', 
	aws_access_key_id='', 
	aws_secret_access_key=''
)

try:
	table = dyndb.create_table( TableName='DataTable', KeySchema=[
		{
			'AttributeName': 'PartitionKey', 
			'KeyType': 'HASH'
		}, 
		{
			'AttributeName': 'RowKey',
			'KeyType': 'RANGE' 
		}
	], 
	AttributeDefinitions=[
		{
			'AttributeName': 'PartitionKey', 
			'AttributeType': 'S'
		}, 
		{
			'AttributeName': 'RowKey',
			'AttributeType': 'S' 
		},
	], 
	ProvisionedThroughput={
		'ReadCapacityUnits': 5,
		'WriteCapacityUnits': 5 
	}
) 
except:
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable")

#wait for the table to be created. 
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv

with open('experiments.csv', "rt", encoding="utf8") as csvfile: 
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	for item in csvf:
		print(item)
		# Put the objects ('exp1.csv' and 'exp2.csv')
		body = open(item[3], 'rb')
		s3.Object('datacont-cs1660-hw3', item[3]).put(Body=body)
		md = s3.Object('datacont-cs1660-hw3', item[3]).Acl().put(ACL='public-read')
		url = "https://s3-us-east-2.amazonaws.com/datacont-cs1660-hw3/"+item[3] 

		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
			'description' : item[4], 'date' : item[2], 'url':url}
		try: 
			table.put_item(Item=metadata_item)
		except:
			print("item may already be there or another failure")

# Try a adding a custom row with an extra field
metadata_item = {'PartitionKey': 'test', 'RowKey': 'test_row_key', 'description' : 'this is a test insertion with an extra column', 'date' : '03/10/21', 'url':'www.nyan.cat', 'extraColumnTest':'so I can just add columns to specific rows?'}

try: 
	table.put_item(Item=metadata_item)
except:
	print("item may already be there or another failure")


response = table.get_item ( 
	Key={
		'PartitionKey': 'experiment2',
		'RowKey': 'data2' 
	}
)

item = response['Item'] 
print(item)

response
