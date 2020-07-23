''' importing libraries '''
import json
import boto3
import pandas as pd
import os

def lambda_handler(event, context):
    if (event):
        print("event occured")
        bucketname=os.environ['Bucket']
			 '''function to deal with json files i.e to convert them to pandas dataframe
by getting last level keys and their respective values''' 
        def openornot(d):
            if(type(d)==dict):
                for key in d:
                    if((type(d[key])==dict)or(type(d[key])==list)):
                        openornot(d[key])  
                    else:
                        keys.append(key)
                        values.append(d[key])
		#creating required connections with s3 				
        s3_buck = boto3.resource('s3')
        bucket = s3_buck.Bucket(bucketname)
        values_jira=[]
        values_dep=[]
        '''get all past data from s3 with a specific prefix representing deployments file'''
        for obj in bucket.objects.filter(Prefix=os.environ['fordep']):
            name=obj.key
            content_object=s3_buck.Object(bucketname,name)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            data_dep= json.loads(file_content)
            for x in data_dep:
                keys=[]
                values=[]
                openornot(data_dep[x])
                values_dep.append(values)
                keys_dep=keys
        '''get all past data from s3 with a specific prefix representing jiradata file'''    
        for obj in bucket.objects.filter(Prefix=os.environ['forjira']):
            name=obj.key
            content_object=s3_buck.Object(bucketname,name)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            data_jira= json.loads(file_content)
            for x in data_jira:
                keys=[]
                values=[]
                openornot(data_jira[x])
                values_jira.append(values)
                keys_jira=keys  

'''check if length of values is > 1 , 
if yes then write the lists to s3 in the form of json
and pass the name to sns '''				
        if(len(values_jira)>0):
             n1,n3='testing_uk_out/values_jira.json','testing_uk_out/keys_jira.json'
             s3_buck.Object(bucketname,n1).put(Body=json.dumps(values_jira))
             s3_buck.Object(bucketname, n3).put(Body=json.dumps(keys_jira))
        if(len(values_dep)>0):
            n2,n4='testing_uk_out/values_dep.json','testing_uk_out/keys_dep.json'
            s3_buck.Object(bucketname, n2).put(Body=json.dumps(values_dep))
            s3_buck.Object(bucketname, n4).put(Body=json.dumps(keys_dep))
            
        # s3_buck.Object(bucketname,n1).put(Body=json.dumps(values_jira))
        # s3_buck.Object(bucketname, n2).put(Body=json.dumps(values_dep))
        # s3_buck.Object(bucketname, n3).put(Body=json.dumps(keys_jira))
        # s3_buck.Object(bucketname, n4).put(Body=json.dumps(keys_dep))
        return {
            'statusCode': 200,
            'body': {'n1':n1,'n2':n2,'n3':n3,'n4':n4 }
            }
            
