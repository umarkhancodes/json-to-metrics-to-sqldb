''' importing libraries '''
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
       
     def openornot(d):
	 '''function to deal with json files i.e to convert them to pandas dataframe
by getting last level keys and their respective values''' 
          if(type(d)==dict):
              for key in d:
                  if((type(d[key])==dict)or(type(d[key])==list)):
                      openornot(d[key])  
                  else:
                      keys.append(key)
                      values.append(d[key])
     #creating required connections with s3                 
     s3=boto3.client('s3')
     s3_buck = boto3.resource('s3')
     if event: #currently the event is set upon a file upload in s3 bucket
         files_read=[]
         file_obj=event['Records'][0]
         file_name=str(file_obj['s3']['object']['key'])
         #print(file_name)
         fordates=file_name.split('/')
         file_name2=fordates[1]
         #print(file_name2)
         dates=file_name2.split('_') # the name of the file is splitted by _ to get access to dates 
         if(dates[0]=="deployments" or dates[0]=="jiradata"):#to check that the file under consideration is indeed jiradata or deployments
             #print(dates)
             cd=dates[1].split("-")#current date 
             sd=dates[2].split("-")#stat date
             ed=dates[3].split(".")[0].split("-")#end date
             cd_f=datetime(int(cd[0]),int(cd[1]),int(cd[2]))#current date of file under consideration
             sd_f=datetime(int(sd[0]),int(sd[1]),int(sd[2]))#start  date of file under consideration
             ed_f=datetime(int(ed[0]),int(ed[1]),int(ed[2]))#end  date of file under consideration
             if(cd_f==ed_f):#if the current date = end date then sprint has ended and we pick up files 
                 values_jira=[]
                 values_dep=[]
                 keys_jira=[]
                 keys_dep=[]
                 print('pick all files between start and end date')
                 bucket = s3_buck.Bucket('mcdmetrics')
                 for obj in bucket.objects.filter(Prefix='sprint_wise_uk_test'):#traversing all files in s3 bucket
                     c_fname=obj.key
                     c_fname2=c_fname.split('/')
                     c_dates=c_fname2[1].split('_')
                     #print(c_fname2[1])
                     if(c_dates[0]=="deployments" or c_dates[0]=="jiradata"):#to check that the file under consideration is indeed jiradata or deployments
                         c_cd=c_dates[1].split("-")
                         c_sd=c_dates[2].split("-")
                         c_ed=c_dates[3].split(".")[0].split("-")
                         cd_c=datetime(int(c_cd[0]),int(c_cd[1]),int(c_cd[2]))#current date of file to check 
                         sd_c=datetime(int(c_sd[0]),int(c_sd[1]),int(c_sd[2]))#start date of file to check  
                         ed_c=datetime(int(c_ed[0]),int(c_ed[1]),int(c_ed[2]))#end date of file to check 
                         
                         if(sd_c>=sd_f and ed_c<=ed_f):#if file is of the sprint under consideration
                         
                         #read the file and convert in json and then pass to openornot function to get keys and values
                             #print("read this ",c_fname)
                             files_read.append(c_fname)
                             content_object=s3_buck.Object('mcdmetrics',c_fname)
                             file_content = content_object.get()['Body'].read().decode('utf-8')
                             json_content = json.loads(file_content)
                             if(c_dates[0]=="deployments"):#if file is deployments
                                 for x in json_content:
                                     keys=[]
                                     values=[]
                                     openornot(json_content[x])
                                     values_dep.append(values)
                                     keys_dep=keys
                             elif(c_dates[0]=="jiradata"):#if file is jiradata
                                 for x in json_content:
                                     keys=[]
                                     values=[]
                                     openornot(json_content[x])
                                     values_jira.append(values)
                                     keys_jira=keys
                         else:
                             pass
                     else:
                         pass
				''' check if values are more than 1 
				then write the list as a json to s3 bucket and pass the name to sns'''
                 if(len(values_jira)>1):
                     n1='testing_uk_out/values-jira_'+c_dates[3]
                     n3='testing_uk_out/keys-jira_'+c_dates[3]
                     print(n1,n3)
                     
                 if(len(values_dep)>1): 
                     n2='testing_uk_out/values-dep_'+c_dates[3]
                     n4='testing_uk_out/keys-dep_'+c_dates[3]
                     print(n2,n4)
                 s3_buck.Object('mcdmetrics',n1).put(Body=json.dumps(values_jira))
                 s3_buck.Object('mcdmetrics', n2).put(Body=json.dumps(values_dep))
                 s3_buck.Object('mcdmetrics', n3).put(Body=json.dumps(keys_jira))
                 s3_buck.Object('mcdmetrics', n4).put(Body=json.dumps(keys_dep))
                 return {
                    'statusCode': 200,
                    'body': {'n1':n1,'n2':n2,'n3':n3,'n4':n4 }
                    }
                             
             else:
                 print('current date is not equal to end date')
		#to check if right files were read 
         print(files_read)             