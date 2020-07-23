''' importing libraries '''
import json
import pandas as pd
import pymysql
import boto3
import io


def defect_count(jd): 
'''
              function for defect count 
:param        param1: (jiradata)data frame representing a particuler instance id
:return       defect_count
'''
    jd_snag=jd[jd.snag==1.0] #get all rows where snag=1
    return jd_snag['snag'].sum() #sum of snag is returned

	
def defect_density(jd):
'''
              function for defect density
:param        param1: (jiradata)data frame representing a particuler instance id
:return       defect_density

'''
    jd_snag=jd[jd.snag==1.0]#get all rows where snag=1
    numerator=jd_snag['snag'].sum()#count
    status_list=['In IPT', 'Ready for IPT','Converted to S-IN STG', 'Ready for QA']
    jd_status=jd[jd.status.isin(status_list)]#get all rows where status is in list status_list 
    denomenator=jd_status['points'].sum()#sum points for jd_status
    #deal with denominator=0
	try:
		out=numerator/denomenator
		return out
	except:
		print("denomenator is 0, thus returning 0.0")
		return 0.0
  
  
def query_quality_insertion(df_jira,instance_list):
'''
             function for getting values of quality table
param:       param1: jiradata data frame 
             param2: instance_list; a list containing information about instance_id and its attributes
return:		 queries: a list of queries to be executed to insert data into RDS

'''


    #and a cursor to a db connection is also required as input
    queries=[]#a list to queries to be filled by following loop
    for x in instance_list:
        squad_name,sprint_label,epic_name=x[1],x[2],x[3]
		
        q_created="insert into Quality (instance_id, defect_count,defect_density) select "
		
        #get data frame for respective instance_id
        df_for_x=df_jira[(df_jira['squad_name']==squad_name) & (df_jira['timebox']==sprint_label) & (df_jira['epic']==epic_name)].copy()
        
		#compute metrics for each instance_id one by one 
        def_cnt=defect_count(df_for_x)
        def_den=defect_density(df_for_x)
        
		#create query
        q_created=q_created+str(x[0])+" as instance_id, "+str(def_cnt)+" as defect_count, "+str(def_den)+" as defect_density "
        q_created=q_created + "where not exists (select * from Quality where instance_id="+str(x[0])+");"
        
		queries.append(q_created)

    return queries #returns list of queries to execute
	
	
def rds_connect(in_database,in_host,in_user,in_password,in_port,in_connect_timeout,in_ssl):
'''
            function to establish RDS connection
param:      in_database,in_host,in_user,in_password,in_port,in_connect_timeout,in_ssl
return:     cursor
'''

	connection = pymysql.connect(database=in_database,\
		host=in_host,\
		user=in_user, password=in_password,port=in_port, \
		connect_timeout=in_connect_timeout, ssl =in_ssl )
	cursor=connection.cursor()
	return cursor
	
	
def exec_queries(q_quality,cursor):
'''         function to execute passed queries
param:       1)queries to be executed
			 2)cursor to RDS
'''	try:
		for insert_query in q_quality:
				print(insert_query)
				cursor.execute(insert_query)
				connection.commit()
	except:
		print("error in execution of queries")
	

def lambda_handler(event, context):
    if event:
	#Getting file names sent on sns topic
        msg = json.loads(event["Records"][0]["Sns"]["Message"])
        df_file_name = msg["responsePayload"]["body"]["df_jira"]
		
		
	#creating a boto3 resource 
        s3_buck = boto3.resource("s3")
        bucket_name = "mcdmetrics"
		
		
	#loading the required files (.csv files)
        print("Loading file now")
        content_object = s3_buck.Object(bucket_name, df_file_name)
        df_jira = pd.read_csv(io.BytesIO(content_object.get()['Body'].read()))

				
		#creating connection with db
		cursor=rds_connect('mcdmetrics','test.cck9zdmlmzli.us-east-1.rds.amazonaws.com',\
		'mcdmetrics','mcdmetrics1@#$%',port=3306,60,{'ssl' : {'ca': './rds-combined-ca-bundle.pem'}})
		
        '''getting instance_list from data frame
		using squad_name,sprint_label(timebox),epic, etc.'''
        instance_list=[]
        #traverse over input jiradata to extract all possible epics and their relevant information
        for _,row in df_jira.iterrows():
			list_to_check=[row['instance_id'],row['squad_name'],row['timebox'],\
			row['epic'],row['manager'],row['product_group'],row['product_name']]
			
            if (list_to_check not in instance_list):
                instance_list.append(list_to_check)
        #call the function to get queries
        q_quality=query_quality_insertion(df_jira,instance_list)

        print(len(q_quality))
		#executing queries 
		exec_queries(q_quality,cursor)
		#just a check to ensure data insertion was successful
        cursor.execute("select * from Quality")
        rows=cursor.fetchall()
        print(rows)
        
        
       