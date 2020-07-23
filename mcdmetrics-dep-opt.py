''' importing libraries '''
import json
import pandas as pd
import pymysql
import boto3
import io


def tot_dep(df_dep): 
'''
	      function to compute total deployments
param:    receives deployments data frame
return:   total deployments
'''
    sum_def=df_dep['defect'].sum()
    sum_fail=df_dep['failure'].sum()
    sum_succ=df_dep['success'].sum()
    out=sum_def+sum_fail+sum_succ
    return out 

	
def per_failed_dep(df_dep):
'''
            function to find percentage of failed deployments
param:      , receives deployments data frame
return:     unintended deployments
'''
    sum_def=df_dep['defect'].sum()
    sum_fail=df_dep['failure'].sum()
    sum_succ=df_dep['success'].sum()
    d=sum_def+sum_fail+sum_succ
    #deals with denominator =0
	try:
		out=float("{:.2f}".format((sum_fail/d)*100))
        return out
	except:
		print('error because denominator=0, so returning 0.0')
		return 0.0

		
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
	
	
def exec_queries(q_dep,cursor):
'''         function to execute passed queries
param:       1)queries to be executed
			 2)cursor to RDS
'''	try:
		for insert_query in q_dep:
				print(insert_query)
				cursor.execute(insert_query)
				connection.commit()
	except:
		print("error in execution of queries")

		
def query_deployment_insertion(df_dep,instance_list):
'''
             function for getting values of deployments table
param:       param1: deployments data frame 
             param2: instance_list; a list containing information about instance_id and its attributes
return:		 queries: a list of queries to be executed to insert data into RDS

'''

    queries=[] #a list to queries to be filled by following loop
    for x in instance_list:
        q_created="insert into deployments (instance_id, total_deployments,impacted_deployments) select "
		
        squad_name,sprint_label,epic_name=x[1],x[2],x[3]
		
        #get data frame for respective instance_id
        df_for_x=df_dep[(df_dep['squad_name']==squad_name) & (df_dep['timebox']==sprint_label) & (df_dep['epic']==epic_name)].copy()
        
		#compute metrics for each instance_id one by one 
        total_deployments=tot_dep(df_for_x)
        impacted_deployments=per_failed_dep(df_for_x)
		
        q_created=q_created+str(x[0])+" as instance_id, "+str(total_deployments)+" as total_deployments, "+\
		str(impacted_deployments)+" as impacted_deployments "
		
        q_created=q_created + "where not exists (select * from deployments where instance_id="+str(x[0])+");"
        queries.append(q_created)
    return queries
	
	
def lambda_handler(event, context):
    if event:
	#Getting file names sent on sns topic
        msg = json.loads(event["Records"][0]["Sns"]["Message"])
        df_file_name = msg["responsePayload"]["body"]["df_dep"]
		
	#creating a boto3 resource 
        s3_buck = boto3.resource("s3")
        bucket_name = "mcdmetrics"
	
	#loading the required files (.csv files)
        print("Loading file now")
        content_object = s3_buck.Object(bucket_name, df_file_name)
        df_dep = pd.read_csv(io.BytesIO(content_object.get()['Body'].read()))
        
        #creating connection with db
        cursor=rds_connect('mcdmetrics','test.cck9zdmlmzli.us-east-1.rds.amazonaws.com',\
		'mcdmetrics','mcdmetrics1@#$%',port=3306,60,{'ssl' : {'ca': './rds-combined-ca-bundle.pem'}})
        
		'''getting instance_list from data frame
			using squad_name,sprint_label(timebox),epic, etc.'''
        instance_list=[]
        #traverse over input jiradata to extract all possible epics and their relevant information
        for _,row in df_dep.iterrows():
			list_to_check=[row['instance_id'],row['squad_name'],row['timebox'],\
			row['epic'],row['manager'],row['product_group'],row['product_name']]
		
            if (list_to_check not in instance_list):
                instance_list.append(list_to_check)

        #call the function to get queries
        q_dep=query_deployment_insertion(df_dep,instance_list)
		#executing queries 
		exec_queries(q_dep,cursor)
		#just a check to ensure data insertion was successful
        cursor.execute("select count(instance_id) from deployments")
        rows=cursor.fetchall()
        print(rows)
