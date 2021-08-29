# FINDING AVERAGE HOURS AND IDLE HOURS FOR USER FROM CSV FILE

## Start hadoop daemons
$ start-all.sh

## Start spark session
from pyspark.sql import SparkSession appName = "PySpark hive example" master = "local" spark =
SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()

## Load data in df and take selected columns in another df
df = spark.read.csv("hdfs://localhost:9000/Spark/CpuLogData/*.csv",header=True) df2 = df.select("user_name","DateTime","keyboard","mouse")

## Create temp view of created dataframe
df2.createOrReplaceTempView("data")

## load data from this view in hive table,it will crrate table also
spark.sql("create table default.new_data as select * from data")

## establish connection with hive
conn = hive.Connection(host='localhost',port=10000,database='log_data')

## Perform hive queries to get work hrs
df = pd.read_sql("select user_name ,count('') as total from cpu_log where keyboard !=0.0 or mouse!=0.0 group by user_name",conn)

## get lowest avg work hrs
lowest_work_time=pd.read_sql("select user_name ,((((count('')-1)*5)*60)/6) as working_sec 
from cpu_log where keyboard!=0.0 or mouse!=0.0 group by user_name",conn) lowest_work_time = 
lowest_work_time.sort_values('working_sec') logger.info(lowest_work_time)

## Convert time in sec to hr and min
lowest_average_work_hour = lowest_work_time[["user_name","working_sec"]] lowest_average_work_hour["working_sec"]= pd.to_datetime(lowest_average_work_hour['working_sec'] ,unit='s').dt.strftime("%H:%M") lowest_average_work_hour = lowest_average_work_hour.sort_values('working_sec') lowest_average_work_hour.rename(columns={'working_sec': 'working_hour'}, inplace=True) logger.info(lowest_average_work_hour)

## Similarly do for highest hrs and idle time

# Visualization with matplotlib

## Do imports

import matplotlib.pyplot as plt 
import pandas

## give variables to column we want plot
username= highest_work_time['user_name'] work=highest_work_time['working_sec']

## Plot the pie chart
plt.style.use("fivethirtyeight") fig = plt.figure(figsize=(6,12)) fig.patch.set_facecolor('w') 
explode = (0.1, 0, 0, 0, 0, 0, 0, 0) plt.pie(work,labels=username,autopct='%1.1f%%',startangle=140,explode= explode) 
plt.title('Avg Hours By Person',bbox={'facecolor':'1', 'pad':4})

## Similarly for bar chart
df = highest_work_time plt.figure(figsize = (8,5)) sns.barplot(x = 'user_name',y='working_sec',data = df) 
plt.title("Percent Avg Hours By Person") plt.ylim(0,25000) plt.xticks(rotation='vertical') plt.show()
