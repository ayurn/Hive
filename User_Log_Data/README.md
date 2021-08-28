# FINDING AVERAGE HOURS AND IDLE HOURS FOR USERS

## Start hadoop daemons
$ start-all.sh

## establish connection with hive
conn = hive.Connection(host=host_name, port=port, username=user,database=database,password =password, auth='CUSTOM')

## Load csv files in hive table
cur.execute("create external table logs(DateTime date,c1 string,c2 string,c3 string,c4 string,c5 string,c6 string,c7 string,c8 string,c9 string,c10 string,c11 string,c12 string,c13 string,c14 string,c15 string,c16 string,c17 string,c18 string,c19 string,c20 string,c21 string,c22 string,c23 string,c24 string,c25 string,c26 string,c27 string,c28 string,c29 string,c30 string,c31 string,c32 string,c33 string,c34 string,c35 string,c36 string,c37 string,c38 string,c39 string,user_name string,keyboard string,mouse string,c44 string,c45 string) row format delimited fields terminated by ',' stored as textfile location 'hdfs://localhost:9000/sparkData/CpuLogs' tblproperties('skip.header.line.count'='1')")

## Perform hive queries to get work hrs
df = pd.read_sql("select user_name ,count('') as total from cpu_log where keyboard !=0.0 or mouse!=0.0 group by user_name",conn)

## Get lowest avg work hrs
lowest_sec=pd.read_sql("select user_name ,((((count('')-1)*5)*60)/6) as working_sec from logs where keyboard!=0.0 or mouse!=0.0 group by user_name",conn)
lowest_average_hour = lowest_sec.sort_values('working_sec')
print(lowest_average_hour)

## Convert seconds to hr and min

lowest_avg_work_hour = lowest_sec[["user_name","working_sec"]]
lowest_avg_work_hour["working_sec"]= pd.to_datetime(lowest_avg_work_hour['working_sec'] ,unit='s').dt.strftime("%H:%M")
lowest_avg_work_hour = lowest_avg_work_hour.sort_values('working_sec')
lowest_avg_work_hour.rename(columns={'working_sec': 'lowest_hour'}, inplace=True)
print(lowest_avg_work_hour)

## Similarly we will do for highest hrs and idle time

## Visualization with matplotlib
import all the libraries 

import matplotlib.pyplot as plt import pandas
give variables to column we want plot

username= highest_work_time['user_name'] work=highest_work_time['working_sec']
Plot the pie chart

plt.style.use("fivethirtyeight") fig = plt.figure(figsize=(6,12)) fig.patch.set_facecolor('w') explode = (0.1, 0, 0, 0, 0, 0, 0, 0) plt.pie(work,labels=username,autopct='%1.1f%%',startangle=140,explode= explode) plt.title('Avg Hours By Person',bbox={'facecolor':'1', 'pad':4})
Similarly for bar chart

df = highest_work_time plt.figure(figsize = (8,5)) sns.barplot(x = 'user_name',y='working_sec',data = df) plt.title("Percent Avg Hours By Person") plt.ylim(0,25000) plt.xticks(rotation='vertical') plt.show()