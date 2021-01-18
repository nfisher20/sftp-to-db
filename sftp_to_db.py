
import paramiko
import datetime
import pandas as pd
import numpy as np
import sqlalchemy

import settings


hostkey=None

t = paramiko.Transport((settings.myHostname, settings.myPort))
t.connect(username=settings.myUsername, password=settings.myPassword, hostkey=hostkey)
sftp = paramiko.SFTPClient.from_transport(t)

# get directory list
dirlist = sftp.listdir_iter('.')

today = datetime.datetime.now().date()

#open file to be inserted into database
fopen = sftp.open(settings.sftppath,'r')

#read lines of txt file to list
txtfile = fopen.readlines()

#close connections
t.close()
sftp.close()

#filter txtfile to remove lines that start with 1 as their first index
filteredtxtfile = []
for line in txtfile:
    if line[0] == '1':
        filteredtxtfile.append(line)

#dictionary of headers and indexes as .csv did not have delimeters
carrierrecord = {'RECORDID' : [0,1],
'PROCESSORNO' : [1,11],
'BATCHNO' : [11,18],
'PROCESSORNAME' : [18,43],
'PROCESSORADDRESS' : [43,63],
'PROCESSORCITY' : [63,81],
'PROCESSORSTATE' : [81,83],
'PROCESSORZIP' : [83,88],
'PROCESSORPHONE' : [88,98],
'CREATIONDATE' : [98,106],
'CREATIONMM' : [98,100],
'CREATIONDD' : [100,102],
'CREATIONCC' : [102,104],
'CREATIONYY' : [104,106],
'ZIPEXPAND' : [106,110],
'DESTCUSTOMER' : [110,125],
'DESTPROCNO' : [125,135],
'FILLER2' : [135,5000]
}

#assign headers to columns based on indexing and store in dict
dict2 = {}
for line in filteredtxtfile:
    for key,value in carrierrecord.items():
    #print(key,value[0]) i[value[0]:value[1]]
        dict2.setdefault(key,[]).append(line[value[0]:value[1]])

#convert dictionary to dataframe
df = pd.DataFrame.from_dict(dict2,dtype=str)

#process phone numbers
df['PROCESSORPHONE']=df['PROCESSORPHONE'].astype(str).apply(lambda x: np.where((len(x)>=10)&set(list(x)).issubset(list('.0123456789')),
                                                                      '('+x[:3]+')'+x[3:6]+'-'+x[6:10],
                                                                      'Phone number not in record'))
#parse date
df['CREATIONDATE'] = pd.to_datetime(df['CREATIONDATE'],format='%m%d%Y', errors='ignore')


sql = """
SELECT PROCESSORNAME
FROM CarrierRecord
"""

engine = sqlalchemy.create_engine('mssql+pyodbc://@' + settings.servername + '/' + settings.databasename + '?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server')

sqltable = pd.read_sql_query(sql, engine)

processornames = sqltable.PROCESSORNAME.unique()

#drop duplicate processor names
for processor in processornames:
    for index, row in df.iterrows():
        if processor == df.PROCESSORNAME[index]:
                df = df.drop([index])      

#write dataframe to database
df.to_sql('CarrierRecord',engine,if_exists = 'append',index=False)
