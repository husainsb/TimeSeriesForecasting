import numpy as np
import pandas as pd
import datetime
from sklearn import metrics

import mysql.connector
import requests
from requests.structures import CaseInsensitiveDict
import json
import yaml 

from .defined_variables import *

with open("/home/cdsw/art_forecasting/appdynamics-config.yml", "r") as stream:
    try:
        config_dict = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        
#Define mariaDB connection and cursor
def set_db_session():

    mydb = mysql.connector.connect(host=config_dict.get('db').get('host'),
                                   user=config_dict.get('db').get('user'),
                                   password=config_dict.get('db').get('password'),
                                   database=config_dict.get('db').get('database'))
    mycursor = mydb.cursor()
    
    return mydb,mycursor

def fetch_token_header():
    ### access_token retrival
    # client credentials
    client_id = config_dict.get('client_creds').get('client_id')
    client_secret = config_dict.get('client_creds').get('client_secret')
    geturl = config_dict.get('client_creds').get('geturl')

    data = {'grant_type': 'client_credentials','client_id':client_id,'client_secret':client_secret}
    access_token_response = requests.post(geturl, data=data, verify=False, allow_redirects=False)
    access_token = json.loads(access_token_response.text)
    access_token = access_token.get("access_token")

    headers = CaseInsensitiveDict()
    headers["Authorization"] = f"Bearer {access_token}"
    
    return headers 

def combineNpivot(tab1,tab2,mydb):
    df1 = pd.read_sql(f"select * from {tab1}",mydb)
    df2 = pd.read_sql(f"select * from {tab2}",mydb)
    
    final_tab = pd.concat([df1,df2],ignore_index=True)
    final_tab.reset_index(drop=True,inplace=True)
    final_tab.columns = [c.upper() for c in final_tab.columns]
    
    final_tab.drop_duplicates(inplace=True)
    
    pred_flag = final_tab[['DATETIMESTAMP_ID','PREDICTED_FLAG']]
    pred_flag.drop_duplicates(inplace=True)
    pred_flag.set_index("DATETIMESTAMP_ID",inplace=True)
    
    final_tab = final_tab[['SERVERNAME', 'DATETIMESTAMP_ID', 'VALUE']]
    final_tab.columns = ['SERVERNAME', 'DATETIMESTAMP_ID', 'VALUE']
    
    avlb_servers = final_tab.SERVERNAME.unique()
    missing_servers = [a for a in server_dict.keys() if ~pd.Series(a).isin(avlb_servers)[0]]
    
    #calc number of hours to create DF index
    number_of_days = final_tab.DATETIMESTAMP_ID.max() - final_tab.DATETIMESTAMP_ID.min()
    number_of_hrs = (number_of_days.days*24) + (number_of_days.seconds/3600) + 1
    number_of_hrs = int(number_of_hrs)
    
    final_tab = final_tab.pivot(index="DATETIMESTAMP_ID", columns="SERVERNAME", values="VALUE")
    col_prefix=cols_dict.get(tab1)
    #extract server name those present in table and reanme columns based on it
    col_names = [col_prefix+'_SN'+server_dict.get(s) for s in server_dict if pd.Series(s).isin(avlb_servers)[0]]
    
    final_tab.columns = col_names
     #for missing servers, add dummy blank columns
    for m in missing_servers:
        colname = col_prefix+'_SN'+server_dict.get(m)
        col_names.append(colname) #add to master col list
        final_tab[colname] = np.nan
        
    temp_df = pd.DataFrame(data = {'tmp': range(number_of_hrs)},
                           index = pd.date_range(str(final_tab.index.min()).split(" ")[0], periods=number_of_hrs, freq="H"))

    final_tab = pd.concat([final_tab, temp_df], axis=1)
    final_tab = final_tab[col_names]
    final_tab = pd.merge(final_tab,pred_flag,"inner",left_index=True,right_index=True)
    
    return final_tab

def lag_transform(df,nlags,tab_name):
    col_prefix=cols_dict.get(tab_name)
    col_names = [col_prefix+'_SN'+server_dict.get(s) for s in server_dict]
    
    final_df=df[col_names].copy()
    for n in range(1,nlags+1):        
        lagged_df = df[col_names].shift(periods=n)
        lagged_df.columns = [f"{c}_L{n}H" for c in col_names]
        final_df = pd.concat([final_df,lagged_df],axis=1)
        
    #final_df = pd.merge(final_df,df[['PREDICTED_FLAG','HOUR']],"inner",left_index=True,right_index=True)
    final_df = pd.merge(final_df,df[['PREDICTED_FLAG']],"inner",left_index=True,right_index=True)
    return final_df

def lag_transform_hourly(df,nlags,tab_name):
    col_prefix=cols_dict.get(tab_name)
    col_names = [col_prefix+'_SN'+server_dict.get(s) for s in server_dict]
    
    final_df=df[col_names].copy()
    for n in range(1,nlags+1):        
        lagged_df = df[col_names].shift(periods=n)
        lagged_df.columns = [f"{c}_L{n}H" for c in col_names]
        final_df = pd.concat([final_df,lagged_df],axis=1)
        
    final_df = pd.merge(final_df,df[['PREDICTED_FLAG','HOUR']],"inner",left_index=True,right_index=True)
    return final_df

def calc_q3_whisker(x):
    q3 = np.quantile(x,0.75)
    q1 = np.quantile(x,0.25)
    IQR = q3 - q1
    return (q3 + 1.5*IQR)

def moving_average(x, w):
    return np.convolve(x, np.ones(w), 'same') / w

def combineNpivot_hourly(tab1,tab2,mydb):
    df1 = pd.read_sql(f"select * from {tab1} a where predicted_flag='Y' \
                    and Datetimestamp_ID>=(select DATE_SUB(min(Datetimestamp_ID),INTERVAL 20 HOUR) from {tab1} where \
                    predicted_flag='N') \
                    union all \
                    select * from {tab1} where predicted_flag='N'",mydb)
    
    df2 = pd.read_sql(f"select * from {tab2} a where predicted_flag='Y' \
                    and Datetimestamp_ID>=(select DATE_SUB(min(Datetimestamp_ID),INTERVAL 20 HOUR) from {tab2} where \
                    predicted_flag='N') \
                    union all \
                    select * from {tab2} where predicted_flag='N'",mydb)
    if (df1.shape[0]==0) and (df2.shape[0]==0):
        raise Exception("No new data to predict. Exiting....")
        
    final_tab = pd.concat([df1,df2],ignore_index=True)
    final_tab.reset_index(drop=True,inplace=True)
    final_tab.columns = [c.upper() for c in final_tab.columns]
    
    avlb_servers = final_tab.SERVERNAME.unique()
    missing_servers = [a for a in server_dict.keys() if ~pd.Series(a).isin(avlb_servers)[0]]
    
    final_tab.drop_duplicates(inplace=True)
    
    pred_flag = final_tab[['DATETIMESTAMP_ID','PREDICTED_FLAG']]
    pred_flag.drop_duplicates(inplace=True)
    pred_flag.set_index("DATETIMESTAMP_ID",inplace=True)
    
    final_tab = final_tab[['SERVERNAME', 'DATETIMESTAMP_ID', 'VALUE','PREDICTED_FLAG']]
    final_tab.columns = ['SERVERNAME', 'DATETIMESTAMP_ID', 'VALUE','PREDICTED_FLAG']
    
    #calc number of hours to create DF index
    number_of_days = final_tab.DATETIMESTAMP_ID.max() - final_tab.DATETIMESTAMP_ID.min()
    number_of_hrs = (number_of_days.days*24) + (number_of_days.seconds/3600) + 1
    number_of_hrs = int(number_of_hrs)
    print(final_tab.DATETIMESTAMP_ID.min())
    
    final_tab = final_tab.pivot(index="DATETIMESTAMP_ID", columns="SERVERNAME", values="VALUE")
    col_prefix=cols_dict.get(tab1)
    #extract server name those present in table and reanme columns based on it
    col_names = [col_prefix+'_SN'+server_dict.get(s) for s in server_dict if pd.Series(s).isin(avlb_servers)[0]]
        
    final_tab.columns = col_names
    #for missing servers, add dummy blank columns
    for m in missing_servers:
        colname = col_prefix+'_SN'+server_dict.get(m)
        col_names.append(colname) #add to master col list
        final_tab[colname] = np.nan
   
    temp_df = pd.DataFrame(data = {'tmp': range(number_of_hrs)},
                           index = pd.date_range(str(final_tab.index.min()).split(" ")[0], periods=number_of_hrs, freq="H"))

    final_tab = pd.concat([final_tab, temp_df], axis=1)
    final_tab = final_tab[col_names]
    final_tab = pd.merge(final_tab,pred_flag,"inner",left_index=True,right_index=True)
    
    return final_tab

def track_metrics(modelname,modeltype,model,X,y,mycursor,current_timestamp,remarks=None):
    y_pred = model.predict(X)
    r2 = round(100*metrics.r2_score(y_true=y,y_pred=y_pred,multioutput='variance_weighted'),2)
    rmse = round(math.sqrt(metrics.mean_squared_error(y_true=y,y_pred=y_pred)),2)
    
    rec = [modelname,modeltype,r2,rmse,X.shape[0],remarks,current_timestamp]

    add_recs = ("INSERT INTO track_metrics"
            "(modelname,modeltype,r2,rmse,cnt_records,remarks,etl_insertion_dt)"
            f"VALUES (%s, %s, %s, %s,%s,%s,%s)")
    mycursor.execute(add_recs,rec)
    
def track_metrics_hourly(y_actual,y_pred,mycursor,current_timestamp):
    r2 = round(100*metrics.r2_score(y_true=y_actual,y_pred=y_pred,multioutput='variance_weighted'),2)
    rmse = round(math.sqrt(metrics.mean_squared_error(y_true=y_actual,y_pred=y_pred)),2)
    
    rec = [modelname,modeltype,r2,rmse,X.shape[0],remarks,current_timestamp]

    add_recs = ("INSERT INTO track_metrics"
            "(modelname,modeltype,r2,rmse,cnt_records,remarks,etl_insertion_dt)"
            f"VALUES (%s, %s, %s, %s,%s,%s,%s)")
    mycursor.execute(add_recs,rec)

def store_model(model,modeltype,current_time,current_timestamp,mycursor):
    filename = f'xgb_{modeltype}_model_{current_time}.pkl'
    pickle.dump(model, open(filename, 'wb'))
    
    rec = [filename,modeltype,os.path.join('/home/cdsw/art_forecasting/models/',filename),current_timestamp]

    add_recs = ("INSERT INTO model_versions"
            "(modelname,modeltype,file_path,etl_insertion_dt)"
            f"VALUES (%s, %s, %s, %s)")
    mycursor.execute(add_recs,rec)
    
    return filename