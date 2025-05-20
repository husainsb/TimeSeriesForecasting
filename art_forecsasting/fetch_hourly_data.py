from art_forecasting import *

#etl_insertion_dt = pd.to_datetime("2021-09-19 02:00:00") #overriding this variable defind in initial file
def execute(mydb,mycursor,headers,current_timestamp,etl_insertion_dt):
    logger.info(f"*****************************************")
    logger.info(f"ETL start date: {etl_insertion_dt}")

    try:
        tab_max_dates = pd.DataFrame()
        for t in tab_dict.keys():
            dt_df = pd.read_sql(f"SELECT MAX(DATETIMESTAMP_ID) FROM {t}",mydb)
            dt_df['table'] = t
            dt_df.rename(columns={'MAX(DATETIMESTAMP_ID)':'max_dt'},inplace=True)
            dt_df['etl_insertion_dt'] = etl_insertion_dt
            tab_max_dates = pd.concat([tab_max_dates,dt_df])
    except:
        logger.error("Error occured in creation of tab_max_dates DataFrame. Exiting....")
        raise Exception("Error occured in creation of tab_max_dates DataFrame: "+str(sys.exc_info()[1]))

    logger.debug(f"tab_max_dates DF created")
    #fetch records from AppDynamics and add to DB
    try:
        for t in tab_dict.keys():
            #fetch max date for which data is available in table
            max_table_dt = tab_max_dates.loc[tab_max_dates['table']==t,'max_dt'].max()
            max_table_dt = max_table_dt+datetime.timedelta(hours=1) #add 1 hour to take data from next hour
            start_time = round(max_table_dt.timestamp()*1000)  #convert to epoch ms

            #calc diff(hours) of 'now' and max date available
            duration_mins = pd.to_datetime(current_timestamp) - max_table_dt
            duration_mins = round((duration_mins.days*24)+(duration_mins.seconds/3600))
            logger.debug(f"{t} start date: {max_table_dt} & duration(hours): {duration_mins}")

            rec_cnt = 0
            for s in range(0,duration_mins+1):
                end_time = max_table_dt+datetime.timedelta(hours=s+1) #increment end time
                end_time = round(end_time.timestamp()*1000)  #convert to epoch ms

                new_start_time = start_time+(s*3600000)   #increment start time
                url = tab_dict.get(t)
                url = url.replace("{start_time}",str(new_start_time)).replace("{end_time}",str(end_time))

                #GET request
                resp = requests.get(url, headers=headers)

                servername_ind = 3 if t.find('hardware')>=0 else -2 
                #Fetch data and save as records
                abp_json = resp.json()
                complete_df = pd.DataFrame()
                #for Individual Nodes, this loop will run multiple times for each metric
                for n in np.arange(0,len(abp_json)):
                    if len(abp_json[n].get("metricValues"))>0:
                        abp_df = pd.DataFrame(abp_json[n].get("metricValues"))

                        #Create DF with repeated values of fixed values rows for each table
                        summarised_dict={}
                        for a in abp_json[n].keys():
                            if a!="metricValues":
                                summarised_dict[a] = abp_json[n].get(a)

                        summarised_df = pd.DataFrame(summarised_dict,index=np.arange(0,abp_df.shape[0]))

                        abp_df = pd.concat([summarised_df,abp_df],axis=1)
                        complete_df = pd.concat([complete_df,abp_df],axis=0)

                if complete_df.shape[0]>0:
                    complete_df.drop(columns='standardDeviation',inplace=True)  #dont consider SD column
                    complete_df["useRange"] = np.where(complete_df["useRange"]==False,0,1)
                    complete_df['Datetimestamp_ID'] = pd.to_datetime(complete_df['startTimeInMillis'],unit='ms')
                    complete_df['Datetimestamp_ID'] = complete_df['Datetimestamp_ID'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
                    complete_df['servername'] = complete_df['metricPath'].apply(lambda x: x.split("|")[servername_ind])
                    complete_df.columns = [c.lower() for c in complete_df.columns]  #all cols to lower case
                    complete_df = complete_df.loc[:,cols_list]  #re-arrange cols

                    rec = complete_df.to_records(index=False)

                    summary_flg = False if len(abp_json)>1 else True
                    #Import into DB
                    logger.debug(f"Insertion to DB starts")
                    add_recs = (f"INSERT INTO {t}"
                                "(metricid,metricname,metricpath,frequency,servername,starttimeinmillis,occurrences,current,min,max,userange,count,sum,value,Datetimestamp_ID,predicted_flag)"
                                f"VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'N')")

                    mycursor.executemany(add_recs,rec.tolist())
                    #mydb.commit()
                    logger.debug(f"Insertion to DB ends")
                    rec_cnt = rec_cnt + len(rec)

        #     if summary_flg:
        #         mycursor.execute(f"UPDATE {t} set metricsummary=true")
        #     else:
        #         mycursor.execute(f"UPDATE {t} set metricsummary=false")
        #     mydb.commit()
        #     logger.debug(f"Update summary flag ends")

            #Record and print number of records & last available date in logging table
            cnt_df = pd.read_sql(f"SELECT count(*) as cnt,max(Datetimestamp_ID) as last_available_dt FROM {t}",con = mydb)
            tab_max_dates.loc[(tab_max_dates['etl_insertion_dt']==etl_insertion_dt) &
                              (tab_max_dates['table']==t),'records_inserted'] = rec_cnt
            tab_max_dates.loc[(tab_max_dates['etl_insertion_dt']==etl_insertion_dt) &
                              (tab_max_dates['table']==t),'last_available_dt'] = cnt_df.last_available_dt[0]
            #print(f"#Records in {t}:{cnt_df.cnt[0]} & records inserted: {rec_cnt}")
            logger.info(f"#Records in {t}:{cnt_df.cnt[0]} & records inserted: {rec_cnt}")
    except:
        logger.error(f"Error occured in fetching records of {t} from AppDynamics. Exiting....")
        raise Exception(f"Error occured in fetching records of {t} from AppDynamics: "+str(sys.exc_info()[1]))

    #Import into DB
    try:
        tab_max_dates['max_dt']=tab_max_dates['max_dt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        tab_max_dates['etl_insertion_dt']=tab_max_dates['etl_insertion_dt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        tab_max_dates['last_available_dt']=tab_max_dates['last_available_dt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        tab_max_dates['records_inserted']=tab_max_dates['records_inserted'].astype(int)

        rec = tab_max_dates[tab_max_dates['records_inserted']>0].to_records(index=False)

        add_recs = ("INSERT INTO tab_max_dates"
                    "(max_dt,table_name,etl_insertion_dt,records_inserted,last_available_dt)"
                    f"VALUES (%s, %s, %s, %s,%s)")

        mycursor.executemany(add_recs,rec.tolist())
    except:
        logger.error(f"Error occured in insertion of records in table tab_max_dates in DB. Exiting....")
        raise Exception(f"Error occured in insertion of records in table tab_max_dates in DB: "+str(sys.exc_info()[1]))

    mydb.commit()
    logger.debug(f"Insertion into tab_max_dates ends")
    logger.info(f"Fetching of records succeeded")