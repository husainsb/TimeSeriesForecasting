from art_forecasting import *
#Date & Time displayed in Dashboard will be in SA time (GMT+2)
def execute(mydb,mycursor,current_timestamp):
    logger.info(f"Start of dashboard refresh........")
    
    #Bullet chart (sheet#1)
    try:
        current_hr = pd.to_datetime(current_timestamp)
        current_hr = current_hr+datetime.timedelta(hours=2)  #for SA time
        df = pd.read_sql("select * from avg_response_time_summary_ms where Datetimestamp_ID >= \
                        (select DATE_SUB(max(Datetimestamp_ID),INTERVAL 180 DAY) from \
                        avg_response_time_summary_ms)",mydb)
        df['Datetimestamp_ID'] = df['Datetimestamp_ID']+datetime.timedelta(hours=2)  #for SA time
        df['hour'] = df.Datetimestamp_ID.apply(lambda x: int(x.strftime("%H")))

        df_concat = pd.DataFrame(index = pd.date_range(current_hr+datetime.timedelta(hours=1),periods=6, freq="H"))
        logger.debug(f"Data from ART table fetched for dashboard!")
    except:
        logger.error(f"Error occured in fetching ART table for bullet chart. Exiting....")
        raise Exception(f"Error occured in fetching ART table for bullet chart: "+str(sys.exc_info()[1]))

    try:   
        final_df = pd.DataFrame()
        for i in range(df_concat.shape[0]):
            ind_value = df_concat.index.values[i]
            ind_hour = int(pd.to_datetime(ind_value).strftime("%H"))
            logger.debug(f"Bullet Chart: Data for predicted time {str(ind_value)} started...")
            ref_df = pd.DataFrame()
            for r in ['7 Days','1 Month','6 Month']:
                if r=='7 Days':
                    last_ref_date = current_hr - datetime.timedelta(days=7)

                    ref_range = df[df.Datetimestamp_ID>=last_ref_date].groupby("servername",as_index=False).agg({"value":[lambda x: np.quantile(x,q=.6),
                                                                                   lambda x: np.quantile(x,q=.75),
                                                                                   lambda x: np.quantile(x,q=.9),
                                                                                   lambda x: np.quantile(x,q=.99)]})
                elif r=='1 Month':
                    last_ref_date = current_hr - datetime.timedelta(days=30)

                    ref_range = df[df.Datetimestamp_ID>=last_ref_date].groupby("servername",as_index=False).agg({"value":[lambda x: np.quantile(x,q=.6),
                                                                                   lambda x: np.quantile(x,q=.75),
                                                                                   lambda x: np.quantile(x,q=.9),
                                                                                   lambda x: np.quantile(x,q=.99)]})
                elif r=='6 Month':
                    last_ref_date = current_hr - datetime.timedelta(days=180)

                    ref_range = df[(df.Datetimestamp_ID>=last_ref_date) & (df.hour==ind_hour)].groupby("servername",as_index=False).agg({"value":[lambda x: np.quantile(x,q=.6),
                                                                                   lambda x: np.quantile(x,q=.75),
                                                                                   lambda x: np.quantile(x,q=.9),
                                                                                   lambda x: np.quantile(x,q=.99)]})
                else:
                    break

                ref_range.columns = ['servername','Percentile_60','Percentile_75','Percentile_90','Percentile_99']

                ref_range['Reference'] = r
                ref_range['Current Hour'] = current_hr
                ref_range['Hour'] = pd.to_datetime(ind_value).strftime("%-I %p")  #hour to AM/PM conversion
                ref_range['Hour_int'] = ind_hour
                ref_range = ref_range[['Current Hour','Reference','Hour','Hour_int','servername','Percentile_60','Percentile_75','Percentile_90','Percentile_99']]

                ref_df = pd.concat([ref_df,ref_range],axis=0)
            final_df = pd.concat([final_df,ref_df],axis=0)
            logger.debug(f"Bullet Chart: Data for predicted time {str(ind_value)} ended!")
    except:
        logger.error(f"Error occured in fetching ART table for bullet chart. Exiting....")
        raise Exception(f"Error occured in fetching ART table for bullet chart: "+str(sys.exc_info()[1]))


    try:
        final_df['Current Hour'] = final_df['Current Hour'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        rec = final_df.to_records(index=False)
        rec = rec.tolist()

        mycursor.execute("DELETE FROM bullet_df")

        add_recs = "INSERT INTO bullet_df (current_hour,reference,hour,hour_int,servername,Percentile_60,Percentile_75, \
                    Percentile_90,Percentile_99) \
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        mycursor.executemany(add_recs,rec)
        logger.debug(f"Insertion to table 'bullet_df' was sucessful!")
    except:
        logger.error(f"Error occured in inserting data into 'bullet_df' table. Exiting....")
        raise Exception(f"Error occured in inserting data into 'bullet_df' table: "+str(sys.exc_info()[1]))

    mydb.commit()

    try:
        mycursor.execute("DROP TABLE IF EXISTS bullet_chart")
        mycursor.execute("CREATE TABLE bullet_chart as \
                    select * from bullet_df b, vw_predicted_df p where HOUR(p.DATETIMESTAMP_ID)=b.hour_int \
                    and p.DATETIMESTAMP_ID>b.current_hour order by reference,hour")
        logger.debug(f"Table 'bullet_chart' was sucessfully created!")
    except:
        logger.error(f"Error occured in inserting data into bullet_chart table. Exiting....")
        raise Exception(f"Error occured in inserting data into bullet_chart table: "+str(sys.exc_info()[1]))

    logger.info(f"Bullet charts refreshed!")
    #Sheet2: ART plots
    try:
        logger.info(f"ART plots refresh started.....")
        df_summ = pd.read_sql("select * from avg_response_time_summary_ms where Datetimestamp_ID >= (select DATE_SUB(max(Datetimestamp_ID),INTERVAL 180 DAY) from avg_response_time_summary_ms)",mydb)
        df_individual = pd.read_sql("select * from avg_response_time_ms where Datetimestamp_ID >= (select DATE_SUB(max(Datetimestamp_ID),INTERVAL 180 DAY) from avg_response_time_ms)",mydb)

        df_art = pd.concat([df_summ,df_individual],axis=0)

        df_art = df_art[['servername','Datetimestamp_ID','value']]
        df_art['Datetimestamp_ID'] = df_art['Datetimestamp_ID']+datetime.timedelta(hours=2)  #for SA time
        df_art = df_art.pivot(index="Datetimestamp_ID",columns="servername",values='value')
        df_art.reset_index(inplace=True)
        df_art.sort_values('Datetimestamp_ID',inplace=True)

        cols_server = df_art.iloc[:,1:].columns
        #take MA of 4hours
        for c in cols_server:
            df_art[c+'_MA4H'] = moving_average(df_art[c],4)

        df_art.drop(columns=cols_server,inplace=True)

        df_art = df_art.melt(id_vars="Datetimestamp_ID")
        df_art['servername'] = df_art['servername'].apply(lambda x: x.split("_")[0])  #capture original servername
        logger.debug(f"ART plots: Moving average of servers ART values created!")
    except:
        logger.error(f"Error occured in fetching ART tables for Sheet2 in dashboard. Exiting....")
        raise Exception(f"Error occured in fetching ART tables for Sheet2 in dashboard: "+str(sys.exc_info()[1]))

    try:
        weekly_df = df_art.loc[df_art.Datetimestamp_ID >= (df_art.Datetimestamp_ID.max()+datetime.timedelta(days=-7)),:]
        weekly_df['reference']='7 Days'
        monthly_df = df_art.loc[df_art.Datetimestamp_ID >= (df_art.Datetimestamp_ID.max()+datetime.timedelta(days=-30)),:]
        monthly_df['reference']='1 Month'

        df_art['reference']='6 Month'  #as original df contains last 6 months data

        final_df = pd.concat([weekly_df,monthly_df,df_art],axis=0)

        bullet_chart = pd.read_sql("Select reference,hour,pred_ART_6H as 6H,pred_ART_5H as 5H,pred_ART_4H as 4H,pred_ART_3H as 3H,pred_ART_2H as 2H,pred_ART_1H as 1H from bullet_chart",mydb)

        merged_df = pd.merge(final_df,bullet_chart,"inner",on=["reference"],copy=False)
        merged_df['act_hour'] = merged_df['Datetimestamp_ID'].apply(lambda x: x.strftime("%-I %p"))
        merged_df = merged_df[(merged_df.reference=='7 Days') | (merged_df.reference=='1 Month') |
                              ((merged_df.reference=='6 Month') & (merged_df.hour==merged_df.act_hour))]

        merged_df.drop(columns='act_hour',inplace=True)
    except:
        logger.error(f"Error occured in constructing data for ART plots in dashboard. Exiting....")
        raise Exception(f"Error occured in constructing data for ART plots in dashboard: "+str(sys.exc_info()[1]))

    try:    
    #Import into DB
        merged_df = merged_df.replace({np.nan: None})
        merged_df['Datetimestamp_ID']=merged_df['Datetimestamp_ID'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        rec = merged_df.to_records(index=False)

        mycursor.execute("DELETE FROM ART_plots_df")

        add_recs = ("INSERT INTO ART_plots_df"
                    "(Datetimestamp_ID,servername,value,reference,hour,6H,5H,4H,3H,2H,1H)"
                    f"VALUES (%s, %s, CAST(%s AS DOUBLE(20,5)), %s,%s,CAST(%s AS DOUBLE(20,5)), CAST(%s AS DOUBLE(20,5)), \
                    CAST(%s AS DOUBLE(20,5)),CAST(%s AS DOUBLE(20,5)),CAST(%s AS DOUBLE(20,5)),CAST(%s AS DOUBLE(20,5)))")

        mycursor.executemany(add_recs,rec.tolist())
        logger.debug(f"Insertion into ART plots table completed!")
    except:
        logger.error(f"Error occured in inserting data for ART plots in DB. Exiting....")
        raise Exception(f"Error occured in inserting data for ART plots in DB: "+str(sys.exc_info()[1]))
    mydb.commit()
    logger.info(f"Sheet2: ART plots refreshed!")

    #Sheet3&4: Server & Metric plots
    median_value= lambda x: np.nanquantile(x,.5,axis=0)
    p75_value= lambda x: np.nanquantile(x,.75,axis=0)
    p90_value= lambda x: np.nanquantile(x,.9,axis=0)

    try:
        logger.info(f"Server/Metric plots refresh started.....")
        final_quantiles_df = pd.DataFrame()
        metric_df = pd.DataFrame()
        for t in main_tables:
            logger.debug(f"Creation of Line plot data for table {t} started....")
            if t=='avg_response_time_ms':  #skip ART as its not required in sheet3&4
                continue
            #suffix tab1 with 'summary' to get tab2
            tab2 = 'avg_response_time_summary_ms' if t=='avg_response_time_ms' else t+'_summary'

            df_summ = pd.read_sql(f"select * from {tab2} where Datetimestamp_ID >= (select DATE_SUB(max(Datetimestamp_ID),INTERVAL 180 DAY) from {tab2})",mydb)
            df_individual = pd.read_sql(f"select * from {t} where Datetimestamp_ID >= (select DATE_SUB(max(Datetimestamp_ID),INTERVAL 180 DAY) from {t})",mydb)

            df_art = pd.concat([df_summ,df_individual],axis=0)

            df_art = df_art[['servername','Datetimestamp_ID','value']]
            df_art['Datetimestamp_ID'] = df_art['Datetimestamp_ID']+datetime.timedelta(hours=2)  #for SA time
            df_art = df_art.pivot(index="Datetimestamp_ID",columns="servername",values='value')
            df_art.reset_index(inplace=True)
            df_art.sort_values('Datetimestamp_ID',inplace=True)

            cols_server = df_art.iloc[:,1:].columns
            #take MA of 4hours
            for c in cols_server:
                df_art[c+'_MA4H'] = moving_average(df_art[c],4)

            df_art.drop(columns=cols_server,inplace=True)    
            df_art = df_art.melt(id_vars="Datetimestamp_ID")
            df_art['servername'] = df_art['servername'].apply(lambda x: x.split("_")[0])  #capture original servername

            #line plots df to be generated for all reference window
            weekly_df = df_art.loc[df_art.Datetimestamp_ID >= (df_art.Datetimestamp_ID.max()+datetime.timedelta(days=-7)),:]
            weekly_df['reference']='7 Days'

            monthly_df = df_art.loc[df_art.Datetimestamp_ID >= (df_art.Datetimestamp_ID.max()+datetime.timedelta(days=-30)),:]
            monthly_df['reference']='1 Month'

            df_art['reference']='6 Month (hourly-seasonal)'  #as original df contains last 6 months data
            df_art['hour'] = df_art['Datetimestamp_ID'].apply(lambda x: x.strftime('%-I %p'))

            final_df = pd.concat([weekly_df,monthly_df,df_art],axis=0)
            final_df['metric'] = t
            final_df['metric'] = final_df['metric'].map(cols_dict) 
            metric_df = pd.concat([metric_df,final_df],axis=0)
            df_art.drop(columns="hour",inplace=True)   #drop hour column as it will be recreated afterwards

            logger.debug(f"Line plot data created for table {t}")
            logger.debug(f"Quantile calculations started for metric {t}")
            #quantile calc in below codes
            weekly_quantile = weekly_df.pivot(["reference","Datetimestamp_ID"],"servername","value").reset_index()
            monthly_quantile = monthly_df.pivot(["reference","Datetimestamp_ID"],"servername","value").reset_index()
            sixmonth_quantile = df_art.pivot(["reference","Datetimestamp_ID"],"servername","value").reset_index()

            dict_Wquantiles = {'50_Percentile':np.nanquantile(weekly_quantile.iloc[:,2:],.5,axis=0),
                              '75_Percentile':np.nanquantile(weekly_quantile.iloc[:,2:],.75,axis=0),
                              '90_Percentile':np.nanquantile(weekly_quantile.iloc[:,2:],.9,axis=0),
                              'Mean':np.nanmean(weekly_quantile.iloc[:,2:],axis=0),
                              'reference':'7 Days',
                              'metric': t}
            W_quantiles = pd.DataFrame(dict_Wquantiles)
            W_quantiles['servername'] = cols_server

            dict_Mquantiles = {'50_Percentile':np.nanquantile(monthly_quantile.iloc[:,2:],.5,axis=0),
                              '75_Percentile':np.nanquantile(monthly_quantile.iloc[:,2:],.75,axis=0),
                              '90_Percentile':np.nanquantile(monthly_quantile.iloc[:,2:],.9,axis=0),
                               'Mean':np.nanmean(monthly_quantile.iloc[:,2:],axis=0),
                              'reference':'1 Month',
                              'metric': t}
            M_quantiles = pd.DataFrame(dict_Mquantiles)
            M_quantiles['servername'] = cols_server

            df_art['hour'] = df_art['Datetimestamp_ID'].apply(lambda x: x.strftime('%-I %p'))
            sixmonth_quantile_median=df_art.pivot_table("value",['reference','hour'],'servername',median_value).reset_index().melt(id_vars=['reference','hour'])
            sixmonth_quantile_median.rename(columns={"value":"50_Percentile"},inplace=True)
            sixmonth_quantile_75=df_art.pivot_table("value",['reference','hour'],'servername',p75_value).reset_index().melt(id_vars=['reference','hour'])
            sixmonth_quantile_75.rename(columns={"value":"75_Percentile"},inplace=True)
            sixmonth_quantile_90=df_art.pivot_table("value",['reference','hour'],'servername',p90_value).reset_index().melt(id_vars=['reference','hour'])
            sixmonth_quantile_90.rename(columns={"value":"90_Percentile"},inplace=True)
            sixmonth_mean=df_art.pivot_table("value",['reference','hour'],'servername',np.nanmean).reset_index().melt(id_vars=['reference','hour'])
            sixmonth_mean.rename(columns={"value":"Mean"},inplace=True)

            temp=pd.merge(sixmonth_quantile_median,sixmonth_quantile_75,"inner",on=['reference','hour','servername'],copy=False)
            temp=pd.merge(temp,sixmonth_quantile_90,"inner",on=['reference','hour','servername'],copy=False)
            sixmonth_merged=pd.merge(temp,sixmonth_mean,"inner",on=['reference','hour','servername'],copy=False)
            sixmonth_merged['metric']=t

            df_quantiles = pd.concat([W_quantiles,M_quantiles,sixmonth_merged],axis=0)
            df_quantiles['metric'] = df_quantiles['metric'].map(cols_dict) 

            final_quantiles_df = pd.concat([final_quantiles_df,df_quantiles],axis=0)
            logger.debug(f"Quantile calculations ended for metric {t}")
    except:
        logger.error(f"Error occured in constructing data for Server/Metric plots. Exiting....")
        raise Exception(f"Error occured in constructing data for Server/Metric plots: "+str(sys.exc_info()[1]))

    try:    
        final_quantiles_df['metric'] = final_quantiles_df['metric'].apply(lambda x: x+' ('+dashboard_metrics.get(x)+')')
        metric_df['metric'] = metric_df['metric'].apply(lambda x: x+' ('+dashboard_metrics.get(x)+')')

        #Import into DB
        final_quantiles_df = final_quantiles_df.replace({np.nan: None})
        rec = final_quantiles_df.to_records(index=False)

        mycursor.execute("DELETE FROM server_quantiles")

        add_recs = ("INSERT INTO server_quantiles"
                    "(50_Percentile,75_Percentile,90_Percentile,Mean,reference,metric,servername,hour)"
                    f"VALUES (CAST(%s AS DOUBLE(20,5)),CAST(%s AS DOUBLE(20,5)), CAST(%s AS DOUBLE(20,5)),CAST(%s AS DOUBLE(20,5)),%s,%s,%s,%s)")

        mycursor.executemany(add_recs,rec.tolist())
        logger.debug(f"Insertion to table 'server_quantiles' completed")
    except:
        logger.error(f"Error occured in inserting data into table 'server_quantiles'. Exiting....")
        raise Exception(f"Error occured in inserting data into table 'server_quantiles': "+str(sys.exc_info()[1]))
    mydb.commit()

    try:
        #Import into DB
        metric_df['Datetimestamp_ID']=metric_df['Datetimestamp_ID'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        metric_df = metric_df.replace({np.nan: None})
        rec = metric_df.to_records(index=False)

        mycursor.execute("DELETE FROM server_plots_df")

        add_recs = ("INSERT INTO server_plots_df"
                    "(Datetimestamp_ID,servername,value,reference,hour,metric)"
                    f"VALUES (%s,%s,CAST(%s AS DOUBLE(20,5)),%s,%s,%s)")

        mycursor.executemany(add_recs,rec.tolist())
        logger.debug(f"Insertion to table 'server_plots_df' was successful!")
    except:
        logger.error(f"Error occured in inserting data into table 'server_plots_df'. Exiting....")
        raise Exception(f"Error occured in inserting data into table 'server_plots_df': "+str(sys.exc_info()[1]))
    mydb.commit()
    logger.info(f"Server/Metrics plots refreshed!")
    logger.info(f"Dashboard refresh completed!")
