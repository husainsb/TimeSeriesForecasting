from art_forecasting import *

def execute(mydb,mycursor):
    logger.info("Start of transformation and forecasting module..........")
    res = pd.DataFrame()
    try:
        for t in main_tables:
            #suffix tab1 with 'summary' to get tab2
            tab2 = 'avg_response_time_summary_ms' if t=='avg_response_time_ms' else t+'_summary'

            df = combineNpivot_hourly(t,tab2,mydb)
            #add extra 6 hours rows for forecast
            temp_df = pd.DataFrame({'HOUR':range(1,7)},
                                    index = pd.date_range(df.index.max()+datetime.timedelta(hours=1),
                                                          periods=6, freq="H"))

            df = pd.concat([df, temp_df], axis=0)

            final_df = lag_transform_hourly(df,12,t)
            res = pd.concat([res,final_df],axis=1)
    except:
        logger.error("Error occured in pivot/lag transform section. Exiting....")
        raise Exception("Error occured in pivot/lag transform section: "+str(sys.exc_info()[1]))

    logger.debug("New features created and extra 6 hours added for forecasting")
    res = res.loc[:,~res.columns.duplicated()]
    res.dropna(how='all',inplace=True)

    gc.collect()
    res_orig = res.copy()

    ## Fill NAs using bfill and ffill on selected columns
    res.drop(columns=['ART_SN00','HOUR'],inplace=True)
    res.bfill(axis=0,inplace=True)
    res.ffill(axis=0,inplace=True)

    res = pd.concat([res_orig[['ART_SN00','HOUR']],res],axis=1)
    logger.debug("Missing values imputed")


    refined_cols = ["ART_SN00","ART_SN01","ART_SN02","ART_SN03","ART_SN04","ART_SN05",
                    "ART_SN06","ART_SNCS","ART_SN00_L1H","ART_SN01_L1H","ART_SN02_L1H",
                    "ART_SN03_L1H","ART_SN04_L1H","ART_SN05_L1H","ART_SN06_L1H","ART_SNCS_L1H",
                    "ART_SN00_L2H","ART_SN01_L2H","ART_SN02_L2H","ART_SN03_L2H","ART_SN04_L2H",
                    "ART_SN05_L2H","ART_SN06_L2H","ART_SNCS_L2H","ART_SN00_L3H","ART_SN01_L3H",
                    "ART_SN02_L3H","ART_SN03_L3H","ART_SN04_L3H","ART_SN05_L3H","ART_SN06_L3H",
                    "ART_SNCS_L3H","ART_SN00_L4H","ART_SN01_L4H","ART_SN02_L4H","ART_SN03_L4H",
                    "ART_SN04_L4H","ART_SN05_L4H","ART_SN06_L4H","ART_SNCS_L4H","ART_SN00_L5H",
                    "ART_SN01_L5H","ART_SN02_L5H","ART_SN03_L5H","ART_SN04_L5H","ART_SN05_L5H",
                    "ART_SN06_L5H","ART_SNCS_L5H","ART_SN00_L6H","ART_SN01_L6H","ART_SN02_L6H",
                    "ART_SN03_L6H","ART_SN04_L6H","ART_SN05_L6H","ART_SN06_L6H","ART_SNCS_L6H",
                    "CPM_SN00","CPM_SN01","CPM_SN02","CPM_SN03","CPM_SN04","CPM_SN05","CPM_SN06",
                    "CPM_SNCS","CPM_SN00_L1H","CPM_SN01_L1H","CPM_SN02_L1H","CPM_SN03_L1H",
                    "CPM_SN04_L1H","CPM_SN05_L1H","CPM_SN06_L1H","CPM_SNCS_L1H","CPM_SN00_L2H",
                    "CPM_SN01_L2H","CPM_SN02_L2H","CPM_SN03_L2H","CPM_SN04_L2H","CPM_SN05_L2H",
                    "CPM_SN06_L2H","CPM_SNCS_L2H","CPM_SN00_L3H","CPM_SN01_L3H","CPM_SN02_L3H",
                    "CPM_SN03_L3H","CPM_SN04_L3H","CPM_SN05_L3H","CPM_SN06_L3H","CPM_SNCS_L3H",
                    "CPM_SN00_L4H","CPM_SN01_L4H","CPM_SN02_L4H","CPM_SN03_L4H","CPM_SN04_L4H",
                    "CPM_SN05_L4H","CPM_SN06_L4H","CPM_SNCS_L4H","CPM_SN00_L5H","CPM_SN01_L5H",\
                    "CPM_SN02_L5H","CPM_SN03_L5H","CPM_SN04_L5H","CPM_SN05_L5H","CPM_SN06_L5H",
                    "CPM_SNCS_L5H","CPM_SN00_L6H","CPM_SN01_L6H","CPM_SN02_L6H","CPM_SN03_L6H",
                    "CPM_SN04_L6H","CPM_SN05_L6H","CPM_SN06_L6H","CPM_SNCS_L6H","EPM_SN00",
                    "EPM_SN01","EPM_SN02","EPM_SN03","EPM_SN04","EPM_SN00_L1H","EPM_SN01_L1H",
                    "EPM_SN02_L1H","EPM_SN03_L1H","EPM_SN04_L1H","EPM_SN00_L2H","EPM_SN01_L2H",
                    "EPM_SN02_L2H","EPM_SN03_L2H","EPM_SN04_L2H","EPM_SN00_L3H","EPM_SN01_L3H",
                    "EPM_SN02_L3H","EPM_SN03_L3H","EPM_SN04_L3H","EPM_SN00_L4H","EPM_SN01_L4H",
                    "EPM_SN02_L4H","EPM_SN03_L4H","EPM_SN04_L4H","EPM_SN00_L5H","EPM_SN01_L5H",
                    "EPM_SN02_L5H","EPM_SN03_L5H","EPM_SN04_L5H","EPM_SN00_L6H","EPM_SN01_L6H",
                    "EPM_SN02_L6H","EPM_SN03_L6H","EPM_SN04_L6H","EXC_SN00","EXC_SN01","EXC_SN02",
                    "EXC_SN03","EXC_SN04","EXC_SN00_L1H","EXC_SN01_L1H","EXC_SN02_L1H","EXC_SN03_L1H",
                    "EXC_SN04_L1H","EXC_SN00_L2H","EXC_SN01_L2H","EXC_SN02_L2H","EXC_SN03_L2H","EXC_SN04_L2H",
                    "EXC_SN00_L3H","EXC_SN01_L3H","EXC_SN02_L3H","EXC_SN03_L3H","EXC_SN04_L3H","EXC_SN00_L4H",
                    "EXC_SN01_L4H","EXC_SN02_L4H","EXC_SN03_L4H","EXC_SN04_L4H","EXC_SN00_L5H","EXC_SN01_L5H",
                    "EXC_SN02_L5H","EXC_SN03_L5H","EXC_SN04_L5H","EXC_SN00_L6H","EXC_SN01_L6H","EXC_SN02_L6H",
                    "EXC_SN03_L6H","EXC_SN04_L6H","SLW_SN00","SLW_SN01","SLW_SN02","SLW_SN03","SLW_SN04","SLW_SN00_L1H",
                    "SLW_SN01_L1H","SLW_SN02_L1H","SLW_SN03_L1H","SLW_SN04_L1H","SLW_SN00_L2H","SLW_SN01_L2H","SLW_SN02_L2H",
                    "SLW_SN03_L2H","SLW_SN04_L2H","SLW_SN00_L3H","SLW_SN01_L3H","SLW_SN02_L3H","SLW_SN03_L3H","SLW_SN04_L3H",
                    "SLW_SN00_L4H","SLW_SN01_L4H","SLW_SN02_L4H","SLW_SN03_L4H","SLW_SN04_L4H","SLW_SN00_L5H",
                    "SLW_SN01_L5H","SLW_SN02_L5H","SLW_SN03_L5H","SLW_SN04_L5H","SLW_SN00_L6H","SLW_SN01_L6H",
                    "SLW_SN02_L6H","SLW_SN03_L6H","SLW_SN04_L6H","VSLW_SN00","VSLW_SN01","VSLW_SN02","VSLW_SN03",
                    "VSLW_SN04","VSLW_SN00_L1H","VSLW_SN01_L1H","VSLW_SN02_L1H","VSLW_SN03_L1H","VSLW_SN04_L1H",
                    "VSLW_SN00_L2H","VSLW_SN01_L2H","VSLW_SN02_L2H","VSLW_SN03_L2H","VSLW_SN04_L2H","VSLW_SN00_L3H",
                    "VSLW_SN01_L3H","VSLW_SN02_L3H","VSLW_SN03_L3H","VSLW_SN04_L3H","VSLW_SN00_L4H","VSLW_SN01_L4H",
                    "VSLW_SN02_L4H","VSLW_SN03_L4H","VSLW_SN04_L4H","VSLW_SN00_L5H","VSLW_SN01_L5H","VSLW_SN02_L5H",
                    "VSLW_SN03_L5H","VSLW_SN04_L5H","VSLW_SN00_L6H","VSLW_SN01_L6H","VSLW_SN02_L6H","VSLW_SN03_L6H",
                    "VSLW_SN04_L6H"]

    try:                
        full_refined = res.loc[:,refined_cols]
        cols_cutoff = pd.read_sql("select * from columns_transform_specs",mydb)
        for c in full_refined.columns:
            cutoff = list(cols_cutoff.loc[cols_cutoff.colname==c,'outlier_cutoff'])[0]
            full_refined[c] = np.where(full_refined[c]>=cutoff,cutoff,full_refined[c])
    except:
        logger.error("Error occured in Outlier treatment section. Exiting....")
        raise Exception("Error occured in Outlier treatment section: "+str(sys.exc_info()[1]))    

    logger.debug("Outliers treated")

    try:    
        full_refined['HR_VAL'] = full_refined.index.to_series().apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').hour)
        full_refined['DAY_OF_WEEK'] = full_refined.index.to_series().apply(lambda x: str(datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').isoweekday()))
        full_refined['WEEKEND_FLG'] = full_refined.DAY_OF_WEEK.apply(lambda x: '1' if ((x == '6') or (x == '7')) else '0')
        #Monday:1.... Sunday:7
        logger.info("Features like Hour, DOW, Weekend flag has been added")

        full_refined['ART_SN00_L6H_MA12'] = moving_average(full_refined['ART_SN00_L6H'],12)
        full_refined['ART_SN00_L5H_MA12'] = moving_average(full_refined['ART_SN00_L5H'],12)
        full_refined['ART_SN00_L4H_MA12'] = moving_average(full_refined['ART_SN00_L4H'],12)
        full_refined['ART_SN00_L3H_MA12'] = moving_average(full_refined['ART_SN00_L3H'],12)
        full_refined['ART_SN00_L2H_MA12'] = moving_average(full_refined['ART_SN00_L2H'],12)
        full_refined['ART_SN00_L1H_MA12'] = moving_average(full_refined['ART_SN00_L1H'],12)

        full_refined['ART_SN01_L6H_MA12'] = moving_average(full_refined['ART_SN01_L6H'],12)
        full_refined['ART_SN01_L5H_MA12'] = moving_average(full_refined['ART_SN01_L5H'],12)
        full_refined['ART_SN01_L4H_MA12'] = moving_average(full_refined['ART_SN01_L4H'],12)
        full_refined['ART_SN01_L3H_MA12'] = moving_average(full_refined['ART_SN01_L3H'],12)
        full_refined['ART_SN01_L2H_MA12'] = moving_average(full_refined['ART_SN01_L2H'],12)
        full_refined['ART_SN01_L1H_MA12'] = moving_average(full_refined['ART_SN01_L1H'],12)

        full_refined['ART_SN02_L6H_MA12'] = moving_average(full_refined['ART_SN02_L6H'],12)
        full_refined['ART_SN02_L5H_MA12'] = moving_average(full_refined['ART_SN02_L5H'],12)
        full_refined['ART_SN02_L4H_MA12'] = moving_average(full_refined['ART_SN02_L4H'],12)
        full_refined['ART_SN02_L3H_MA12'] = moving_average(full_refined['ART_SN02_L3H'],12)
        full_refined['ART_SN02_L2H_MA12'] = moving_average(full_refined['ART_SN02_L2H'],12)
        full_refined['ART_SN02_L1H_MA12'] = moving_average(full_refined['ART_SN02_L1H'],12)

        full_refined['ART_SN03_L6H_MA12'] = moving_average(full_refined['ART_SN03_L6H'],12)
        full_refined['ART_SN03_L5H_MA12'] = moving_average(full_refined['ART_SN03_L5H'],12)
        full_refined['ART_SN03_L4H_MA12'] = moving_average(full_refined['ART_SN03_L4H'],12)
        full_refined['ART_SN03_L3H_MA12'] = moving_average(full_refined['ART_SN03_L3H'],12)
        full_refined['ART_SN03_L2H_MA12'] = moving_average(full_refined['ART_SN03_L2H'],12)
        full_refined['ART_SN03_L1H_MA12'] = moving_average(full_refined['ART_SN03_L1H'],12)

        full_refined['ART_SN04_L6H_MA12'] = moving_average(full_refined['ART_SN04_L6H'],12)
        full_refined['ART_SN04_L5H_MA12'] = moving_average(full_refined['ART_SN04_L5H'],12)
        full_refined['ART_SN04_L4H_MA12'] = moving_average(full_refined['ART_SN04_L4H'],12)
        full_refined['ART_SN04_L3H_MA12'] = moving_average(full_refined['ART_SN04_L3H'],12)
        full_refined['ART_SN04_L2H_MA12'] = moving_average(full_refined['ART_SN04_L2H'],12)
        full_refined['ART_SN04_L1H_MA12'] = moving_average(full_refined['ART_SN04_L1H'],12)

        full_refined['ART_SN05_L6H_MA12'] = moving_average(full_refined['ART_SN05_L6H'],12)
        full_refined['ART_SN05_L5H_MA12'] = moving_average(full_refined['ART_SN05_L5H'],12)
        full_refined['ART_SN05_L4H_MA12'] = moving_average(full_refined['ART_SN05_L4H'],12)
        full_refined['ART_SN05_L3H_MA12'] = moving_average(full_refined['ART_SN05_L3H'],12)
        full_refined['ART_SN05_L2H_MA12'] = moving_average(full_refined['ART_SN05_L2H'],12)
        full_refined['ART_SN05_L1H_MA12'] = moving_average(full_refined['ART_SN05_L1H'],12)

        full_refined['ART_SN06_L6H_MA12'] = moving_average(full_refined['ART_SN06_L6H'],12)
        full_refined['ART_SN06_L5H_MA12'] = moving_average(full_refined['ART_SN06_L5H'],12)
        full_refined['ART_SN06_L4H_MA12'] = moving_average(full_refined['ART_SN06_L4H'],12)
        full_refined['ART_SN06_L3H_MA12'] = moving_average(full_refined['ART_SN06_L3H'],12)
        full_refined['ART_SN06_L2H_MA12'] = moving_average(full_refined['ART_SN06_L2H'],12)
        full_refined['ART_SN06_L1H_MA12'] = moving_average(full_refined['ART_SN06_L1H'],12)

        full_refined['ART_SNCS_L6H_MA12'] = moving_average(full_refined['ART_SNCS_L6H'],12)
        full_refined['ART_SNCS_L5H_MA12'] = moving_average(full_refined['ART_SNCS_L5H'],12)
        full_refined['ART_SNCS_L4H_MA12'] = moving_average(full_refined['ART_SNCS_L4H'],12)
        full_refined['ART_SNCS_L3H_MA12'] = moving_average(full_refined['ART_SNCS_L3H'],12)
        full_refined['ART_SNCS_L2H_MA12'] = moving_average(full_refined['ART_SNCS_L2H'],12)
        full_refined['ART_SNCS_L1H_MA12'] = moving_average(full_refined['ART_SNCS_L1H'],12)
        #
        full_refined['CPM_SN00_L6H_MA12'] = moving_average(full_refined['CPM_SN00_L6H'],12)
        full_refined['CPM_SN00_L5H_MA12'] = moving_average(full_refined['CPM_SN00_L5H'],12)
        full_refined['CPM_SN00_L4H_MA12'] = moving_average(full_refined['CPM_SN00_L4H'],12)
        full_refined['CPM_SN00_L3H_MA12'] = moving_average(full_refined['CPM_SN00_L3H'],12)
        full_refined['CPM_SN00_L2H_MA12'] = moving_average(full_refined['CPM_SN00_L2H'],12)
        full_refined['CPM_SN00_L1H_MA12'] = moving_average(full_refined['CPM_SN00_L1H'],12)

        full_refined['EPM_SN00_L6H_MA12'] = moving_average(full_refined['EPM_SN00_L6H'],12)
        full_refined['EPM_SN00_L5H_MA12'] = moving_average(full_refined['EPM_SN00_L5H'],12)
        full_refined['EPM_SN00_L4H_MA12'] = moving_average(full_refined['EPM_SN00_L4H'],12)
        full_refined['EPM_SN00_L3H_MA12'] = moving_average(full_refined['EPM_SN00_L3H'],12)
        full_refined['EPM_SN00_L2H_MA12'] = moving_average(full_refined['EPM_SN00_L2H'],12)
        full_refined['EPM_SN00_L1H_MA12'] = moving_average(full_refined['EPM_SN00_L1H'],12)

        full_refined['EXC_SN00_L6H_MA12'] = moving_average(full_refined['EXC_SN00_L6H'],12)
        full_refined['EXC_SN00_L5H_MA12'] = moving_average(full_refined['EXC_SN00_L5H'],12)
        full_refined['EXC_SN00_L4H_MA12'] = moving_average(full_refined['EXC_SN00_L4H'],12)
        full_refined['EXC_SN00_L3H_MA12'] = moving_average(full_refined['EXC_SN00_L3H'],12)
        full_refined['EXC_SN00_L2H_MA12'] = moving_average(full_refined['EXC_SN00_L2H'],12)
        full_refined['EXC_SN00_L1H_MA12'] = moving_average(full_refined['EXC_SN00_L1H'],12)

        full_refined['SLW_SN00_L6H_MA12'] = moving_average(full_refined['SLW_SN00_L6H'],12)
        full_refined['SLW_SN00_L5H_MA12'] = moving_average(full_refined['SLW_SN00_L5H'],12)
        full_refined['SLW_SN00_L4H_MA12'] = moving_average(full_refined['SLW_SN00_L4H'],12)
        full_refined['SLW_SN00_L3H_MA12'] = moving_average(full_refined['SLW_SN00_L3H'],12)
        full_refined['SLW_SN00_L2H_MA12'] = moving_average(full_refined['SLW_SN00_L2H'],12)
        full_refined['SLW_SN00_L1H_MA12'] = moving_average(full_refined['SLW_SN00_L1H'],12)

        full_refined['VSLW_SN00_L6H_MA12'] = moving_average(full_refined['VSLW_SN00_L6H'],12)
        full_refined['VSLW_SN00_L5H_MA12'] = moving_average(full_refined['VSLW_SN00_L5H'],12)
        full_refined['VSLW_SN00_L4H_MA12'] = moving_average(full_refined['VSLW_SN00_L4H'],12)
        full_refined['VSLW_SN00_L3H_MA12'] = moving_average(full_refined['VSLW_SN00_L3H'],12)
        full_refined['VSLW_SN00_L2H_MA12'] = moving_average(full_refined['VSLW_SN00_L2H'],12)
        full_refined['VSLW_SN00_L1H_MA12'] = moving_average(full_refined['VSLW_SN00_L1H'],12)

        ######################

        full_refined['ART_SN00_L6H_MA3'] = moving_average(full_refined['ART_SN00_L6H'],3)
        full_refined['ART_SN00_L5H_MA3'] = moving_average(full_refined['ART_SN00_L5H'],3)
        full_refined['ART_SN00_L4H_MA3'] = moving_average(full_refined['ART_SN00_L4H'],3)
        full_refined['ART_SN00_L3H_MA3'] = moving_average(full_refined['ART_SN00_L3H'],3)
        full_refined['ART_SN00_L2H_MA3'] = moving_average(full_refined['ART_SN00_L2H'],3)
        full_refined['ART_SN00_L1H_MA3'] = moving_average(full_refined['ART_SN00_L1H'],3)

        full_refined['ART_SN01_L6H_MA3'] = moving_average(full_refined['ART_SN01_L6H'],3)
        full_refined['ART_SN01_L5H_MA3'] = moving_average(full_refined['ART_SN01_L5H'],3)
        full_refined['ART_SN01_L4H_MA3'] = moving_average(full_refined['ART_SN01_L4H'],3)
        full_refined['ART_SN01_L3H_MA3'] = moving_average(full_refined['ART_SN01_L3H'],3)
        full_refined['ART_SN01_L2H_MA3'] = moving_average(full_refined['ART_SN01_L2H'],3)
        full_refined['ART_SN01_L1H_MA3'] = moving_average(full_refined['ART_SN01_L1H'],3)

        full_refined['ART_SN02_L6H_MA3'] = moving_average(full_refined['ART_SN02_L6H'],3)
        full_refined['ART_SN02_L5H_MA3'] = moving_average(full_refined['ART_SN02_L5H'],3)
        full_refined['ART_SN02_L4H_MA3'] = moving_average(full_refined['ART_SN02_L4H'],3)
        full_refined['ART_SN02_L3H_MA3'] = moving_average(full_refined['ART_SN02_L3H'],3)
        full_refined['ART_SN02_L2H_MA3'] = moving_average(full_refined['ART_SN02_L2H'],3)
        full_refined['ART_SN02_L1H_MA3'] = moving_average(full_refined['ART_SN02_L1H'],3)

        full_refined['ART_SN03_L6H_MA3'] = moving_average(full_refined['ART_SN03_L6H'],3)
        full_refined['ART_SN03_L5H_MA3'] = moving_average(full_refined['ART_SN03_L5H'],3)
        full_refined['ART_SN03_L4H_MA3'] = moving_average(full_refined['ART_SN03_L4H'],3)
        full_refined['ART_SN03_L3H_MA3'] = moving_average(full_refined['ART_SN03_L3H'],3)
        full_refined['ART_SN03_L2H_MA3'] = moving_average(full_refined['ART_SN03_L2H'],3)
        full_refined['ART_SN03_L1H_MA3'] = moving_average(full_refined['ART_SN03_L1H'],3)

        full_refined['ART_SN04_L6H_MA3'] = moving_average(full_refined['ART_SN04_L6H'],3)
        full_refined['ART_SN04_L5H_MA3'] = moving_average(full_refined['ART_SN04_L5H'],3)
        full_refined['ART_SN04_L4H_MA3'] = moving_average(full_refined['ART_SN04_L4H'],3)
        full_refined['ART_SN04_L3H_MA3'] = moving_average(full_refined['ART_SN04_L3H'],3)
        full_refined['ART_SN04_L2H_MA3'] = moving_average(full_refined['ART_SN04_L2H'],3)
        full_refined['ART_SN04_L1H_MA3'] = moving_average(full_refined['ART_SN04_L1H'],3)

        full_refined['ART_SN05_L6H_MA3'] = moving_average(full_refined['ART_SN05_L6H'],3)
        full_refined['ART_SN05_L5H_MA3'] = moving_average(full_refined['ART_SN05_L5H'],3)
        full_refined['ART_SN05_L4H_MA3'] = moving_average(full_refined['ART_SN05_L4H'],3)
        full_refined['ART_SN05_L3H_MA3'] = moving_average(full_refined['ART_SN05_L3H'],3)
        full_refined['ART_SN05_L2H_MA3'] = moving_average(full_refined['ART_SN05_L2H'],3)
        full_refined['ART_SN05_L1H_MA3'] = moving_average(full_refined['ART_SN05_L1H'],3)

        full_refined['ART_SN06_L6H_MA3'] = moving_average(full_refined['ART_SN06_L6H'],3)
        full_refined['ART_SN06_L5H_MA3'] = moving_average(full_refined['ART_SN06_L5H'],3)
        full_refined['ART_SN06_L4H_MA3'] = moving_average(full_refined['ART_SN06_L4H'],3)
        full_refined['ART_SN06_L3H_MA3'] = moving_average(full_refined['ART_SN06_L3H'],3)
        full_refined['ART_SN06_L2H_MA3'] = moving_average(full_refined['ART_SN06_L2H'],3)
        full_refined['ART_SN06_L1H_MA3'] = moving_average(full_refined['ART_SN06_L1H'],3)

        full_refined['ART_SNCS_L6H_MA3'] = moving_average(full_refined['ART_SNCS_L6H'],3)
        full_refined['ART_SNCS_L5H_MA3'] = moving_average(full_refined['ART_SNCS_L5H'],3)
        full_refined['ART_SNCS_L4H_MA3'] = moving_average(full_refined['ART_SNCS_L4H'],3)
        full_refined['ART_SNCS_L3H_MA3'] = moving_average(full_refined['ART_SNCS_L3H'],3)
        full_refined['ART_SNCS_L2H_MA3'] = moving_average(full_refined['ART_SNCS_L2H'],3)
        full_refined['ART_SNCS_L1H_MA3'] = moving_average(full_refined['ART_SNCS_L1H'],3)
        #
        full_refined['CPM_SN00_L6H_MA3'] = moving_average(full_refined['CPM_SN00_L6H'],3)
        full_refined['CPM_SN00_L5H_MA3'] = moving_average(full_refined['CPM_SN00_L5H'],3)
        full_refined['CPM_SN00_L4H_MA3'] = moving_average(full_refined['CPM_SN00_L4H'],3)
        full_refined['CPM_SN00_L3H_MA3'] = moving_average(full_refined['CPM_SN00_L3H'],3)
        full_refined['CPM_SN00_L2H_MA3'] = moving_average(full_refined['CPM_SN00_L2H'],3)
        full_refined['CPM_SN00_L1H_MA3'] = moving_average(full_refined['CPM_SN00_L1H'],3)

        full_refined['EPM_SN00_L6H_MA3'] = moving_average(full_refined['EPM_SN00_L6H'],3)
        full_refined['EPM_SN00_L5H_MA3'] = moving_average(full_refined['EPM_SN00_L5H'],3)
        full_refined['EPM_SN00_L4H_MA3'] = moving_average(full_refined['EPM_SN00_L4H'],3)
        full_refined['EPM_SN00_L3H_MA3'] = moving_average(full_refined['EPM_SN00_L3H'],3)
        full_refined['EPM_SN00_L2H_MA3'] = moving_average(full_refined['EPM_SN00_L2H'],3)
        full_refined['EPM_SN00_L1H_MA3'] = moving_average(full_refined['EPM_SN00_L1H'],3)

        full_refined['EXC_SN00_L6H_MA3'] = moving_average(full_refined['EXC_SN00_L6H'],3)
        full_refined['EXC_SN00_L5H_MA3'] = moving_average(full_refined['EXC_SN00_L5H'],3)
        full_refined['EXC_SN00_L4H_MA3'] = moving_average(full_refined['EXC_SN00_L4H'],3)
        full_refined['EXC_SN00_L3H_MA3'] = moving_average(full_refined['EXC_SN00_L3H'],3)
        full_refined['EXC_SN00_L2H_MA3'] = moving_average(full_refined['EXC_SN00_L2H'],3)
        full_refined['EXC_SN00_L1H_MA3'] = moving_average(full_refined['EXC_SN00_L1H'],3)

        full_refined['SLW_SN00_L6H_MA3'] = moving_average(full_refined['SLW_SN00_L6H'],3)
        full_refined['SLW_SN00_L5H_MA3'] = moving_average(full_refined['SLW_SN00_L5H'],3)
        full_refined['SLW_SN00_L4H_MA3'] = moving_average(full_refined['SLW_SN00_L4H'],3)
        full_refined['SLW_SN00_L3H_MA3'] = moving_average(full_refined['SLW_SN00_L3H'],3)
        full_refined['SLW_SN00_L2H_MA3'] = moving_average(full_refined['SLW_SN00_L2H'],3)
        full_refined['SLW_SN00_L1H_MA3'] = moving_average(full_refined['SLW_SN00_L1H'],3)

        full_refined['VSLW_SN00_L6H_MA3'] = moving_average(full_refined['VSLW_SN00_L6H'],3)
        full_refined['VSLW_SN00_L5H_MA3'] = moving_average(full_refined['VSLW_SN00_L5H'],3)
        full_refined['VSLW_SN00_L4H_MA3'] = moving_average(full_refined['VSLW_SN00_L4H'],3)
        full_refined['VSLW_SN00_L3H_MA3'] = moving_average(full_refined['VSLW_SN00_L3H'],3)
        full_refined['VSLW_SN00_L2H_MA3'] = moving_average(full_refined['VSLW_SN00_L2H'],3)
        full_refined['VSLW_SN00_L1H_MA3'] = moving_average(full_refined['VSLW_SN00_L1H'],3)
    except:
        logger.error("Error occured in Feature engineering section. Exiting....")
        raise Exception("Error occured in Feature engineering section: "+str(sys.exc_info()[1])) 

    logger.info("Moving Average with Lag features has been added")

    #feature scaling
    try:
        standard_scaler = pickle.load(open("./art_forecasting/XGB Models Params/scaler.pkl","rb"))
        num_data = full_refined.drop(columns=['ART_SN00','DAY_OF_WEEK','WEEKEND_FLG'])
        data_scaled = pd.DataFrame(standard_scaler.transform(num_data))
        data_scaled.columns = num_data.columns
        data_scaled.index=num_data.index
    except:
        logger.error("Error occured in Feature scaling section. Exiting....")
        raise Exception("Error occured in Feature scaling section: "+str(sys.exc_info()[1]))

    final_data = pd.concat([full_refined[['ART_SN00','DAY_OF_WEEK', 'WEEKEND_FLG']],data_scaled],axis=1)
    logger.debug("Features scaled!")

    #impute hour field where its blank and row yet to be predicted
    empty_hour_ind = res[(res.PREDICTED_FLAG=='N') & (res.HOUR.isna())].index.values
    last_predicted_hour = max(res[(res.PREDICTED_FLAG=='Y')].index)

    for i in empty_hour_ind:
        hour_diff = i - last_predicted_hour
        hour_diff = round(hour_diff.days*24+hour_diff.seconds/3600)
        res.loc[i,'HOUR']=hour_diff

    min_start_ts = res[(res.PREDICTED_FLAG=='Y')].index.max()  #take latest available time of predicted hour
    max_end_ts = res.index.max()  #take latest available time of predicted hour

    if (pd.isnull(min_start_ts)) or (pd.isnull(max_end_ts)) or (min_start_ts==max_end_ts):
        logger.error("Either latest predicted hour is not available or max hour to be predicted is not avaiable or both are equal. Exiting....")
        raise Exception("Either latest predicted hour is not available or max hour to be predicted is not avaiable or both are equal")

    logger.info(f"Last available predicted hour: {min_start_ts} & max hour to be predicted: {max_end_ts}")
    # XGBoost Baseline for 6 hour window prediction
    gc.collect()
    try:
        hour_id = '6H'
        cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG','ART_SN00_L6H_MA12','ART_SN01_L6H_MA12',
                'ART_SN02_L6H_MA12','ART_SN03_L6H_MA12','ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12',
                'ART_SNCS_L6H_MA12','CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
                'VSLW_SN00_L6H_MA12']

        #predict all those hours which may have been missed and forecast the new ones(future)
        forecast_hours_ind=res.loc[(res.index>min_start_ts) & (res.index<=max_end_ts+datetime.timedelta(hours=float(hour_id[0])-6))].index

        X_val = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].values
        X_val_ind = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].index
        y_val = final_data.loc[forecast_hours_ind,'ART_SN00'].values
        y_val_ind = forecast_hours_ind

        #extract latest model version and load
        modelname = pd.read_sql(f"select modelname from model_versions a where modeltype='{hour_id}' and etl_insertion_dt= \
                    (select max(etl_insertion_dt) from model_versions b where a.modeltype=b.modeltype)",mydb)

        modelname = list(modelname.modelname)[0]
        model=pickle.load(open("./art_forecasting/models/"+modelname,"rb"))

        val_prediction = pd.DataFrame({
                                       f'pred_{hour_id}_window':model.predict(X_val)},index=y_val_ind)
        #concat all
        predicted_df = val_prediction.copy()
        logger.info(f"Model successfully ran for hour ID: {hour_id}")
        # XGBoost Baseline for 5 hour window prediction
        hour_id = '5H'
        cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG','ART_SN00_L5H_MA12','ART_SN01_L5H_MA12',
                 'ART_SN02_L5H_MA12','ART_SN03_L5H_MA12','ART_SN04_L5H_MA12','ART_SN05_L5H_MA12','ART_SN06_L5H_MA12',
                 'ART_SNCS_L5H_MA12','CPM_SN00_L5H_MA12','EPM_SN00_L5H_MA12','EXC_SN00_L5H_MA12','SLW_SN00_L5H_MA12',
                 'VSLW_SN00_L5H_MA12','ART_SN00_L6H_MA12','ART_SN01_L6H_MA12','ART_SN02_L6H_MA12',
                 'ART_SN03_L6H_MA12','ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12',
                 'ART_SNCS_L6H_MA12','CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
                 'VSLW_SN00_L6H_MA12']

        #data = data[:-6]
        forecast_hours_ind=res.loc[(res.index>min_start_ts) & (res.index<=max_end_ts+datetime.timedelta(hours=float(hour_id[0])-6))].index
        X_val = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].values
        X_val_ind = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].index
        y_val = final_data.loc[forecast_hours_ind,'ART_SN00'].values
        y_val_ind = forecast_hours_ind

        #extract latest model version and load
        modelname = pd.read_sql(f"select modelname from model_versions a where modeltype='{hour_id}' and etl_insertion_dt= \
                    (select max(etl_insertion_dt) from model_versions b where a.modeltype=b.modeltype)",mydb)

        modelname = list(modelname.modelname)[0]
        model=pickle.load(open("./art_forecasting/models/"+modelname,"rb"))

        val_prediction = pd.DataFrame({
                                       f'pred_{hour_id}_window':model.predict(X_val)},index=y_val_ind)
        #concat all
        predicted_df = pd.merge(predicted_df,val_prediction,"outer",left_index=True,right_index=True,copy=False)
        logger.info(f"Model successfully ran for hour ID: {hour_id}")
        #XGBoost Baseline for 4 hour window prediction
        hour_id = '4H'
        cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG',
                 'ART_SN00_L4H_MA12','ART_SN01_L4H_MA12','ART_SN02_L4H_MA12','ART_SN03_L4H_MA12',
                 'ART_SN04_L4H_MA12','ART_SN05_L4H_MA12','ART_SN06_L4H_MA12','ART_SNCS_L4H_MA12',
                 'CPM_SN00_L4H_MA12','EPM_SN00_L4H_MA12','EXC_SN00_L4H_MA12','SLW_SN00_L4H_MA12',
                 'VSLW_SN00_L4H_MA12',
                 'ART_SN00_L5H_MA12','ART_SN01_L5H_MA12','ART_SN02_L5H_MA12','ART_SN03_L5H_MA12',
                 'ART_SN04_L5H_MA12','ART_SN05_L5H_MA12','ART_SN06_L5H_MA12','ART_SNCS_L5H_MA12',
                 'CPM_SN00_L5H_MA12','EPM_SN00_L5H_MA12','EXC_SN00_L5H_MA12','SLW_SN00_L5H_MA12',
                 'VSLW_SN00_L5H_MA12',
                 'ART_SN00_L6H_MA12','ART_SN01_L6H_MA12','ART_SN02_L6H_MA12','ART_SN03_L6H_MA12',
                 'ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12','ART_SNCS_L6H_MA12',
                 'CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
                 'VSLW_SN00_L6H_MA12']

        #data = data[:-6]

        forecast_hours_ind=res.loc[(res.index>min_start_ts) & (res.index<=max_end_ts+datetime.timedelta(hours=float(hour_id[0])-6))].index

        X_val = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].values
        X_val_ind = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].index
        y_val = final_data.loc[forecast_hours_ind,'ART_SN00'].values
        y_val_ind = forecast_hours_ind

        #extract latest model version and load
        modelname = pd.read_sql(f"select modelname from model_versions a where modeltype='{hour_id}' and etl_insertion_dt= \
                    (select max(etl_insertion_dt) from model_versions b where a.modeltype=b.modeltype)",mydb)

        modelname = list(modelname.modelname)[0]
        model=pickle.load(open("./art_forecasting/models/"+modelname,"rb"))

        val_prediction = pd.DataFrame({
                                       f'pred_{hour_id}_window':model.predict(X_val)},index=y_val_ind)
        #concat all
        predicted_df = pd.merge(predicted_df,val_prediction,"outer",left_index=True,right_index=True,copy=False)
        logger.info(f"Model successfully ran for hour ID: {hour_id}")
        #XGBoost Baseline for 3 hour window prediction
        hour_id = '3H'
        cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG',
                 'ART_SN00_L3H_MA12','ART_SN01_L3H_MA12',
                 'ART_SN02_L3H_MA12','ART_SN03_L3H_MA12','ART_SN04_L3H_MA12','ART_SN05_L3H_MA12','ART_SN06_L3H_MA12',
                 'ART_SNCS_L3H_MA12','CPM_SN00_L3H_MA12','EPM_SN00_L3H_MA12','EXC_SN00_L3H_MA12','SLW_SN00_L3H_MA12',
                 'VSLW_SN00_L3H_MA12',
                 'ART_SN00_L4H_MA12','ART_SN01_L4H_MA12','ART_SN02_L4H_MA12','ART_SN03_L4H_MA12',
                 'ART_SN04_L4H_MA12','ART_SN05_L4H_MA12','ART_SN06_L4H_MA12','ART_SNCS_L4H_MA12',
                 'CPM_SN00_L4H_MA12','EPM_SN00_L4H_MA12','EXC_SN00_L4H_MA12','SLW_SN00_L4H_MA12',
                 'VSLW_SN00_L4H_MA12',
                 'ART_SN00_L5H_MA12','ART_SN01_L5H_MA12','ART_SN02_L5H_MA12','ART_SN03_L5H_MA12',
                 'ART_SN04_L5H_MA12','ART_SN05_L5H_MA12','ART_SN06_L5H_MA12','ART_SNCS_L5H_MA12',
                 'CPM_SN00_L5H_MA12','EPM_SN00_L5H_MA12','EXC_SN00_L5H_MA12','SLW_SN00_L5H_MA12',
                 'VSLW_SN00_L5H_MA12',
                 'ART_SN00_L6H_MA12','ART_SN01_L6H_MA12','ART_SN02_L6H_MA12','ART_SN03_L6H_MA12',
                 'ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12','ART_SNCS_L6H_MA12',
                 'CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
                 'VSLW_SN00_L6H_MA12']

        #data = data[:-6]
        forecast_hours_ind=res.loc[(res.index>min_start_ts) & (res.index<=max_end_ts+datetime.timedelta(hours=float(hour_id[0])-6))].index

        X_val = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].values
        X_val_ind = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].index
        y_val = final_data.loc[forecast_hours_ind,'ART_SN00'].values
        y_val_ind = forecast_hours_ind

        #extract latest model version and load
        modelname = pd.read_sql(f"select modelname from model_versions a where modeltype='{hour_id}' and etl_insertion_dt= \
                    (select max(etl_insertion_dt) from model_versions b where a.modeltype=b.modeltype)",mydb)

        modelname = list(modelname.modelname)[0]
        model=pickle.load(open("./art_forecasting/models/"+modelname,"rb"))

        val_prediction = pd.DataFrame({
                                       f'pred_{hour_id}_window':model.predict(X_val)},index=y_val_ind)
        #concat all
        predicted_df = pd.merge(predicted_df,val_prediction,"outer",left_index=True,right_index=True,copy=False)
        logger.info(f"Model successfully ran for hour ID: {hour_id}")
        hour_id='2H'
        cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG',
                 'ART_SN00_L2H_MA12','ART_SN01_L2H_MA12',
                 'ART_SN02_L2H_MA12','ART_SN03_L2H_MA12','ART_SN04_L2H_MA12','ART_SN05_L2H_MA12','ART_SN06_L2H_MA12',
                 'ART_SNCS_L2H_MA12','CPM_SN00_L2H_MA12','EPM_SN00_L2H_MA12','EXC_SN00_L2H_MA12','SLW_SN00_L2H_MA12',
                 'VSLW_SN00_L2H_MA12',
                 'ART_SN00_L3H_MA12','ART_SN01_L3H_MA12',
                 'ART_SN02_L3H_MA12','ART_SN03_L3H_MA12','ART_SN04_L3H_MA12','ART_SN05_L3H_MA12','ART_SN06_L3H_MA12',
                 'ART_SNCS_L3H_MA12','CPM_SN00_L3H_MA12','EPM_SN00_L3H_MA12','EXC_SN00_L3H_MA12','SLW_SN00_L3H_MA12',
                 'VSLW_SN00_L3H_MA12',
                 'ART_SN00_L4H_MA12','ART_SN01_L4H_MA12','ART_SN02_L4H_MA12','ART_SN03_L4H_MA12',
                 'ART_SN04_L4H_MA12','ART_SN05_L4H_MA12','ART_SN06_L4H_MA12','ART_SNCS_L4H_MA12',
                 'CPM_SN00_L4H_MA12','EPM_SN00_L4H_MA12','EXC_SN00_L4H_MA12','SLW_SN00_L4H_MA12',
                 'VSLW_SN00_L4H_MA12',
                 'ART_SN00_L5H_MA12','ART_SN01_L5H_MA12','ART_SN02_L5H_MA12','ART_SN03_L5H_MA12',
                 'ART_SN04_L5H_MA12','ART_SN05_L5H_MA12','ART_SN06_L5H_MA12','ART_SNCS_L5H_MA12',
                 'CPM_SN00_L5H_MA12','EPM_SN00_L5H_MA12','EXC_SN00_L5H_MA12','SLW_SN00_L5H_MA12',
                 'VSLW_SN00_L5H_MA12',
                 'ART_SN00_L6H_MA12','ART_SN01_L6H_MA12','ART_SN02_L6H_MA12','ART_SN03_L6H_MA12',
                 'ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12','ART_SNCS_L6H_MA12',
                 'CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
                 'VSLW_SN00_L6H_MA12']

        #data = data[:-6]

        forecast_hours_ind=res.loc[(res.index>min_start_ts) & (res.index<=max_end_ts+datetime.timedelta(hours=float(hour_id[0])-6))].index

        X_val = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].values
        X_val_ind = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].index
        y_val = final_data.loc[forecast_hours_ind,'ART_SN00'].values
        y_val_ind = forecast_hours_ind

        #extract latest model version and load
        modelname = pd.read_sql(f"select modelname from model_versions a where modeltype='{hour_id}' and etl_insertion_dt= \
                    (select max(etl_insertion_dt) from model_versions b where a.modeltype=b.modeltype)",mydb)

        modelname = list(modelname.modelname)[0]
        model=pickle.load(open("./art_forecasting/models/"+modelname,"rb"))

        val_prediction = pd.DataFrame({
                                       f'pred_{hour_id}_window':model.predict(X_val)},index=y_val_ind)
        #concat all
        predicted_df = pd.merge(predicted_df,val_prediction,"outer",left_index=True,right_index=True,copy=False)
        logger.info(f"Model successfully ran for hour ID: {hour_id}")
        #XGBoost Baseline for 1 hour window prediction
        hour_id = '1H'
        cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG',
                 'ART_SN00_L1H_MA3','ART_SN01_L1H_MA3',
                 'ART_SN02_L1H_MA3','ART_SN03_L1H_MA3','ART_SN04_L1H_MA3','ART_SN05_L1H_MA3','ART_SN06_L1H_MA3',
                 'ART_SNCS_L1H_MA3','CPM_SN00_L1H_MA3','EPM_SN00_L1H_MA3','EXC_SN00_L1H_MA3','SLW_SN00_L1H_MA3',
                 'VSLW_SN00_L1H_MA3',
                 'ART_SN00_L2H_MA3','ART_SN01_L2H_MA3',
                 'ART_SN02_L2H_MA3','ART_SN03_L2H_MA3','ART_SN04_L2H_MA3','ART_SN05_L2H_MA3','ART_SN06_L2H_MA3',
                 'ART_SNCS_L2H_MA3','CPM_SN00_L2H_MA3','EPM_SN00_L2H_MA3','EXC_SN00_L2H_MA3','SLW_SN00_L2H_MA3',
                 'VSLW_SN00_L2H_MA3',
                 'ART_SN00_L3H_MA3','ART_SN01_L3H_MA3',
                 'ART_SN02_L3H_MA3','ART_SN03_L3H_MA3','ART_SN04_L3H_MA3','ART_SN05_L3H_MA3','ART_SN06_L3H_MA3',
                 'ART_SNCS_L3H_MA3','CPM_SN00_L3H_MA3','EPM_SN00_L3H_MA3','EXC_SN00_L3H_MA3','SLW_SN00_L3H_MA3',
                 'VSLW_SN00_L3H_MA3',
                 'ART_SN00_L4H_MA3','ART_SN01_L4H_MA3','ART_SN02_L4H_MA3','ART_SN03_L4H_MA3',
                 'ART_SN04_L4H_MA3','ART_SN05_L4H_MA3','ART_SN06_L4H_MA3','ART_SNCS_L4H_MA3',
                 'CPM_SN00_L4H_MA3','EPM_SN00_L4H_MA3','EXC_SN00_L4H_MA3','SLW_SN00_L4H_MA3',
                 'VSLW_SN00_L4H_MA3',
                 'ART_SN00_L5H_MA3','ART_SN01_L5H_MA3','ART_SN02_L5H_MA3','ART_SN03_L5H_MA3',
                 'ART_SN04_L5H_MA3','ART_SN05_L5H_MA3','ART_SN06_L5H_MA3','ART_SNCS_L5H_MA3',
                 'CPM_SN00_L5H_MA3','EPM_SN00_L5H_MA3','EXC_SN00_L5H_MA3','SLW_SN00_L5H_MA3',
                 'VSLW_SN00_L5H_MA3',
                 'ART_SN00_L6H_MA3','ART_SN01_L6H_MA3','ART_SN02_L6H_MA3','ART_SN03_L6H_MA3',
                 'ART_SN04_L6H_MA3','ART_SN05_L6H_MA3','ART_SN06_L6H_MA3','ART_SNCS_L6H_MA3',
                 'CPM_SN00_L6H_MA3','EPM_SN00_L6H_MA3','EXC_SN00_L6H_MA3','SLW_SN00_L6H_MA3',
                 'VSLW_SN00_L6H_MA3']

        #data = data[:-6]

        forecast_hours_ind=res.loc[(res.index>min_start_ts) & (res.index<=max_end_ts+datetime.timedelta(hours=float(hour_id[0])-6))].index

        X_val = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].values
        X_val_ind = final_data.loc[forecast_hours_ind,cols].iloc[:, 1:].index
        y_val = final_data.loc[forecast_hours_ind,'ART_SN00'].values
        y_val_ind = forecast_hours_ind

        #extract latest model version and load
        modelname = pd.read_sql(f"select modelname from model_versions a where modeltype='{hour_id}' and etl_insertion_dt= \
                    (select max(etl_insertion_dt) from model_versions b where a.modeltype=b.modeltype)",mydb)

        modelname = list(modelname.modelname)[0]
        model=pickle.load(open("./art_forecasting/models/"+modelname,"rb"))

        val_prediction = pd.DataFrame({f'pred_{hour_id}_window':model.predict(X_val),
                                      'actual' : y_val},index=y_val_ind)
        #concat all
        predicted_df = pd.merge(predicted_df,val_prediction,"outer",left_index=True,right_index=True,copy=False)
        logger.info(f"Model successfully ran for hour ID: {hour_id}")
    except:
        logger.error(f"Error occured in predcitions for {hour_id} model. Exiting....")
        raise Exception(f"Error occured in predcitions for {hour_id} model: "+str(sys.exc_info()[1]))

    try:    
        predicted_df = pd.merge(predicted_df,res['PREDICTED_FLAG'],"inner",left_index=True,right_index=True)
        predicted_df = predicted_df.reset_index(drop=False)
        predicted_df['index'] = predicted_df['index'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        predicted_df = predicted_df[predicted_df.PREDICTED_FLAG=='N']

        #re-arrange cols
        predicted_df = predicted_df[['index', 'actual', 'pred_6H_window', 'pred_5H_window', 'pred_4H_window',
                                     'pred_3H_window', 'pred_2H_window', 'pred_1H_window','PREDICTED_FLAG']]
        #first part for Insert and other for Update params
        rec = pd.concat([predicted_df.iloc[:,:-1],predicted_df.iloc[:,1:-1]],axis=1) 

        rec.columns = ['index', 'actual', 'pred_6H_window', 'pred_5H_window', 'pred_4H_window',
                       'pred_3H_window', 'pred_2H_window', 'pred_1H_window','actual_1','pred_6H_window_1',
                       'pred_5H_window_1', 'pred_4H_window_1','pred_3H_window_1', 'pred_2H_window_1',
                       'pred_1H_window_1']  #rename cols as duplicate cols cannot exists in records
        rec = rec.replace({np.nan: None})  #convert nan to None as SQL only knows nulls
        rec = rec.to_records(index=False)
        rec = rec.tolist()  

        #upsert to predicted_df table
        add_recs =("INSERT INTO predicted_df (DATETIMESTAMP_ID,ART_SN00,pred_ART_6H,pred_ART_5H,pred_ART_4H, \
                    pred_ART_3H,pred_ART_2H,pred_ART_1H) "
                    f"VALUES (%s,CAST(%s AS DOUBLE(20,16)),CAST(%s AS DOUBLE(20,16)),CAST(%s AS DOUBLE(20,16)), \
                   CAST(%s AS DOUBLE(20,16)),CAST(%s AS DOUBLE(20,16)),CAST(%s AS DOUBLE(20,16)),CAST(%s AS DOUBLE(20,16))) \
                    ON DUPLICATE KEY UPDATE ART_SN00=IFNULL(ART_SN00,CAST(%s AS DOUBLE(20,16))),pred_ART_6H=IFNULL(pred_ART_6H,CAST(%s AS DOUBLE(20,16))),pred_ART_5H=IFNULL(pred_ART_5H,CAST(%s AS DOUBLE(20,16))), \
                    pred_ART_4H=IFNULL(pred_ART_4H,CAST(%s AS DOUBLE(20,16))),pred_ART_3H=IFNULL(pred_ART_3H,CAST(%s AS DOUBLE(20,16))), \
                    pred_ART_2H=IFNULL(pred_ART_2H,CAST(%s AS DOUBLE(20,16))), \
                    pred_ART_1H=IFNULL(pred_ART_1H,CAST(%s AS DOUBLE(20,16)))")
        for r in rec:
            mycursor.execute(add_recs,r)
        #mydb.commit()
    except:
        logger.error(f"Error occured in inserting/updating predictions in DB. Exiting....")
        raise Exception("Error occured in inserting/updating predictions in DB: "+str(sys.exc_info()[1]))

    logger.info("Insert/Update to predicted_df table completed!")

    #Set predicted_flag to Y to all those records which has been completely predicted for all 6 hours
    try:
        for t in tab_dict:
            mycursor.execute(f"update {t} t1,(SELECT distinct a.Datetimestamp_ID FROM predicted_df p,{t} a where a.datetimestamp_id=p.datetimestamp_id and a.predicted_flag='N' and \
                            p.ART_SN00 is not null and pred_ART_6H is not null and pred_ART_5H is not null \
                            and pred_ART_4H is not null and	pred_ART_3H is not null and	pred_ART_2H is not null	and pred_ART_1H is not null) t2 \
                            set t1.predicted_flag='Y' where t1.Datetimestamp_ID=t2.Datetimestamp_ID")
    except:
        logger.error(f"Error occured in setting predicted flag in table {t}. Exiting....")
        raise Exception(f"Error occured in setting predicted flag in table {t}: "+str(sys.exc_info()[1]))

    mydb.commit()
    logger.info("Update of Predicted_Flag to base tables completed!")
    logger.info("Prediction module executed successfully!")