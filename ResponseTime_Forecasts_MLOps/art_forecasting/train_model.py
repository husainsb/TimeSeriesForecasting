import os
import sys
os.chdir("/home/cdsw/")
from art_forecasting import *

current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
etl_insertion_dt = pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'))
current_time = datetime.now().strftime("%Y%m%d_%H%M")

#Function to perform XGBoost regression with Cross-validation
def xgb_regressor(config):
    config['n_estimators'] = int(config['n_estimators'])
    config['max_depth'] = int(config['max_depth'])
    config['min_child_weight'] = int(config['min_child_weight'])
    config['learning_rate'] = 10 ** config['learning_rate']
    config['subsample'] =config['subsample']/10
    config['colsample_bytree'] = config['colsample_bytree']/10
    
    xgb_01 = xgb.XGBRegressor(eval_metric="rmse",random_state= 123,n_jobs=-1,**config)
    
    cv_score = -cross_val_score(xgb_01,X_train, y_train,cv=5,scoring='neg_root_mean_squared_error')
    rmse=cv_score.mean()
    return {"loss": rmse, 'status':STATUS_OK,'model':xgb_01}

#Create session
mydb,mycursor = set_db_session()

logger.info(f"*****************************************")
logger.info(f"Model training script start: {etl_insertion_dt}")

try:
    res = pd.DataFrame()
    for t in main_tables:
        #suffix tab1 with 'summary' to get tab2
        tab2 = 'avg_response_time_summary_ms' if t=='avg_response_time_ms' else t+'_summary'
        
        df = combineNpivot(t,tab2,mydb)
        final_df = lag_transform(df,12,t)
        res = pd.concat([res,final_df],axis=1)
except:
    logger.error("Error occured in pivot/lag transform. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()

try:   
    FULL_REFINED = res[12:]
    ## Fill NAs using bfill and ffill
    FULL_REFINED.bfill(axis=0,inplace=True)
    FULL_REFINED.ffill(axis=0,inplace=True)

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
                    "CPM_SN05_L4H","CPM_SN06_L4H","CPM_SNCS_L4H","CPM_SN00_L5H","CPM_SN01_L5H",
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
                    
    FULL_REFINED_orig = FULL_REFINED.copy()
    FULL_REFINED = FULL_REFINED.loc[:,refined_cols]

    rec=[]
    for c in FULL_REFINED.columns:
        cutoff = calc_q3_whisker(FULL_REFINED[c])
        rec.append((c,round(cutoff,2)))
except:
    logger.error("Error occured in calculating max of whiskers. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
#Import into DB
try:
    mycursor.execute("truncate table columns_transform_specs")

    add_recs = ("INSERT INTO columns_transform_specs (colname,outlier_cutoff)"                
                f"VALUES (%s, %s)")

    mycursor.executemany(add_recs,rec)
except:
    logger.error("Error occured in inserting records in table columns_transform_specs. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
try:
    cols_cutoff = pd.read_sql("select * from columns_transform_specs",mydb)for c in FULL_REFINED.columns:
    cutoff = list(cols_cutoff.loc[cols_cutoff.colname==c,'outlier_cutoff'])[0]
    FULL_REFINED[c] = np.where(FULL_REFINED[c]<cutoff,FULL_REFINED[c],cutoff)

    FULL_REFINED['HR_VAL'] = FULL_REFINED.index.to_series().apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').hour)
    FULL_REFINED['DAY_OF_WEEK'] = FULL_REFINED.index.to_series().apply(lambda x: str(datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').isoweekday()))
    FULL_REFINED['WEEKEND_FLG'] = FULL_REFINED.DAY_OF_WEEK.apply(lambda x: '1' if ((x == '6') or (x == '7')) else '0')
    #Monday:1.... Sunday:7
    FULL_REFINED['ART_SN00_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN00_L6H'],12)
    FULL_REFINED['ART_SN00_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN00_L5H'],12)
    FULL_REFINED['ART_SN00_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN00_L4H'],12)
    FULL_REFINED['ART_SN00_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN00_L3H'],12)
    FULL_REFINED['ART_SN00_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN00_L2H'],12)
    FULL_REFINED['ART_SN00_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN00_L1H'],12)

    FULL_REFINED['ART_SN01_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN01_L6H'],12)
    FULL_REFINED['ART_SN01_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN01_L5H'],12)
    FULL_REFINED['ART_SN01_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN01_L4H'],12)
    FULL_REFINED['ART_SN01_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN01_L3H'],12)
    FULL_REFINED['ART_SN01_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN01_L2H'],12)
    FULL_REFINED['ART_SN01_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN01_L1H'],12)

    FULL_REFINED['ART_SN02_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN02_L6H'],12)
    FULL_REFINED['ART_SN02_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN02_L5H'],12)
    FULL_REFINED['ART_SN02_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN02_L4H'],12)
    FULL_REFINED['ART_SN02_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN02_L3H'],12)
    FULL_REFINED['ART_SN02_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN02_L2H'],12)
    FULL_REFINED['ART_SN02_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN02_L1H'],12)

    FULL_REFINED['ART_SN03_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN03_L6H'],12)
    FULL_REFINED['ART_SN03_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN03_L5H'],12)
    FULL_REFINED['ART_SN03_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN03_L4H'],12)
    FULL_REFINED['ART_SN03_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN03_L3H'],12)
    FULL_REFINED['ART_SN03_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN03_L2H'],12)
    FULL_REFINED['ART_SN03_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN03_L1H'],12)

    FULL_REFINED['ART_SN04_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN04_L6H'],12)
    FULL_REFINED['ART_SN04_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN04_L5H'],12)
    FULL_REFINED['ART_SN04_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN04_L4H'],12)
    FULL_REFINED['ART_SN04_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN04_L3H'],12)
    FULL_REFINED['ART_SN04_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN04_L2H'],12)
    FULL_REFINED['ART_SN04_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN04_L1H'],12)

    FULL_REFINED['ART_SN05_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN05_L6H'],12)
    FULL_REFINED['ART_SN05_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN05_L5H'],12)
    FULL_REFINED['ART_SN05_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN05_L4H'],12)
    FULL_REFINED['ART_SN05_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN05_L3H'],12)
    FULL_REFINED['ART_SN05_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN05_L2H'],12)
    FULL_REFINED['ART_SN05_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN05_L1H'],12)

    FULL_REFINED['ART_SN06_L6H_MA12'] = moving_average(FULL_REFINED['ART_SN06_L6H'],12)
    FULL_REFINED['ART_SN06_L5H_MA12'] = moving_average(FULL_REFINED['ART_SN06_L5H'],12)
    FULL_REFINED['ART_SN06_L4H_MA12'] = moving_average(FULL_REFINED['ART_SN06_L4H'],12)
    FULL_REFINED['ART_SN06_L3H_MA12'] = moving_average(FULL_REFINED['ART_SN06_L3H'],12)
    FULL_REFINED['ART_SN06_L2H_MA12'] = moving_average(FULL_REFINED['ART_SN06_L2H'],12)
    FULL_REFINED['ART_SN06_L1H_MA12'] = moving_average(FULL_REFINED['ART_SN06_L1H'],12)

    FULL_REFINED['ART_SNCS_L6H_MA12'] = moving_average(FULL_REFINED['ART_SNCS_L6H'],12)
    FULL_REFINED['ART_SNCS_L5H_MA12'] = moving_average(FULL_REFINED['ART_SNCS_L5H'],12)
    FULL_REFINED['ART_SNCS_L4H_MA12'] = moving_average(FULL_REFINED['ART_SNCS_L4H'],12)
    FULL_REFINED['ART_SNCS_L3H_MA12'] = moving_average(FULL_REFINED['ART_SNCS_L3H'],12)
    FULL_REFINED['ART_SNCS_L2H_MA12'] = moving_average(FULL_REFINED['ART_SNCS_L2H'],12)
    FULL_REFINED['ART_SNCS_L1H_MA12'] = moving_average(FULL_REFINED['ART_SNCS_L1H'],12)
    #
    FULL_REFINED['CPM_SN00_L6H_MA12'] = moving_average(FULL_REFINED['CPM_SN00_L6H'],12)
    FULL_REFINED['CPM_SN00_L5H_MA12'] = moving_average(FULL_REFINED['CPM_SN00_L5H'],12)
    FULL_REFINED['CPM_SN00_L4H_MA12'] = moving_average(FULL_REFINED['CPM_SN00_L4H'],12)
    FULL_REFINED['CPM_SN00_L3H_MA12'] = moving_average(FULL_REFINED['CPM_SN00_L3H'],12)
    FULL_REFINED['CPM_SN00_L2H_MA12'] = moving_average(FULL_REFINED['CPM_SN00_L2H'],12)
    FULL_REFINED['CPM_SN00_L1H_MA12'] = moving_average(FULL_REFINED['CPM_SN00_L1H'],12)

    FULL_REFINED['EPM_SN00_L6H_MA12'] = moving_average(FULL_REFINED['EPM_SN00_L6H'],12)
    FULL_REFINED['EPM_SN00_L5H_MA12'] = moving_average(FULL_REFINED['EPM_SN00_L5H'],12)
    FULL_REFINED['EPM_SN00_L4H_MA12'] = moving_average(FULL_REFINED['EPM_SN00_L4H'],12)
    FULL_REFINED['EPM_SN00_L3H_MA12'] = moving_average(FULL_REFINED['EPM_SN00_L3H'],12)
    FULL_REFINED['EPM_SN00_L2H_MA12'] = moving_average(FULL_REFINED['EPM_SN00_L2H'],12)
    FULL_REFINED['EPM_SN00_L1H_MA12'] = moving_average(FULL_REFINED['EPM_SN00_L1H'],12)

    FULL_REFINED['EXC_SN00_L6H_MA12'] = moving_average(FULL_REFINED['EXC_SN00_L6H'],12)
    FULL_REFINED['EXC_SN00_L5H_MA12'] = moving_average(FULL_REFINED['EXC_SN00_L5H'],12)
    FULL_REFINED['EXC_SN00_L4H_MA12'] = moving_average(FULL_REFINED['EXC_SN00_L4H'],12)
    FULL_REFINED['EXC_SN00_L3H_MA12'] = moving_average(FULL_REFINED['EXC_SN00_L3H'],12)
    FULL_REFINED['EXC_SN00_L2H_MA12'] = moving_average(FULL_REFINED['EXC_SN00_L2H'],12)
    FULL_REFINED['EXC_SN00_L1H_MA12'] = moving_average(FULL_REFINED['EXC_SN00_L1H'],12)

    FULL_REFINED['SLW_SN00_L6H_MA12'] = moving_average(FULL_REFINED['SLW_SN00_L6H'],12)
    FULL_REFINED['SLW_SN00_L5H_MA12'] = moving_average(FULL_REFINED['SLW_SN00_L5H'],12)
    FULL_REFINED['SLW_SN00_L4H_MA12'] = moving_average(FULL_REFINED['SLW_SN00_L4H'],12)
    FULL_REFINED['SLW_SN00_L3H_MA12'] = moving_average(FULL_REFINED['SLW_SN00_L3H'],12)
    FULL_REFINED['SLW_SN00_L2H_MA12'] = moving_average(FULL_REFINED['SLW_SN00_L2H'],12)
    FULL_REFINED['SLW_SN00_L1H_MA12'] = moving_average(FULL_REFINED['SLW_SN00_L1H'],12)

    FULL_REFINED['VSLW_SN00_L6H_MA12'] = moving_average(FULL_REFINED['VSLW_SN00_L6H'],12)
    FULL_REFINED['VSLW_SN00_L5H_MA12'] = moving_average(FULL_REFINED['VSLW_SN00_L5H'],12)
    FULL_REFINED['VSLW_SN00_L4H_MA12'] = moving_average(FULL_REFINED['VSLW_SN00_L4H'],12)
    FULL_REFINED['VSLW_SN00_L3H_MA12'] = moving_average(FULL_REFINED['VSLW_SN00_L3H'],12)
    FULL_REFINED['VSLW_SN00_L2H_MA12'] = moving_average(FULL_REFINED['VSLW_SN00_L2H'],12)
    FULL_REFINED['VSLW_SN00_L1H_MA12'] = moving_average(FULL_REFINED['VSLW_SN00_L1H'],12)

    ######################

    FULL_REFINED['ART_SN00_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN00_L6H'],3)
    FULL_REFINED['ART_SN00_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN00_L5H'],3)
    FULL_REFINED['ART_SN00_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN00_L4H'],3)
    FULL_REFINED['ART_SN00_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN00_L3H'],3)
    FULL_REFINED['ART_SN00_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN00_L2H'],3)
    FULL_REFINED['ART_SN00_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN00_L1H'],3)

    FULL_REFINED['ART_SN01_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN01_L6H'],3)
    FULL_REFINED['ART_SN01_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN01_L5H'],3)
    FULL_REFINED['ART_SN01_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN01_L4H'],3)
    FULL_REFINED['ART_SN01_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN01_L3H'],3)
    FULL_REFINED['ART_SN01_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN01_L2H'],3)
    FULL_REFINED['ART_SN01_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN01_L1H'],3)

    FULL_REFINED['ART_SN02_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN02_L6H'],3)
    FULL_REFINED['ART_SN02_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN02_L5H'],3)
    FULL_REFINED['ART_SN02_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN02_L4H'],3)
    FULL_REFINED['ART_SN02_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN02_L3H'],3)
    FULL_REFINED['ART_SN02_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN02_L2H'],3)
    FULL_REFINED['ART_SN02_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN02_L1H'],3)

    FULL_REFINED['ART_SN03_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN03_L6H'],3)
    FULL_REFINED['ART_SN03_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN03_L5H'],3)
    FULL_REFINED['ART_SN03_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN03_L4H'],3)
    FULL_REFINED['ART_SN03_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN03_L3H'],3)
    FULL_REFINED['ART_SN03_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN03_L2H'],3)
    FULL_REFINED['ART_SN03_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN03_L1H'],3)

    FULL_REFINED['ART_SN04_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN04_L6H'],3)
    FULL_REFINED['ART_SN04_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN04_L5H'],3)
    FULL_REFINED['ART_SN04_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN04_L4H'],3)
    FULL_REFINED['ART_SN04_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN04_L3H'],3)
    FULL_REFINED['ART_SN04_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN04_L2H'],3)
    FULL_REFINED['ART_SN04_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN04_L1H'],3)

    FULL_REFINED['ART_SN05_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN05_L6H'],3)
    FULL_REFINED['ART_SN05_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN05_L5H'],3)
    FULL_REFINED['ART_SN05_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN05_L4H'],3)
    FULL_REFINED['ART_SN05_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN05_L3H'],3)
    FULL_REFINED['ART_SN05_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN05_L2H'],3)
    FULL_REFINED['ART_SN05_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN05_L1H'],3)

    FULL_REFINED['ART_SN06_L6H_MA3'] = moving_average(FULL_REFINED['ART_SN06_L6H'],3)
    FULL_REFINED['ART_SN06_L5H_MA3'] = moving_average(FULL_REFINED['ART_SN06_L5H'],3)
    FULL_REFINED['ART_SN06_L4H_MA3'] = moving_average(FULL_REFINED['ART_SN06_L4H'],3)
    FULL_REFINED['ART_SN06_L3H_MA3'] = moving_average(FULL_REFINED['ART_SN06_L3H'],3)
    FULL_REFINED['ART_SN06_L2H_MA3'] = moving_average(FULL_REFINED['ART_SN06_L2H'],3)
    FULL_REFINED['ART_SN06_L1H_MA3'] = moving_average(FULL_REFINED['ART_SN06_L1H'],3)

    FULL_REFINED['ART_SNCS_L6H_MA3'] = moving_average(FULL_REFINED['ART_SNCS_L6H'],3)
    FULL_REFINED['ART_SNCS_L5H_MA3'] = moving_average(FULL_REFINED['ART_SNCS_L5H'],3)
    FULL_REFINED['ART_SNCS_L4H_MA3'] = moving_average(FULL_REFINED['ART_SNCS_L4H'],3)
    FULL_REFINED['ART_SNCS_L3H_MA3'] = moving_average(FULL_REFINED['ART_SNCS_L3H'],3)
    FULL_REFINED['ART_SNCS_L2H_MA3'] = moving_average(FULL_REFINED['ART_SNCS_L2H'],3)
    FULL_REFINED['ART_SNCS_L1H_MA3'] = moving_average(FULL_REFINED['ART_SNCS_L1H'],3)
    #
    FULL_REFINED['CPM_SN00_L6H_MA3'] = moving_average(FULL_REFINED['CPM_SN00_L6H'],3)
    FULL_REFINED['CPM_SN00_L5H_MA3'] = moving_average(FULL_REFINED['CPM_SN00_L5H'],3)
    FULL_REFINED['CPM_SN00_L4H_MA3'] = moving_average(FULL_REFINED['CPM_SN00_L4H'],3)
    FULL_REFINED['CPM_SN00_L3H_MA3'] = moving_average(FULL_REFINED['CPM_SN00_L3H'],3)
    FULL_REFINED['CPM_SN00_L2H_MA3'] = moving_average(FULL_REFINED['CPM_SN00_L2H'],3)
    FULL_REFINED['CPM_SN00_L1H_MA3'] = moving_average(FULL_REFINED['CPM_SN00_L1H'],3)

    FULL_REFINED['EPM_SN00_L6H_MA3'] = moving_average(FULL_REFINED['EPM_SN00_L6H'],3)
    FULL_REFINED['EPM_SN00_L5H_MA3'] = moving_average(FULL_REFINED['EPM_SN00_L5H'],3)
    FULL_REFINED['EPM_SN00_L4H_MA3'] = moving_average(FULL_REFINED['EPM_SN00_L4H'],3)
    FULL_REFINED['EPM_SN00_L3H_MA3'] = moving_average(FULL_REFINED['EPM_SN00_L3H'],3)
    FULL_REFINED['EPM_SN00_L2H_MA3'] = moving_average(FULL_REFINED['EPM_SN00_L2H'],3)
    FULL_REFINED['EPM_SN00_L1H_MA3'] = moving_average(FULL_REFINED['EPM_SN00_L1H'],3)

    FULL_REFINED['EXC_SN00_L6H_MA3'] = moving_average(FULL_REFINED['EXC_SN00_L6H'],3)
    FULL_REFINED['EXC_SN00_L5H_MA3'] = moving_average(FULL_REFINED['EXC_SN00_L5H'],3)
    FULL_REFINED['EXC_SN00_L4H_MA3'] = moving_average(FULL_REFINED['EXC_SN00_L4H'],3)
    FULL_REFINED['EXC_SN00_L3H_MA3'] = moving_average(FULL_REFINED['EXC_SN00_L3H'],3)
    FULL_REFINED['EXC_SN00_L2H_MA3'] = moving_average(FULL_REFINED['EXC_SN00_L2H'],3)
    FULL_REFINED['EXC_SN00_L1H_MA3'] = moving_average(FULL_REFINED['EXC_SN00_L1H'],3)

    FULL_REFINED['SLW_SN00_L6H_MA3'] = moving_average(FULL_REFINED['SLW_SN00_L6H'],3)
    FULL_REFINED['SLW_SN00_L5H_MA3'] = moving_average(FULL_REFINED['SLW_SN00_L5H'],3)
    FULL_REFINED['SLW_SN00_L4H_MA3'] = moving_average(FULL_REFINED['SLW_SN00_L4H'],3)
    FULL_REFINED['SLW_SN00_L3H_MA3'] = moving_average(FULL_REFINED['SLW_SN00_L3H'],3)
    FULL_REFINED['SLW_SN00_L2H_MA3'] = moving_average(FULL_REFINED['SLW_SN00_L2H'],3)
    FULL_REFINED['SLW_SN00_L1H_MA3'] = moving_average(FULL_REFINED['SLW_SN00_L1H'],3)

    FULL_REFINED['VSLW_SN00_L6H_MA3'] = moving_average(FULL_REFINED['VSLW_SN00_L6H'],3)
    FULL_REFINED['VSLW_SN00_L5H_MA3'] = moving_average(FULL_REFINED['VSLW_SN00_L5H'],3)
    FULL_REFINED['VSLW_SN00_L4H_MA3'] = moving_average(FULL_REFINED['VSLW_SN00_L4H'],3)
    FULL_REFINED['VSLW_SN00_L3H_MA3'] = moving_average(FULL_REFINED['VSLW_SN00_L3H'],3)
    FULL_REFINED['VSLW_SN00_L2H_MA3'] = moving_average(FULL_REFINED['VSLW_SN00_L2H'],3)
    FULL_REFINED['VSLW_SN00_L1H_MA3'] = moving_average(FULL_REFINED['VSLW_SN00_L1H'],3)
except:
    logger.error("Error occured in feature engineering. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#feature scaling
try:
    standard_scaler = sklearn.preprocessing.StandardScaler()
    num_data = FULL_REFINED.drop(columns=['ART_SN00','DAY_OF_WEEK','WEEKEND_FLG'])
    standard_scaler.fit(num_data)
    #dump scaler
    pickle.dump(standard_scaler, open('./art_forecasting/XGB Models Params/scaler.pkl', 'wb'))
    data_scaled = pd.DataFrame(standard_scaler.transform(num_data))
    data_scaled.columns = num_data.columns
    data_scaled.index=num_data.index
except:
    logger.error("Error occured in scaling the features. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
try:    
    final_data = pd.concat([FULL_REFINED[['ART_SN00','DAY_OF_WEEK', 'WEEKEND_FLG']],data_scaled],axis=1)

    #validation data set
    validation_data = final_data.tail(6)
    final_data = final_data[:-6]

    transformed_df = final_data.reset_index(drop=False)
    transformed_df['index'] = transformed_df['index'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    rec = transformed_df.to_records(index=False)
    rec = rec.tolist()

    add_recs =("INSERT INTO transformed_df (DATETIMESTAMP_ID,ART_SN00,DAY_OF_WEEK,WEEKEND_FLG,ART_SN01,ART_SN02,ART_SN03,ART_SN04,ART_SN05,ART_SN06, \
    ART_SNCS,ART_SN00_L1H,ART_SN01_L1H,ART_SN02_L1H,ART_SN03_L1H,ART_SN04_L1H,ART_SN05_L1H, \
    ART_SN06_L1H,ART_SNCS_L1H,ART_SN00_L2H,ART_SN01_L2H,ART_SN02_L2H,ART_SN03_L2H,ART_SN04_L2H, \
    ART_SN05_L2H,ART_SN06_L2H,ART_SNCS_L2H,ART_SN00_L3H,ART_SN01_L3H,ART_SN02_L3H,ART_SN03_L3H, \
    ART_SN04_L3H,ART_SN05_L3H,ART_SN06_L3H,ART_SNCS_L3H,ART_SN00_L4H,ART_SN01_L4H,ART_SN02_L4H, \
    ART_SN03_L4H,ART_SN04_L4H,ART_SN05_L4H,ART_SN06_L4H,ART_SNCS_L4H,ART_SN00_L5H,ART_SN01_L5H, \
    ART_SN02_L5H,ART_SN03_L5H,ART_SN04_L5H,ART_SN05_L5H,ART_SN06_L5H,ART_SNCS_L5H,ART_SN00_L6H, \
    ART_SN01_L6H,ART_SN02_L6H,ART_SN03_L6H,ART_SN04_L6H,ART_SN05_L6H,ART_SN06_L6H,ART_SNCS_L6H, \
    CPM_SN00,CPM_SN01,CPM_SN02,CPM_SN03,CPM_SN04,CPM_SN05,CPM_SN06,CPM_SNCS,CPM_SN00_L1H, \
    CPM_SN01_L1H,CPM_SN02_L1H,CPM_SN03_L1H,CPM_SN04_L1H,CPM_SN05_L1H,CPM_SN06_L1H,CPM_SNCS_L1H, \
    CPM_SN00_L2H,CPM_SN01_L2H,CPM_SN02_L2H,CPM_SN03_L2H,CPM_SN04_L2H,CPM_SN05_L2H,CPM_SN06_L2H, \
    CPM_SNCS_L2H,CPM_SN00_L3H,CPM_SN01_L3H,CPM_SN02_L3H,CPM_SN03_L3H,CPM_SN04_L3H,CPM_SN05_L3H, \
    CPM_SN06_L3H,CPM_SNCS_L3H,CPM_SN00_L4H,CPM_SN01_L4H,CPM_SN02_L4H,CPM_SN03_L4H,CPM_SN04_L4H, \
    CPM_SN05_L4H,CPM_SN06_L4H,CPM_SNCS_L4H,CPM_SN00_L5H,CPM_SN01_L5H,CPM_SN02_L5H,CPM_SN03_L5H, \
    CPM_SN04_L5H,CPM_SN05_L5H,CPM_SN06_L5H,CPM_SNCS_L5H,CPM_SN00_L6H,CPM_SN01_L6H,CPM_SN02_L6H, \
    CPM_SN03_L6H,CPM_SN04_L6H,CPM_SN05_L6H,CPM_SN06_L6H,CPM_SNCS_L6H,EPM_SN00,EPM_SN01,EPM_SN02, \
    EPM_SN03,EPM_SN04,EPM_SN00_L1H,EPM_SN01_L1H,EPM_SN02_L1H,EPM_SN03_L1H,EPM_SN04_L1H,EPM_SN00_L2H, \
    EPM_SN01_L2H,EPM_SN02_L2H,EPM_SN03_L2H,EPM_SN04_L2H,EPM_SN00_L3H,EPM_SN01_L3H,EPM_SN02_L3H, \
    EPM_SN03_L3H,EPM_SN04_L3H,EPM_SN00_L4H,EPM_SN01_L4H,EPM_SN02_L4H,EPM_SN03_L4H,EPM_SN04_L4H, \
    EPM_SN00_L5H,EPM_SN01_L5H,EPM_SN02_L5H,EPM_SN03_L5H,EPM_SN04_L5H,EPM_SN00_L6H,EPM_SN01_L6H, \
    EPM_SN02_L6H,EPM_SN03_L6H,EPM_SN04_L6H,EXC_SN00,EXC_SN01,EXC_SN02,EXC_SN03,EXC_SN04, \
    EXC_SN00_L1H,EXC_SN01_L1H,EXC_SN02_L1H,EXC_SN03_L1H,EXC_SN04_L1H,EXC_SN00_L2H,EXC_SN01_L2H, \
    EXC_SN02_L2H,EXC_SN03_L2H,EXC_SN04_L2H,EXC_SN00_L3H,EXC_SN01_L3H,EXC_SN02_L3H,EXC_SN03_L3H, \
    EXC_SN04_L3H,EXC_SN00_L4H,EXC_SN01_L4H,EXC_SN02_L4H,EXC_SN03_L4H,EXC_SN04_L4H,EXC_SN00_L5H, \
    EXC_SN01_L5H,EXC_SN02_L5H,EXC_SN03_L5H,EXC_SN04_L5H,EXC_SN00_L6H,EXC_SN01_L6H,EXC_SN02_L6H, \
    EXC_SN03_L6H,EXC_SN04_L6H,SLW_SN00,SLW_SN01,SLW_SN02,SLW_SN03,SLW_SN04,SLW_SN00_L1H, \
    SLW_SN01_L1H,SLW_SN02_L1H,SLW_SN03_L1H,SLW_SN04_L1H,SLW_SN00_L2H,SLW_SN01_L2H,SLW_SN02_L2H, \
    SLW_SN03_L2H,SLW_SN04_L2H,SLW_SN00_L3H,SLW_SN01_L3H,SLW_SN02_L3H,SLW_SN03_L3H,SLW_SN04_L3H, \
    SLW_SN00_L4H,SLW_SN01_L4H,SLW_SN02_L4H,SLW_SN03_L4H,SLW_SN04_L4H,SLW_SN00_L5H,SLW_SN01_L5H, \
    SLW_SN02_L5H,SLW_SN03_L5H,SLW_SN04_L5H,SLW_SN00_L6H,SLW_SN01_L6H,SLW_SN02_L6H,SLW_SN03_L6H, \
    SLW_SN04_L6H,VSLW_SN00,VSLW_SN01,VSLW_SN02,VSLW_SN03,VSLW_SN04,VSLW_SN00_L1H,VSLW_SN01_L1H, \
    VSLW_SN02_L1H,VSLW_SN03_L1H,VSLW_SN04_L1H,VSLW_SN00_L2H,VSLW_SN01_L2H,VSLW_SN02_L2H, \
    VSLW_SN03_L2H,VSLW_SN04_L2H,VSLW_SN00_L3H,VSLW_SN01_L3H,VSLW_SN02_L3H,VSLW_SN03_L3H, \
    VSLW_SN04_L3H,VSLW_SN00_L4H,VSLW_SN01_L4H,VSLW_SN02_L4H,VSLW_SN03_L4H,VSLW_SN04_L4H, \
    VSLW_SN00_L5H,VSLW_SN01_L5H,VSLW_SN02_L5H,VSLW_SN03_L5H,VSLW_SN04_L5H,VSLW_SN00_L6H, \
    VSLW_SN01_L6H,VSLW_SN02_L6H,VSLW_SN03_L6H,VSLW_SN04_L6H,HR_VAL,ART_SN00_L6H_MA12, \
    ART_SN00_L5H_MA12,ART_SN00_L4H_MA12,ART_SN00_L3H_MA12,ART_SN00_L2H_MA12,ART_SN00_L1H_MA12, \
    ART_SN01_L6H_MA12,ART_SN01_L5H_MA12,ART_SN01_L4H_MA12,ART_SN01_L3H_MA12,ART_SN01_L2H_MA12, \
    ART_SN01_L1H_MA12,ART_SN02_L6H_MA12,ART_SN02_L5H_MA12,ART_SN02_L4H_MA12,ART_SN02_L3H_MA12, \
    ART_SN02_L2H_MA12,ART_SN02_L1H_MA12,ART_SN03_L6H_MA12,ART_SN03_L5H_MA12,ART_SN03_L4H_MA12, \
    ART_SN03_L3H_MA12,ART_SN03_L2H_MA12,ART_SN03_L1H_MA12,ART_SN04_L6H_MA12,ART_SN04_L5H_MA12, \
    ART_SN04_L4H_MA12,ART_SN04_L3H_MA12,ART_SN04_L2H_MA12,ART_SN04_L1H_MA12,ART_SN05_L6H_MA12, \
    ART_SN05_L5H_MA12,ART_SN05_L4H_MA12,ART_SN05_L3H_MA12,ART_SN05_L2H_MA12,ART_SN05_L1H_MA12, \
    ART_SN06_L6H_MA12,ART_SN06_L5H_MA12,ART_SN06_L4H_MA12,ART_SN06_L3H_MA12,ART_SN06_L2H_MA12, \
    ART_SN06_L1H_MA12,ART_SNCS_L6H_MA12,ART_SNCS_L5H_MA12,ART_SNCS_L4H_MA12,ART_SNCS_L3H_MA12, \
    ART_SNCS_L2H_MA12,ART_SNCS_L1H_MA12,CPM_SN00_L6H_MA12,CPM_SN00_L5H_MA12,CPM_SN00_L4H_MA12, \
    CPM_SN00_L3H_MA12,CPM_SN00_L2H_MA12,CPM_SN00_L1H_MA12,EPM_SN00_L6H_MA12,EPM_SN00_L5H_MA12, \
    EPM_SN00_L4H_MA12,EPM_SN00_L3H_MA12,EPM_SN00_L2H_MA12,EPM_SN00_L1H_MA12,EXC_SN00_L6H_MA12, \
    EXC_SN00_L5H_MA12,EXC_SN00_L4H_MA12,EXC_SN00_L3H_MA12,EXC_SN00_L2H_MA12,EXC_SN00_L1H_MA12, \
    SLW_SN00_L6H_MA12,SLW_SN00_L5H_MA12,SLW_SN00_L4H_MA12,SLW_SN00_L3H_MA12,SLW_SN00_L2H_MA12, \
    SLW_SN00_L1H_MA12,VSLW_SN00_L6H_MA12,VSLW_SN00_L5H_MA12,VSLW_SN00_L4H_MA12,VSLW_SN00_L3H_MA12, \
    VSLW_SN00_L2H_MA12,VSLW_SN00_L1H_MA12,ART_SN00_L6H_MA3,ART_SN00_L5H_MA3,ART_SN00_L4H_MA3, \
    ART_SN00_L3H_MA3,ART_SN00_L2H_MA3,ART_SN00_L1H_MA3,ART_SN01_L6H_MA3,ART_SN01_L5H_MA3, \
    ART_SN01_L4H_MA3,ART_SN01_L3H_MA3,ART_SN01_L2H_MA3,ART_SN01_L1H_MA3,ART_SN02_L6H_MA3, \
    ART_SN02_L5H_MA3,ART_SN02_L4H_MA3,ART_SN02_L3H_MA3,ART_SN02_L2H_MA3,ART_SN02_L1H_MA3, \
    ART_SN03_L6H_MA3,ART_SN03_L5H_MA3,ART_SN03_L4H_MA3,ART_SN03_L3H_MA3,ART_SN03_L2H_MA3, \
    ART_SN03_L1H_MA3,ART_SN04_L6H_MA3,ART_SN04_L5H_MA3,ART_SN04_L4H_MA3,ART_SN04_L3H_MA3, \
    ART_SN04_L2H_MA3,ART_SN04_L1H_MA3,ART_SN05_L6H_MA3,ART_SN05_L5H_MA3,ART_SN05_L4H_MA3, \
    ART_SN05_L3H_MA3,ART_SN05_L2H_MA3,ART_SN05_L1H_MA3,ART_SN06_L6H_MA3,ART_SN06_L5H_MA3, \
    ART_SN06_L4H_MA3,ART_SN06_L3H_MA3,ART_SN06_L2H_MA3,ART_SN06_L1H_MA3,ART_SNCS_L6H_MA3, \
    ART_SNCS_L5H_MA3,ART_SNCS_L4H_MA3,ART_SNCS_L3H_MA3,ART_SNCS_L2H_MA3,ART_SNCS_L1H_MA3, \
    CPM_SN00_L6H_MA3,CPM_SN00_L5H_MA3,CPM_SN00_L4H_MA3,CPM_SN00_L3H_MA3,CPM_SN00_L2H_MA3, \
    CPM_SN00_L1H_MA3,EPM_SN00_L6H_MA3,EPM_SN00_L5H_MA3,EPM_SN00_L4H_MA3,EPM_SN00_L3H_MA3, \
    EPM_SN00_L2H_MA3,EPM_SN00_L1H_MA3,EXC_SN00_L6H_MA3,EXC_SN00_L5H_MA3,EXC_SN00_L4H_MA3, \
    EXC_SN00_L3H_MA3,EXC_SN00_L2H_MA3,EXC_SN00_L1H_MA3,SLW_SN00_L6H_MA3,SLW_SN00_L5H_MA3, \
    SLW_SN00_L4H_MA3,SLW_SN00_L3H_MA3,SLW_SN00_L2H_MA3,SLW_SN00_L1H_MA3,VSLW_SN00_L6H_MA3, \
    VSLW_SN00_L5H_MA3,VSLW_SN00_L4H_MA3,VSLW_SN00_L3H_MA3,VSLW_SN00_L2H_MA3,VSLW_SN00_L1H_MA3) "
    f"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )

    mycursor.executemany(add_recs,rec)
except:
    logger.error("Error occured in inserting records in table trasnformed_df. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#XGBoost 6 hour model training
try:
    gc.collect()
    cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG','ART_SN00_L6H_MA12','ART_SN01_L6H_MA12',
            'ART_SN02_L6H_MA12','ART_SN03_L6H_MA12','ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12',
            'ART_SNCS_L6H_MA12','CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
            'VSLW_SN00_L6H_MA12']
            
    train, test = train_test_split(final_data[cols], test_size=0.2, random_state=1546)
    X_train = train.iloc[:, 1:].values
    X_train_ind = train.iloc[:, 1:].index
    y_train = train.ART_SN00.values
    y_train_ind = train.ART_SN00.index

    X_test = test.iloc[:, 1:].values
    X_test_ind = test.iloc[:, 1:].index
    y_test = test.ART_SN00.values
    y_test_ind = test.ART_SN00.index

    X_val = validation_data[cols].iloc[:, 1:].values
    X_val_ind = validation_data[cols].iloc[:, 1:].index
    y_val = validation_data.ART_SN00.values
    y_val_ind = validation_data.ART_SN00.index

    #Bayes Optimization
    trials = Trials()  #uncommented to refer to older trials
    best = fmin(fn=xgb_regressor,
                space=params,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials,verbose=False,trials_save_file=f'./art_forecasting/XGB Models Params/6H model/trials_6H_{current_time}.p')

    best['n_estimators'] = int(best['n_estimators'])
    best['max_depth'] = int(best['max_depth'])
    best['min_child_weight'] = int(best['min_child_weight'])
    best['learning_rate'] = 10 ** best['learning_rate']
    best['subsample'] =best['subsample']/10
    best['colsample_bytree'] = best['colsample_bytree']/10

    xgb_6h_final = xgb.XGBRegressor(base_score=None,eval_metric='rmse',importance_type='gain',n_jobs=-1,random_state=123,verbosity=0,
                                   **best)
    xgb_6h_final.fit(X_train,y_train)

    #store model file and metrics in DB 
    modelname = store_model(xgb_6h_final,'6H',current_time,str(etl_insertion_dt),mycursor)

    track_metrics(modelname,'6H',xgb_6h_final,X_train,y_train,mycursor,str(etl_insertion_dt),'train set')
    track_metrics(modelname,'6H',xgb_6h_final,X_test,y_test,mycursor,str(etl_insertion_dt),'baseline test')
    track_metrics(modelname,'6H',xgb_6h_final,X_val,y_val,mycursor,str(etl_insertion_dt),'baseline validation')
except:
    logger.error("Error occured in training 6H model. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#XGBoost 5 hour model training
try:
    gc.collect()
    cols = ['ART_SN00','HR_VAL','DAY_OF_WEEK','WEEKEND_FLG','ART_SN00_L5H_MA12','ART_SN01_L5H_MA12',
             'ART_SN02_L5H_MA12','ART_SN03_L5H_MA12','ART_SN04_L5H_MA12','ART_SN05_L5H_MA12','ART_SN06_L5H_MA12',
             'ART_SNCS_L5H_MA12','CPM_SN00_L5H_MA12','EPM_SN00_L5H_MA12','EXC_SN00_L5H_MA12','SLW_SN00_L5H_MA12',
             'VSLW_SN00_L5H_MA12','ART_SN00_L6H_MA12','ART_SN01_L6H_MA12','ART_SN02_L6H_MA12',
             'ART_SN03_L6H_MA12','ART_SN04_L6H_MA12','ART_SN05_L6H_MA12','ART_SN06_L6H_MA12',
             'ART_SNCS_L6H_MA12','CPM_SN00_L6H_MA12','EPM_SN00_L6H_MA12','EXC_SN00_L6H_MA12','SLW_SN00_L6H_MA12',
             'VSLW_SN00_L6H_MA12']
            
    train, test = train_test_split(final_data[cols], test_size=0.2, random_state=1546)
    X_train = train.iloc[:, 1:].values
    X_train_ind = train.iloc[:, 1:].index
    y_train = train.ART_SN00.values
    y_train_ind = train.ART_SN00.index

    X_test = test.iloc[:, 1:].values
    X_test_ind = test.iloc[:, 1:].index
    y_test = test.ART_SN00.values
    y_test_ind = test.ART_SN00.index

    X_val = validation_data[cols].iloc[:, 1:].values
    X_val_ind = validation_data[cols].iloc[:, 1:].index
    y_val = validation_data.ART_SN00.values
    y_val_ind = validation_data.ART_SN00.index

    #Bayes Optimization
    trials = Trials()  #uncommented to refer to older trials
    best = fmin(fn=xgb_regressor,
                space=params,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials,verbose=False,trials_save_file=f'./art_forecasting/XGB Models Params/5H model/trials_5H_{current_time}.p')

    best['n_estimators'] = int(best['n_estimators'])
    best['max_depth'] = int(best['max_depth'])
    best['min_child_weight'] = int(best['min_child_weight'])
    best['learning_rate'] = 10 ** best['learning_rate']
    best['subsample'] =best['subsample']/10
    best['colsample_bytree'] = best['colsample_bytree']/10

    xgb_5h_final = xgb.XGBRegressor(base_score=None,eval_metric='rmse',importance_type='gain',n_jobs=-1,random_state=123,verbosity=0,
                                   **best)
    xgb_5h_final.fit(X_train,y_train)

    #store model file and metrics in DB 
    modelname = store_model(xgb_5h_final,'5H',current_time,str(etl_insertion_dt),mycursor)

    track_metrics(modelname,'5H',xgb_5h_final,X_train,y_train,mycursor,str(etl_insertion_dt),'train set')
    track_metrics(modelname,'5H',xgb_5h_final,X_test,y_test,mycursor,str(etl_insertion_dt),'baseline test')
    track_metrics(modelname,'5H',xgb_5h_final,X_val,y_val,mycursor,str(etl_insertion_dt),'baseline validation')
except:
    logger.error("Error occured in training 5H model. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#XGBoost 4 hour model training
try:
    gc.collect()
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
            
    train, test = train_test_split(final_data[cols], test_size=0.2, random_state=1546)
    X_train = train.iloc[:, 1:].values
    X_train_ind = train.iloc[:, 1:].index
    y_train = train.ART_SN00.values
    y_train_ind = train.ART_SN00.index

    X_test = test.iloc[:, 1:].values
    X_test_ind = test.iloc[:, 1:].index
    y_test = test.ART_SN00.values
    y_test_ind = test.ART_SN00.index

    X_val = validation_data[cols].iloc[:, 1:].values
    X_val_ind = validation_data[cols].iloc[:, 1:].index
    y_val = validation_data.ART_SN00.values
    y_val_ind = validation_data.ART_SN00.index

    #Bayes Optimization
    trials = Trials()  #uncommented to refer to older trials
    best = fmin(fn=xgb_regressor,
                space=params,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials,verbose=False,trials_save_file=f'./art_forecasting/XGB Models Params/4H model/trials_4H_{current_time}.p')

    best['n_estimators'] = int(best['n_estimators'])
    best['max_depth'] = int(best['max_depth'])
    best['min_child_weight'] = int(best['min_child_weight'])
    best['learning_rate'] = 10 ** best['learning_rate']
    best['subsample'] =best['subsample']/10
    best['colsample_bytree'] = best['colsample_bytree']/10

    xgb_4h_final = xgb.XGBRegressor(base_score=None,eval_metric='rmse',importance_type='gain',n_jobs=-1,random_state=123,verbosity=0,
                                   **best)
    xgb_4h_final.fit(X_train,y_train)

    #store model file and metrics in DB 
    modelname = store_model(xgb_4h_final,'4H',current_time,str(etl_insertion_dt),mycursor)

    track_metrics(modelname,'4H',xgb_4h_final,X_train,y_train,mycursor,str(etl_insertion_dt),'train set')
    track_metrics(modelname,'4H',xgb_4h_final,X_test,y_test,mycursor,str(etl_insertion_dt),'baseline test')
    track_metrics(modelname,'4H',xgb_4h_final,X_val,y_val,mycursor,str(etl_insertion_dt),'baseline validation')
except:
    logger.error("Error occured in training 4H model. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#XGBoost 3 hour model training
try:
    gc.collect()
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
            
    train, test = train_test_split(final_data[cols], test_size=0.2, random_state=1546)
    X_train = train.iloc[:, 1:].values
    X_train_ind = train.iloc[:, 1:].index
    y_train = train.ART_SN00.values
    y_train_ind = train.ART_SN00.index

    X_test = test.iloc[:, 1:].values
    X_test_ind = test.iloc[:, 1:].index
    y_test = test.ART_SN00.values
    y_test_ind = test.ART_SN00.index

    X_val = validation_data[cols].iloc[:, 1:].values
    X_val_ind = validation_data[cols].iloc[:, 1:].index
    y_val = validation_data.ART_SN00.values
    y_val_ind = validation_data.ART_SN00.index

    #Bayes Optimization
    trials = Trials()  #uncommented to refer to older trials
    best = fmin(fn=xgb_regressor,
                space=params,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials,verbose=False,trials_save_file=f'./art_forecasting/XGB Models Params/3H model/trials_3H_{current_time}.p')

    best['n_estimators'] = int(best['n_estimators'])
    best['max_depth'] = int(best['max_depth'])
    best['min_child_weight'] = int(best['min_child_weight'])
    best['learning_rate'] = 10 ** best['learning_rate']
    best['subsample'] =best['subsample']/10
    best['colsample_bytree'] = best['colsample_bytree']/10

    xgb_3h_final = xgb.XGBRegressor(base_score=None,eval_metric='rmse',importance_type='gain',n_jobs=-1,random_state=123,verbosity=0,
                                   **best)
    xgb_3h_final.fit(X_train,y_train)

    #store model file and metrics in DB 
    modelname = store_model(xgb_3h_final,'3H',current_time,str(etl_insertion_dt),mycursor)

    track_metrics(modelname,'3H',xgb_3h_final,X_train,y_train,mycursor,str(etl_insertion_dt),'train set')
    track_metrics(modelname,'3H',xgb_3h_final,X_test,y_test,mycursor,str(etl_insertion_dt),'baseline test')
    track_metrics(modelname,'3H',xgb_3h_final,X_val,y_val,mycursor,str(etl_insertion_dt),'baseline validation')
except:
    logger.error("Error occured in training 3H model. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#XGBoost 2 hour model training
try:
    gc.collect()
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
            
    train, test = train_test_split(final_data[cols], test_size=0.2, random_state=1546)
    X_train = train.iloc[:, 1:].values
    X_train_ind = train.iloc[:, 1:].index
    y_train = train.ART_SN00.values
    y_train_ind = train.ART_SN00.index

    X_test = test.iloc[:, 1:].values
    X_test_ind = test.iloc[:, 1:].index
    y_test = test.ART_SN00.values
    y_test_ind = test.ART_SN00.index

    X_val = validation_data[cols].iloc[:, 1:].values
    X_val_ind = validation_data[cols].iloc[:, 1:].index
    y_val = validation_data.ART_SN00.values
    y_val_ind = validation_data.ART_SN00.index

    #Bayes Optimization
    trials = Trials()  #uncommented to refer to older trials
    best = fmin(fn=xgb_regressor,
                space=params,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials,verbose=False,trials_save_file=f'./art_forecasting/XGB Models Params/2H model/trials_1H_{current_time}.p')

    best['n_estimators'] = int(best['n_estimators'])
    best['max_depth'] = int(best['max_depth'])
    best['min_child_weight'] = int(best['min_child_weight'])
    best['learning_rate'] = 10 ** best['learning_rate']
    best['subsample'] =best['subsample']/10
    best['colsample_bytree'] = best['colsample_bytree']/10

    xgb_2h_final = xgb.XGBRegressor(base_score=None,eval_metric='rmse',importance_type='gain',n_jobs=-1,random_state=123,verbosity=0,
                                   **best)
    xgb_2h_final.fit(X_train,y_train)

    #store model file and metrics in DB 
    modelname = store_model(xgb_2h_final,'2H',current_time,str(etl_insertion_dt),mycursor)

    track_metrics(modelname,'2H',xgb_2h_final,X_train,y_train,mycursor,str(etl_insertion_dt),'train set')
    track_metrics(modelname,'2H',xgb_2h_final,X_test,y_test,mycursor,str(etl_insertion_dt),'baseline test')
    track_metrics(modelname,'2H',xgb_2h_final,X_val,y_val,mycursor,str(etl_insertion_dt),'baseline validation')
except:
    logger.error("Error occured in training 2H model. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
#XGBoost 1 hour model training
try:
    gc.collect()
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
            
    train, test = train_test_split(final_data[cols], test_size=0.2, random_state=1546)
    X_train = train.iloc[:, 1:].values
    X_train_ind = train.iloc[:, 1:].index
    y_train = train.ART_SN00.values
    y_train_ind = train.ART_SN00.index

    X_test = test.iloc[:, 1:].values
    X_test_ind = test.iloc[:, 1:].index
    y_test = test.ART_SN00.values
    y_test_ind = test.ART_SN00.index

    X_val = validation_data[cols].iloc[:, 1:].values
    X_val_ind = validation_data[cols].iloc[:, 1:].index
    y_val = validation_data.ART_SN00.values
    y_val_ind = validation_data.ART_SN00.index

    #Bayes Optimization
    trials = Trials()
    best = fmin(fn=xgb_regressor,
                space=params,
                algo=tpe.suggest,
                max_evals=50,
                trials=trials,verbose=False,trials_save_file=f'./art_forecasting/XGB Models Params/1H model/trials_1H_{current_time}.p')

    best['n_estimators'] = int(best['n_estimators'])
    best['max_depth'] = int(best['max_depth'])
    best['min_child_weight'] = int(best['min_child_weight'])
    best['learning_rate'] = 10 ** best['learning_rate']
    best['subsample'] =best['subsample']/10
    best['colsample_bytree'] = best['colsample_bytree']/10

    xgb_1h_final = xgb.XGBRegressor(base_score=None,eval_metric='rmse',importance_type='gain',n_jobs=-1,random_state=123,verbosity=0,
                                   **best)
    xgb_1h_final.fit(X_train,y_train)

    #store model file and metrics in DB 
    modelname = store_model(xgb_1h_final,'1H',current_time,str(etl_insertion_dt),mycursor)

    track_metrics(modelname,'1H',xgb_1h_final,X_train,y_train,mycursor,str(etl_insertion_dt),'train set')
    track_metrics(modelname,'1H',xgb_1h_final,X_test,y_test,mycursor,str(etl_insertion_dt),'baseline test')
    track_metrics(modelname,'1H',xgb_1h_final,X_val,y_val,mycursor,str(etl_insertion_dt),'baseline validation')
except:
    logger.error("Error occured in training 1H model. "+str(sys.exc_info()[1]))
    mycursor.close()
    mydb.close()
    
mycursor.close()
mydb.close()
logger.info(f"Model training script completed!")