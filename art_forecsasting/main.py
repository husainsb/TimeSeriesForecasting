import os
import sys
os.chdir("/home/cdsw/")
from art_forecasting import *
import art_forecasting.fetch_hourly_data as fetch_hourly_data
import art_forecasting.transform_predict as transform_predict
import art_forecasting.dashboard_refresh as dashboard_refresh

if __name__ == '__main__':
    #Create session
    mydb,mycursor = set_db_session()
    #generate access token
    headers = fetch_token_header()
    
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
    etl_insertion_dt = pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'))
    
    try:
        fetch_hourly_data.execute(mydb,mycursor,headers,current_timestamp,etl_insertion_dt)
        transform_predict.execute(mydb,mycursor)
        dashboard_refresh.execute(mydb,mycursor,current_timestamp)
    except Exception as err:
        logger.error("Exception occured in submodules: "+str(err))
    except:
        logger.error("Undefined exception occured: "+str(sys.exc_info()[1]))
    finally:
        mycursor.close()
        mydb.close()
        