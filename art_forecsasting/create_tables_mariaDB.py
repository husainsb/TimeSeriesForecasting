import os
import sys
os.chdir("/home/cdsw/")
from art_forecasting import *

if __name__ == '__main__':
    #Create session
    mydb,mycursor = set_db_session()
    #Create tables one at a time 
    try:
        mycursor.execute("CREATE TABLE avg_response_time_ms \
                        (metricId BIGINT, \
                        metricsummary BOOLEAN, \
                        metricName varchar(100), \
                        metricPath varchar(200), \
                        frequency varchar(50), \
                        servername varchar(50), \
                        starttimeinmillis	BIGINT(8), \
                        occurrences	int(10), \
                        current	int(10), \
                        min	int(10), \
                        max	int(10), \
                        userange	tinyint(1), \
                        count	int(10), \
                        sum	BIGINT, \
                        value	BIGINT, \
                        Datetimestamp_ID TIMESTAMP NULL, \
                        predicted_flag char(1))")
                        
        mycursor.execute("CREATE TABLE calls_per_minute AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE calls_per_minute_summary AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE errors_per_minute AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE errors_per_minute_summary AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE exceptions_per_minue AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE exceptions_per_minue_summary AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE number_of_slow_calls AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE number_of_slow_calls_summary AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE number_of_very_slow_calls AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE number_of_very_slow_calls_summary AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE hardware_resources_cpu_busy_summary AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE hardware_resources_memory_used AS SELECT * FROM avg_response_time_ms")
        mycursor.execute("CREATE TABLE threads_hogging_cpu AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE threads_hogging_cpu_summary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE hardware_resources_network_bond2_incoming_kb_sec_sumary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE number_of_bytes_read_from_socket AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE number_of_bytes_written_to_socket AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_read_time AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_reads AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_write_time AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_writes AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE avg_response_time_summary_ms AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE number_of_bytes_read_from_socket_summary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE number_of_bytes_written_to_socket_summary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_read_time_summary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_reads_summary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_write_time_summary AS SELECT * FROM avg_response_time_ms where false")
        mycursor.execute("CREATE TABLE socket_writes_summary AS SELECT * FROM avg_response_time_ms where false")
                   
        mycursor.execute("CREATE TABLE tab_max_dates (max_dt TIMESTAMP NULL, table_name varchar(100), etl_insertion_dt TIMESTAMP NULL, records_inserted BIGINT,last_available_dt TIMESTAMP NULL")

        mycursor.execute("CREATE TABLE track_metrics \
                        (modelname varchar(100), \
                        modeltype varchar(10), \
                        r2 FLOAT(5,2), \
                        rmse FLOAT(10,2), \
                        cnt_records INT(10), \
                        remarks varchar(100), \
                        etl_insertion_dt TIMESTAMP NULL)")
                        
        mycursor.execute("CREATE TABLE model_versions \
                        (modelname varchar(100), \
                        modeltype varchar(10), \
                        file_path varchar(200), \
                        etl_insertion_dt TIMESTAMP NULL)")
                        
        mycursor.execute("CREATE TABLE columns_transform_specs \
                        (colname varchar(100), \
                        outlier_cutoff FLOAT(12,2))")
                        
        mycursor.execute("CREATE TABLE transformed_df \
                        (DATETIMESTAMP_ID TIMESTAMP NULL, \
                        ART_SN00	DOUBLE(20,16), \
                        DAY_OF_WEEK	CHAR(1), \
                        WEEKEND_FLG	CHAR(1), \
                        ART_SN01	DOUBLE(20,16), \
                        ART_SN02	DOUBLE(20,16), \
                        ART_SN03	DOUBLE(20,16), \
                        ART_SN04	DOUBLE(20,16), \
                        ART_SN05	DOUBLE(20,16), \
                        ART_SN06	DOUBLE(20,16), \
                        ART_SNCS	DOUBLE(20,16), \
                        ART_SN00_L1H	DOUBLE(20,16), \
                        ART_SN01_L1H	DOUBLE(20,16), \
                        ART_SN02_L1H	DOUBLE(20,16), \
                        ART_SN03_L1H	DOUBLE(20,16), \
                        ART_SN04_L1H	DOUBLE(20,16), \
                        ART_SN05_L1H	DOUBLE(20,16), \
                        ART_SN06_L1H	DOUBLE(20,16), \
                        ART_SNCS_L1H	DOUBLE(20,16), \
                        ART_SN00_L2H	DOUBLE(20,16), \
                        ART_SN01_L2H	DOUBLE(20,16), \
                        ART_SN02_L2H	DOUBLE(20,16), \
                        ART_SN03_L2H	DOUBLE(20,16), \
                        ART_SN04_L2H	DOUBLE(20,16), \
                        ART_SN05_L2H	DOUBLE(20,16), \
                        ART_SN06_L2H	DOUBLE(20,16), \
                        ART_SNCS_L2H	DOUBLE(20,16), \
                        ART_SN00_L3H	DOUBLE(20,16), \
                        ART_SN01_L3H	DOUBLE(20,16), \
                        ART_SN02_L3H	DOUBLE(20,16), \
                        ART_SN03_L3H	DOUBLE(20,16), \
                        ART_SN04_L3H	DOUBLE(20,16), \
                        ART_SN05_L3H	DOUBLE(20,16), \
                        ART_SN06_L3H	DOUBLE(20,16), \
                        ART_SNCS_L3H	DOUBLE(20,16), \
                        ART_SN00_L4H	DOUBLE(20,16), \
                        ART_SN01_L4H	DOUBLE(20,16), \
                        ART_SN02_L4H	DOUBLE(20,16), \
                        ART_SN03_L4H	DOUBLE(20,16), \
                        ART_SN04_L4H	DOUBLE(20,16), \
                        ART_SN05_L4H	DOUBLE(20,16), \
                        ART_SN06_L4H	DOUBLE(20,16), \
                        ART_SNCS_L4H	DOUBLE(20,16), \
                        ART_SN00_L5H	DOUBLE(20,16), \
                        ART_SN01_L5H	DOUBLE(20,16), \
                        ART_SN02_L5H	DOUBLE(20,16), \
                        ART_SN03_L5H	DOUBLE(20,16), \
                        ART_SN04_L5H	DOUBLE(20,16), \
                        ART_SN05_L5H	DOUBLE(20,16), \
                        ART_SN06_L5H	DOUBLE(20,16), \
                        ART_SNCS_L5H	DOUBLE(20,16), \
                        ART_SN00_L6H	DOUBLE(20,16), \
                        ART_SN01_L6H	DOUBLE(20,16), \
                        ART_SN02_L6H	DOUBLE(20,16), \
                        ART_SN03_L6H	DOUBLE(20,16), \
                        ART_SN04_L6H	DOUBLE(20,16), \
                        ART_SN05_L6H	DOUBLE(20,16), \
                        ART_SN06_L6H	DOUBLE(20,16), \
                        ART_SNCS_L6H	DOUBLE(20,16), \
                        CPM_SN00	DOUBLE(20,16), \
                        CPM_SN01	DOUBLE(20,16), \
                        CPM_SN02	DOUBLE(20,16), \
                        CPM_SN03	DOUBLE(20,16), \
                        CPM_SN04	DOUBLE(20,16), \
                        CPM_SN05	DOUBLE(20,16), \
                        CPM_SN06	DOUBLE(20,16), \
                        CPM_SNCS	DOUBLE(20,16), \
                        CPM_SN00_L1H	DOUBLE(20,16), \
                        CPM_SN01_L1H	DOUBLE(20,16), \
                        CPM_SN02_L1H	DOUBLE(20,16), \
                        CPM_SN03_L1H	DOUBLE(20,16), \
                        CPM_SN04_L1H	DOUBLE(20,16), \
                        CPM_SN05_L1H	DOUBLE(20,16), \
                        CPM_SN06_L1H	DOUBLE(20,16), \
                        CPM_SNCS_L1H	DOUBLE(20,16), \
                        CPM_SN00_L2H	DOUBLE(20,16), \
                        CPM_SN01_L2H	DOUBLE(20,16), \
                        CPM_SN02_L2H	DOUBLE(20,16), \
                        CPM_SN03_L2H	DOUBLE(20,16), \
                        CPM_SN04_L2H	DOUBLE(20,16), \
                        CPM_SN05_L2H	DOUBLE(20,16), \
                        CPM_SN06_L2H	DOUBLE(20,16), \
                        CPM_SNCS_L2H	DOUBLE(20,16), \
                        CPM_SN00_L3H	DOUBLE(20,16), \
                        CPM_SN01_L3H	DOUBLE(20,16), \
                        CPM_SN02_L3H	DOUBLE(20,16), \
                        CPM_SN03_L3H	DOUBLE(20,16), \
                        CPM_SN04_L3H	DOUBLE(20,16), \
                        CPM_SN05_L3H	DOUBLE(20,16), \
                        CPM_SN06_L3H	DOUBLE(20,16), \
                        CPM_SNCS_L3H	DOUBLE(20,16), \
                        CPM_SN00_L4H	DOUBLE(20,16), \
                        CPM_SN01_L4H	DOUBLE(20,16), \
                        CPM_SN02_L4H	DOUBLE(20,16), \
                        CPM_SN03_L4H	DOUBLE(20,16), \
                        CPM_SN04_L4H	DOUBLE(20,16), \
                        CPM_SN05_L4H	DOUBLE(20,16), \
                        CPM_SN06_L4H	DOUBLE(20,16), \
                        CPM_SNCS_L4H	DOUBLE(20,16), \
                        CPM_SN00_L5H	DOUBLE(20,16), \
                        CPM_SN01_L5H	DOUBLE(20,16), \
                        CPM_SN02_L5H	DOUBLE(20,16), \
                        CPM_SN03_L5H	DOUBLE(20,16), \
                        CPM_SN04_L5H	DOUBLE(20,16), \
                        CPM_SN05_L5H	DOUBLE(20,16), \
                        CPM_SN06_L5H	DOUBLE(20,16), \
                        CPM_SNCS_L5H	DOUBLE(20,16), \
                        CPM_SN00_L6H	DOUBLE(20,16), \
                        CPM_SN01_L6H	DOUBLE(20,16), \
                        CPM_SN02_L6H	DOUBLE(20,16), \
                        CPM_SN03_L6H	DOUBLE(20,16), \
                        CPM_SN04_L6H	DOUBLE(20,16), \
                        CPM_SN05_L6H	DOUBLE(20,16), \
                        CPM_SN06_L6H	DOUBLE(20,16), \
                        CPM_SNCS_L6H	DOUBLE(20,16), \
                        EPM_SN00	DOUBLE(20,16), \
                        EPM_SN01	DOUBLE(20,16), \
                        EPM_SN02	DOUBLE(20,16), \
                        EPM_SN03	DOUBLE(20,16), \
                        EPM_SN04	DOUBLE(20,16), \
                        EPM_SN00_L1H	DOUBLE(20,16), \
                        EPM_SN01_L1H	DOUBLE(20,16), \
                        EPM_SN02_L1H	DOUBLE(20,16), \
                        EPM_SN03_L1H	DOUBLE(20,16), \
                        EPM_SN04_L1H	DOUBLE(20,16), \
                        EPM_SN00_L2H	DOUBLE(20,16), \
                        EPM_SN01_L2H	DOUBLE(20,16), \
                        EPM_SN02_L2H	DOUBLE(20,16), \
                        EPM_SN03_L2H	DOUBLE(20,16), \
                        EPM_SN04_L2H	DOUBLE(20,16), \
                        EPM_SN00_L3H	DOUBLE(20,16), \
                        EPM_SN01_L3H	DOUBLE(20,16), \
                        EPM_SN02_L3H	DOUBLE(20,16), \
                        EPM_SN03_L3H	DOUBLE(20,16), \
                        EPM_SN04_L3H	DOUBLE(20,16), \
                        EPM_SN00_L4H	DOUBLE(20,16), \
                        EPM_SN01_L4H	DOUBLE(20,16), \
                        EPM_SN02_L4H	DOUBLE(20,16), \
                        EPM_SN03_L4H	DOUBLE(20,16), \
                        EPM_SN04_L4H	DOUBLE(20,16), \
                        EPM_SN00_L5H	DOUBLE(20,16), \
                        EPM_SN01_L5H	DOUBLE(20,16), \
                        EPM_SN02_L5H	DOUBLE(20,16), \
                        EPM_SN03_L5H	DOUBLE(20,16), \
                        EPM_SN04_L5H	DOUBLE(20,16), \
                        EPM_SN00_L6H	DOUBLE(20,16), \
                        EPM_SN01_L6H	DOUBLE(20,16), \
                        EPM_SN02_L6H	DOUBLE(20,16), \
                        EPM_SN03_L6H	DOUBLE(20,16), \
                        EPM_SN04_L6H	DOUBLE(20,16), \
                        EXC_SN00	DOUBLE(20,16), \
                        EXC_SN01	DOUBLE(20,16), \
                        EXC_SN02	DOUBLE(20,16), \
                        EXC_SN03	DOUBLE(20,16), \
                        EXC_SN04	DOUBLE(20,16), \
                        EXC_SN00_L1H	DOUBLE(20,16), \
                        EXC_SN01_L1H	DOUBLE(20,16), \
                        EXC_SN02_L1H	DOUBLE(20,16), \
                        EXC_SN03_L1H	DOUBLE(20,16), \
                        EXC_SN04_L1H	DOUBLE(20,16), \
                        EXC_SN00_L2H	DOUBLE(20,16), \
                        EXC_SN01_L2H	DOUBLE(20,16), \
                        EXC_SN02_L2H	DOUBLE(20,16), \
                        EXC_SN03_L2H	DOUBLE(20,16), \
                        EXC_SN04_L2H	DOUBLE(20,16), \
                        EXC_SN00_L3H	DOUBLE(20,16), \
                        EXC_SN01_L3H	DOUBLE(20,16), \
                        EXC_SN02_L3H	DOUBLE(20,16), \
                        EXC_SN03_L3H	DOUBLE(20,16), \
                        EXC_SN04_L3H	DOUBLE(20,16), \
                        EXC_SN00_L4H	DOUBLE(20,16), \
                        EXC_SN01_L4H	DOUBLE(20,16), \
                        EXC_SN02_L4H	DOUBLE(20,16), \
                        EXC_SN03_L4H	DOUBLE(20,16), \
                        EXC_SN04_L4H	DOUBLE(20,16), \
                        EXC_SN00_L5H	DOUBLE(20,16), \
                        EXC_SN01_L5H	DOUBLE(20,16), \
                        EXC_SN02_L5H	DOUBLE(20,16), \
                        EXC_SN03_L5H	DOUBLE(20,16), \
                        EXC_SN04_L5H	DOUBLE(20,16), \
                        EXC_SN00_L6H	DOUBLE(20,16), \
                        EXC_SN01_L6H	DOUBLE(20,16), \
                        EXC_SN02_L6H	DOUBLE(20,16), \
                        EXC_SN03_L6H	DOUBLE(20,16), \
                        EXC_SN04_L6H	DOUBLE(20,16), \
                        SLW_SN00	DOUBLE(20,16), \
                        SLW_SN01	DOUBLE(20,16), \
                        SLW_SN02	DOUBLE(20,16), \
                        SLW_SN03	DOUBLE(20,16), \
                        SLW_SN04	DOUBLE(20,16), \
                        SLW_SN00_L1H	DOUBLE(20,16), \
                        SLW_SN01_L1H	DOUBLE(20,16), \
                        SLW_SN02_L1H	DOUBLE(20,16), \
                        SLW_SN03_L1H	DOUBLE(20,16), \
                        SLW_SN04_L1H	DOUBLE(20,16), \
                        SLW_SN00_L2H	DOUBLE(20,16), \
                        SLW_SN01_L2H	DOUBLE(20,16), \
                        SLW_SN02_L2H	DOUBLE(20,16), \
                        SLW_SN03_L2H	DOUBLE(20,16), \
                        SLW_SN04_L2H	DOUBLE(20,16), \
                        SLW_SN00_L3H	DOUBLE(20,16), \
                        SLW_SN01_L3H	DOUBLE(20,16), \
                        SLW_SN02_L3H	DOUBLE(20,16), \
                        SLW_SN03_L3H	DOUBLE(20,16), \
                        SLW_SN04_L3H	DOUBLE(20,16), \
                        SLW_SN00_L4H	DOUBLE(20,16), \
                        SLW_SN01_L4H	DOUBLE(20,16), \
                        SLW_SN02_L4H	DOUBLE(20,16), \
                        SLW_SN03_L4H	DOUBLE(20,16), \
                        SLW_SN04_L4H	DOUBLE(20,16), \
                        SLW_SN00_L5H	DOUBLE(20,16), \
                        SLW_SN01_L5H	DOUBLE(20,16), \
                        SLW_SN02_L5H	DOUBLE(20,16), \
                        SLW_SN03_L5H	DOUBLE(20,16), \
                        SLW_SN04_L5H	DOUBLE(20,16), \
                        SLW_SN00_L6H	DOUBLE(20,16), \
                        SLW_SN01_L6H	DOUBLE(20,16), \
                        SLW_SN02_L6H	DOUBLE(20,16), \
                        SLW_SN03_L6H	DOUBLE(20,16), \
                        SLW_SN04_L6H	DOUBLE(20,16), \
                        VSLW_SN00	DOUBLE(20,16), \
                        VSLW_SN01	DOUBLE(20,16), \
                        VSLW_SN02	DOUBLE(20,16), \
                        VSLW_SN03	DOUBLE(20,16), \
                        VSLW_SN04	DOUBLE(20,16), \
                        VSLW_SN00_L1H	DOUBLE(20,16), \
                        VSLW_SN01_L1H	DOUBLE(20,16), \
                        VSLW_SN02_L1H	DOUBLE(20,16), \
                        VSLW_SN03_L1H	DOUBLE(20,16), \
                        VSLW_SN04_L1H	DOUBLE(20,16), \
                        VSLW_SN00_L2H	DOUBLE(20,16), \
                        VSLW_SN01_L2H	DOUBLE(20,16), \
                        VSLW_SN02_L2H	DOUBLE(20,16), \
                        VSLW_SN03_L2H	DOUBLE(20,16), \
                        VSLW_SN04_L2H	DOUBLE(20,16), \
                        VSLW_SN00_L3H	DOUBLE(20,16), \
                        VSLW_SN01_L3H	DOUBLE(20,16), \
                        VSLW_SN02_L3H	DOUBLE(20,16), \
                        VSLW_SN03_L3H	DOUBLE(20,16), \
                        VSLW_SN04_L3H	DOUBLE(20,16), \
                        VSLW_SN00_L4H	DOUBLE(20,16), \
                        VSLW_SN01_L4H	DOUBLE(20,16), \
                        VSLW_SN02_L4H	DOUBLE(20,16), \
                        VSLW_SN03_L4H	DOUBLE(20,16), \
                        VSLW_SN04_L4H	DOUBLE(20,16), \
                        VSLW_SN00_L5H	DOUBLE(20,16), \
                        VSLW_SN01_L5H	DOUBLE(20,16), \
                        VSLW_SN02_L5H	DOUBLE(20,16), \
                        VSLW_SN03_L5H	DOUBLE(20,16), \
                        VSLW_SN04_L5H	DOUBLE(20,16), \
                        VSLW_SN00_L6H	DOUBLE(20,16), \
                        VSLW_SN01_L6H	DOUBLE(20,16), \
                        VSLW_SN02_L6H	DOUBLE(20,16), \
                        VSLW_SN03_L6H	DOUBLE(20,16), \
                        VSLW_SN04_L6H	DOUBLE(20,16), \
                        HR_VAL	DOUBLE(20,16), \
                        ART_SN00_L6H_MA12	DOUBLE(20,16), \
                        ART_SN00_L5H_MA12	DOUBLE(20,16), \
                        ART_SN00_L4H_MA12	DOUBLE(20,16), \
                        ART_SN00_L3H_MA12	DOUBLE(20,16), \
                        ART_SN00_L2H_MA12	DOUBLE(20,16), \
                        ART_SN00_L1H_MA12	DOUBLE(20,16), \
                        ART_SN01_L6H_MA12	DOUBLE(20,16), \
                        ART_SN01_L5H_MA12	DOUBLE(20,16), \
                        ART_SN01_L4H_MA12	DOUBLE(20,16), \
                        ART_SN01_L3H_MA12	DOUBLE(20,16), \
                        ART_SN01_L2H_MA12	DOUBLE(20,16), \
                        ART_SN01_L1H_MA12	DOUBLE(20,16), \
                        ART_SN02_L6H_MA12	DOUBLE(20,16), \
                        ART_SN02_L5H_MA12	DOUBLE(20,16), \
                        ART_SN02_L4H_MA12	DOUBLE(20,16), \
                        ART_SN02_L3H_MA12	DOUBLE(20,16), \
                        ART_SN02_L2H_MA12	DOUBLE(20,16), \
                        ART_SN02_L1H_MA12	DOUBLE(20,16), \
                        ART_SN03_L6H_MA12	DOUBLE(20,16), \
                        ART_SN03_L5H_MA12	DOUBLE(20,16), \
                        ART_SN03_L4H_MA12	DOUBLE(20,16), \
                        ART_SN03_L3H_MA12	DOUBLE(20,16), \
                        ART_SN03_L2H_MA12	DOUBLE(20,16), \
                        ART_SN03_L1H_MA12	DOUBLE(20,16), \
                        ART_SN04_L6H_MA12	DOUBLE(20,16), \
                        ART_SN04_L5H_MA12	DOUBLE(20,16), \
                        ART_SN04_L4H_MA12	DOUBLE(20,16), \
                        ART_SN04_L3H_MA12	DOUBLE(20,16), \
                        ART_SN04_L2H_MA12	DOUBLE(20,16), \
                        ART_SN04_L1H_MA12	DOUBLE(20,16), \
                        ART_SN05_L6H_MA12	DOUBLE(20,16), \
                        ART_SN05_L5H_MA12	DOUBLE(20,16), \
                        ART_SN05_L4H_MA12	DOUBLE(20,16), \
                        ART_SN05_L3H_MA12	DOUBLE(20,16), \
                        ART_SN05_L2H_MA12	DOUBLE(20,16), \
                        ART_SN05_L1H_MA12	DOUBLE(20,16), \
                        ART_SN06_L6H_MA12	DOUBLE(20,16), \
                        ART_SN06_L5H_MA12	DOUBLE(20,16), \
                        ART_SN06_L4H_MA12	DOUBLE(20,16), \
                        ART_SN06_L3H_MA12	DOUBLE(20,16), \
                        ART_SN06_L2H_MA12	DOUBLE(20,16), \
                        ART_SN06_L1H_MA12	DOUBLE(20,16), \
                        ART_SNCS_L6H_MA12	DOUBLE(20,16), \
                        ART_SNCS_L5H_MA12	DOUBLE(20,16), \
                        ART_SNCS_L4H_MA12	DOUBLE(20,16), \
                        ART_SNCS_L3H_MA12	DOUBLE(20,16), \
                        ART_SNCS_L2H_MA12	DOUBLE(20,16), \
                        ART_SNCS_L1H_MA12	DOUBLE(20,16), \
                        CPM_SN00_L6H_MA12	DOUBLE(20,16), \
                        CPM_SN00_L5H_MA12	DOUBLE(20,16), \
                        CPM_SN00_L4H_MA12	DOUBLE(20,16), \
                        CPM_SN00_L3H_MA12	DOUBLE(20,16), \
                        CPM_SN00_L2H_MA12	DOUBLE(20,16), \
                        CPM_SN00_L1H_MA12	DOUBLE(20,16), \
                        EPM_SN00_L6H_MA12	DOUBLE(20,16), \
                        EPM_SN00_L5H_MA12	DOUBLE(20,16), \
                        EPM_SN00_L4H_MA12	DOUBLE(20,16), \
                        EPM_SN00_L3H_MA12	DOUBLE(20,16), \
                        EPM_SN00_L2H_MA12	DOUBLE(20,16), \
                        EPM_SN00_L1H_MA12	DOUBLE(20,16), \
                        EXC_SN00_L6H_MA12	DOUBLE(20,16), \
                        EXC_SN00_L5H_MA12	DOUBLE(20,16), \
                        EXC_SN00_L4H_MA12	DOUBLE(20,16), \
                        EXC_SN00_L3H_MA12	DOUBLE(20,16), \
                        EXC_SN00_L2H_MA12	DOUBLE(20,16), \
                        EXC_SN00_L1H_MA12	DOUBLE(20,16), \
                        SLW_SN00_L6H_MA12	DOUBLE(20,16), \
                        SLW_SN00_L5H_MA12	DOUBLE(20,16), \
                        SLW_SN00_L4H_MA12	DOUBLE(20,16), \
                        SLW_SN00_L3H_MA12	DOUBLE(20,16), \
                        SLW_SN00_L2H_MA12	DOUBLE(20,16), \
                        SLW_SN00_L1H_MA12	DOUBLE(20,16), \
                        VSLW_SN00_L6H_MA12	DOUBLE(20,16), \
                        VSLW_SN00_L5H_MA12	DOUBLE(20,16), \
                        VSLW_SN00_L4H_MA12	DOUBLE(20,16), \
                        VSLW_SN00_L3H_MA12	DOUBLE(20,16), \
                        VSLW_SN00_L2H_MA12	DOUBLE(20,16), \
                        VSLW_SN00_L1H_MA12	DOUBLE(20,16), \
                        ART_SN00_L6H_MA3	DOUBLE(20,16), \
                        ART_SN00_L5H_MA3	DOUBLE(20,16), \
                        ART_SN00_L4H_MA3	DOUBLE(20,16), \
                        ART_SN00_L3H_MA3	DOUBLE(20,16), \
                        ART_SN00_L2H_MA3	DOUBLE(20,16), \
                        ART_SN00_L1H_MA3	DOUBLE(20,16), \
                        ART_SN01_L6H_MA3	DOUBLE(20,16), \
                        ART_SN01_L5H_MA3	DOUBLE(20,16), \
                        ART_SN01_L4H_MA3	DOUBLE(20,16), \
                        ART_SN01_L3H_MA3	DOUBLE(20,16), \
                        ART_SN01_L2H_MA3	DOUBLE(20,16), \
                        ART_SN01_L1H_MA3	DOUBLE(20,16), \
                        ART_SN02_L6H_MA3	DOUBLE(20,16), \
                        ART_SN02_L5H_MA3	DOUBLE(20,16), \
                        ART_SN02_L4H_MA3	DOUBLE(20,16), \
                        ART_SN02_L3H_MA3	DOUBLE(20,16), \
                        ART_SN02_L2H_MA3	DOUBLE(20,16), \
                        ART_SN02_L1H_MA3	DOUBLE(20,16), \
                        ART_SN03_L6H_MA3	DOUBLE(20,16), \
                        ART_SN03_L5H_MA3	DOUBLE(20,16), \
                        ART_SN03_L4H_MA3	DOUBLE(20,16), \
                        ART_SN03_L3H_MA3	DOUBLE(20,16), \
                        ART_SN03_L2H_MA3	DOUBLE(20,16), \
                        ART_SN03_L1H_MA3	DOUBLE(20,16), \
                        ART_SN04_L6H_MA3	DOUBLE(20,16), \
                        ART_SN04_L5H_MA3	DOUBLE(20,16), \
                        ART_SN04_L4H_MA3	DOUBLE(20,16), \
                        ART_SN04_L3H_MA3	DOUBLE(20,16), \
                        ART_SN04_L2H_MA3	DOUBLE(20,16), \
                        ART_SN04_L1H_MA3	DOUBLE(20,16), \
                        ART_SN05_L6H_MA3	DOUBLE(20,16), \
                        ART_SN05_L5H_MA3	DOUBLE(20,16), \
                        ART_SN05_L4H_MA3	DOUBLE(20,16), \
                        ART_SN05_L3H_MA3	DOUBLE(20,16), \
                        ART_SN05_L2H_MA3	DOUBLE(20,16), \
                        ART_SN05_L1H_MA3	DOUBLE(20,16), \
                        ART_SN06_L6H_MA3	DOUBLE(20,16), \
                        ART_SN06_L5H_MA3	DOUBLE(20,16), \
                        ART_SN06_L4H_MA3	DOUBLE(20,16), \
                        ART_SN06_L3H_MA3	DOUBLE(20,16), \
                        ART_SN06_L2H_MA3	DOUBLE(20,16), \
                        ART_SN06_L1H_MA3	DOUBLE(20,16), \
                        ART_SNCS_L6H_MA3	DOUBLE(20,16), \
                        ART_SNCS_L5H_MA3	DOUBLE(20,16), \
                        ART_SNCS_L4H_MA3	DOUBLE(20,16), \
                        ART_SNCS_L3H_MA3	DOUBLE(20,16), \
                        ART_SNCS_L2H_MA3	DOUBLE(20,16), \
                        ART_SNCS_L1H_MA3	DOUBLE(20,16), \
                        CPM_SN00_L6H_MA3	DOUBLE(20,16), \
                        CPM_SN00_L5H_MA3	DOUBLE(20,16), \
                        CPM_SN00_L4H_MA3	DOUBLE(20,16), \
                        CPM_SN00_L3H_MA3	DOUBLE(20,16), \
                        CPM_SN00_L2H_MA3	DOUBLE(20,16), \
                        CPM_SN00_L1H_MA3	DOUBLE(20,16), \
                        EPM_SN00_L6H_MA3	DOUBLE(20,16), \
                        EPM_SN00_L5H_MA3	DOUBLE(20,16), \
                        EPM_SN00_L4H_MA3	DOUBLE(20,16), \
                        EPM_SN00_L3H_MA3	DOUBLE(20,16), \
                        EPM_SN00_L2H_MA3	DOUBLE(20,16), \
                        EPM_SN00_L1H_MA3	DOUBLE(20,16), \
                        EXC_SN00_L6H_MA3	DOUBLE(20,16), \
                        EXC_SN00_L5H_MA3	DOUBLE(20,16), \
                        EXC_SN00_L4H_MA3	DOUBLE(20,16), \
                        EXC_SN00_L3H_MA3	DOUBLE(20,16), \
                        EXC_SN00_L2H_MA3	DOUBLE(20,16), \
                        EXC_SN00_L1H_MA3	DOUBLE(20,16), \
                        SLW_SN00_L6H_MA3	DOUBLE(20,16), \
                        SLW_SN00_L5H_MA3	DOUBLE(20,16), \
                        SLW_SN00_L4H_MA3	DOUBLE(20,16), \
                        SLW_SN00_L3H_MA3	DOUBLE(20,16), \
                        SLW_SN00_L2H_MA3	DOUBLE(20,16), \
                        SLW_SN00_L1H_MA3	DOUBLE(20,16), \
                        VSLW_SN00_L6H_MA3	DOUBLE(20,16), \
                        VSLW_SN00_L5H_MA3	DOUBLE(20,16), \
                        VSLW_SN00_L4H_MA3	DOUBLE(20,16), \
                        VSLW_SN00_L3H_MA3	DOUBLE(20,16), \
                        VSLW_SN00_L2H_MA3	DOUBLE(20,16), \
                        VSLW_SN00_L1H_MA3	DOUBLE(20,16) )")
                        
        mycursor.execute("CREATE TABLE predicted_df \
                        (DATETIMESTAMP_ID TIMESTAMP PRIMARY KEY, \
                        ART_SN00	DOUBLE(20,16), \
                        pred_ART_6H DOUBLE(20,16), \
                        pred_ART_5H DOUBLE(20,16), \
                        pred_ART_4H DOUBLE(20,16), \
                        pred_ART_3H DOUBLE(20,16), \
                        pred_ART_2H DOUBLE(20,16), \
                        pred_ART_1H DOUBLE(20,16))")
                        
        mycursor.execute("CREATE TABLE bullet_df \
                        (current_hour TIMESTAMP NULL, \
                        reference	varchar(10), \
                        hour varchar(5), \
                        hour_int INT, \
                        servername varchar(15), \
                        Percentile_60 DOUBLE(20,5), \
                        Percentile_75 DOUBLE(20,5), \
                        Percentile_90 DOUBLE(20,5), \
                        Percentile_99 DOUBLE(20,5))")

        mycursor.execute("CREATE TABLE track_metrics_hourly \
                        (DATETIMESTAMP_ID TIMESTAMP PRIMARY KEY, \
                        actual	DOUBLE(20,16), \
                        rmse_6H FLOAT(10,2), \
                        rmse_5H FLOAT(10,2), \
                        rmse_4H FLOAT(10,2), \
                        rmse_3H FLOAT(10,2), \
                        rmse_2H FLOAT(10,2), \
                        rmse_1H FLOAT(10,2))")

        mycursor.execute("CREATE TABLE ART_plots_df \
                        (Datetimestamp_ID TIMESTAMP NULL, \
                        servername varchar(20), \
                        value DOUBLE(20,5), \
                        reference	varchar(10), \
                        hour varchar(5), \
                        6H DOUBLE(20,5), \
                        5H DOUBLE(20,5), \
                        4H DOUBLE(20,5), \
                        3H DOUBLE(20,5), \
                        2H DOUBLE(20,5), \
                        1H DOUBLE(20,5))")
                        
                        
        mycursor.execute("CREATE TABLE server_quantiles \
                        (50_Percentile DOUBLE(20,5), \
                        75_Percentile DOUBLE(20,5), \
                        90_Percentile DOUBLE(20,5), \
                        Mean DOUBLE(20,5), \
                        reference varchar(25), \
                        metric varchar(25), \
                        servername varchar(20), \
                        hour varchar(5))")
                        
        mycursor.execute("CREATE TABLE server_plots_df \
                        (Datetimestamp_ID TIMESTAMP NULL, \
                        servername varchar(20), \
                        value DOUBLE(20,5), \
                        reference	varchar(25), \
                        hour varchar(5), \
                        metric varchar(25))")
        #predicted_df view for SA times (GMT+2) for dashboard
        mycursor.execute("CREATE or replace view vw_predicted_df as \
                        select DATE_ADD(DATETIMESTAMP_ID,INTERVAL 2 HOUR) as DATETIMESTAMP_ID, \
                        ART_SN00,pred_ART_6H ,pred_ART_5H ,pred_ART_4H ,pred_ART_3H ,pred_ART_2H, \
                        pred_ART_1H from predicted_df")
                        
        mycursor.execute("create or replace view vw_model_performance_abp as \
        select DATETIMESTAMP_ID, ABS(pred_ART_6H-ART_SN00) model_6H_err,ABS(pred_ART_5H-ART_SN00) model_5H_err, \
        ABS(pred_ART_4H-ART_SN00) model_4H_err,ABS(pred_ART_3H-ART_SN00) model_3H_err,ABS(pred_ART_2H-ART_SN00) model_2H_err, \
        ABS(pred_ART_1H-ART_SN00) model_1H_err from predicted_df where ART_SN00 is not null order by DATETIMESTAMP_ID desc limit 720")

        mycursor.execute("create or replace view vw_model_performance_dashboard as \
        select DATETIMESTAMP_ID, '6H' model, model_6H_err as AbsoluteError from vw_model_performance_abp \
        union all \
        select DATETIMESTAMP_ID, '5H' model, model_5H_err as AbsoluteError from vw_model_performance_abp \
        union all \
        select DATETIMESTAMP_ID, '4H' model, model_4H_err as AbsoluteError from vw_model_performance_abp \
        union all \
        select DATETIMESTAMP_ID, '3H' model, model_3H_err as AbsoluteError from vw_model_performance_abp \
        union all \
        select DATETIMESTAMP_ID, '2H' model, model_2H_err as AbsoluteError from vw_model_performance_abp \
        union all \
        select DATETIMESTAMP_ID, '1H' model, model_1H_err as AbsoluteError from vw_model_performance_abp")
    except:
        logger.error("Error in table creation script 'create_tables_mariaDB.py': "+str(sys.exc_info()[1]))
    finally:
        mycursor.close()
        mydb.close()
                