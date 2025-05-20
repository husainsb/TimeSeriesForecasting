import datetime
import numpy as np
import pandas as pd
import logging
import sys
from hyperopt import hp

current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")

cols_list = ['metricid','metricname','metricpath','frequency','servername','starttimeinmillis','occurrences','current','min','max','userange','count','sum','value','datetimestamp_id']

#these are APIs for each table to be created in MariaDB
tab_dict = {"avg_response_time_summary_ms":"API",
            "avg_response_time_ms":"API",
            "calls_per_minute_summary":"API",
            "calls_per_minute":"API",
            "errors_per_minute_summary":"API",
            "errors_per_minute":"API",
            "exceptions_per_minue_summary":"API",
            "exceptions_per_minue":"API",
            "number_of_slow_calls_summary":"API",
            "number_of_slow_calls":"API",
            "number_of_very_slow_calls_summary":"API",
            "number_of_very_slow_calls":"API",
            "hardware_resources_cpu_busy_summary":"API",
            "hardware_resources_memory_used_summary":"API",
            "threads_hogging_cpu":"API",
            "hardware_resources_network_bond2_incoming_kb_sec_summary":"API",
            "number_of_bytes_read_from_socket_summary":"API",
            "number_of_bytes_read_from_socket":"API",
            "number_of_bytes_written_to_socket_summary":"API",
            "number_of_bytes_written_to_socket":"API",
            "socket_read_time_summary":"API",
            "socket_read_time":"API",
            "socket_reads_summary":"API",
            "socket_reads":"API",
            "socket_write_time_summary":"API",
            "socket_write_time":"API",
            "socket_writes_summary":"API",
            "socket_writes":"API"
           }

main_tables = ["avg_response_time_ms","calls_per_minute","errors_per_minute",
               "exceptions_per_minue","number_of_slow_calls","number_of_very_slow_calls"]

cols_dict={"avg_response_time_summary_ms":"ART",
            "avg_response_time_ms":"ART",
            "calls_per_minute_summary":"CPM",
            "calls_per_minute":"CPM",
            "errors_per_minute_summary":"EPM",
            "errors_per_minute":"EPM",
            "exceptions_per_minue_summary":"EXC",
            "exceptions_per_minue":"EXC",
            "number_of_slow_calls_summary":"SLW",
            "number_of_slow_calls":"SLW",
            "number_of_very_slow_calls_summary":"VSLW",
            "number_of_very_slow_calls":"VSLW"
            }

server_dict = {'ABP':'00',
              'ABPServer1':'01',
              'ABPServer2':'02',
              'ABPServer3':'03',
              'ABPServer4':'04',
              'ABPServer5':'05',
              'ABPServer6':'06',
              'CollectionServer':'CS'}

dashboard_metrics = {'ART':'Avg Response time',
                    'CPM':'Calls/min',
                    'EPM':'Errors/min',
                    'EXC':'Exceptions/min',
                    'SLW':'#Slow Calls',
                    'VSLW':'#Very Slow Calls'}
#XGBoost parameter search space for hyperopt
params = {"n_estimators":  hp.quniform("n_estimators",100, 2000,50),
        'max_depth': hp.quniform('max_depth',3, 15,1),
        'reg_alpha': hp.uniform('reg_alpha',0,50),
        'reg_lambda': hp.uniform('reg_lambda',0,50),
        'min_child_weight': hp.quniform('min_child_weight',5, 30,1),
        "learning_rate":hp.uniform('learning_rate',-3.0, 0.0),
        "gamma": hp.uniform('gamma',0,50),                                            
        "subsample":hp.quniform('subsample',3,10,1),
        "colsample_bytree":hp.quniform('colsample_bytree',3,8,1)
        }  

# Logger
logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "minimal": {"format": "%(message)s"},
        "detailed": {
            "format": "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d]\n%(message)s\n"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "minimal",
            "level": logging.DEBUG,
        },
        "info": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/home/cdsw/art_forecasting/logs/"+"info.log",
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.INFO,
        },
        "error": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/home/cdsw/art_forecasting/logs/"+"error.log",
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.ERROR,
        },
        "debug": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/home/cdsw/art_forecasting/logs/"+"debug.log",
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.DEBUG,
        }
    },
    "loggers": {
        "root": {
            "handlers": ["console","info", "error"],  #add 'console' if needs to be diplayed on console
            "level": logging.INFO,
            "propagate": True,
        },
    },
}

