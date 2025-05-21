# Network Response Time Forecasting project - MLOps

## Project Overview

This is the time series forecasting exercise. This has been inspired by a real use-case that I had done few years ago for one client. This project will show how to include MLOps in pure pythonic way.

There is a main server which is connected to 7 different sub-servers. This main server recieves the load for these sub-servers. The aim of this project is forecast next 6 hours of Network's Average Response time (ART) of main server.

This ML pipeline is designed to run every hour and forecast for next 6 hours. It uses XGBoost model with hyperparameters tuned.It runs 6 different models every hour.

All data is stored in MariaDB, which can be used to create dashboards for visualization purpose.

There are majorly 3 steps in this end-to-end ML pipeline:
1. Fetching the data
2. Transforming and Forecasting
3. Dashboard Refresh

*By forecasting beforehand, it enables the operations analysts to prepare & take remiadiation steps before experiencing spikes in ART.*

## Files & Folders

* `README.md` -- This project's readme in Markdown format.
* `requirements.txt` -- Dependencies that need to be manually installed if you are running
    project/scripts in ML Runtimes, especially while initiating the new docker runtime.
* `art_forecasting` -- This contains all source codes, model hyperparameters & model/pickle
    objects to run this project. Also this folder act as package name for Python session.
* `art_forecasting/logs` -- Folder which contains logs for project session run.
* `art_forecasting/models` -- Folder which contains all baseline hourly model objects.
* `art_forecasting/XGB Models Params` -- Folder which contains all baseline hourly model hyperparameters 
   and feature scaling object.
* `art_forecasting/appdynamics-config.yml` -- YAML config file to define database and server's
    REST API access token configs
* `art_forecasting/__init__.py` -- Package init file to initialize all required packages for this project
* `art_forecasting/create_tables_mariaDB.py` -- File to create required tables in MariaDB.
    Used only **one time** especially during new database setup.
* `art_forecasting/defined_variables.py` -- Source file that contains all user-defined variables
    (like dictionaries)
* `art_forecasting/defined_functions.py` -- Source file that contains all user-defined functions used
    in transforming the data
* `art_forecasting/main.py` -- Main file which triggers all orchestration steps of this project.
    Right from fetching new data till refreshing dashboard is handled here. This file is triggered
    from Jobs section **ART_forecasting**.
* `art_forecasting/fetch_hourly_data.py` -- File to fetch new data from server REST API and 
    load it to database.
* `art_forecasting/transform_predict.py` -- File to transform new data in required format and
    forecast using new data. The six models are executed on new data and forecasted data is
    stored in the database.
* `art_forecasting/dashboard_refresh.py` -- File to refresh the tables which contains the data for
    dashboard/app visuals. This marks the end of the entire automated session.
* `art_forecasting/train_model.py` -- File to train all 6 models manually if data or model drift is 
    observed. This file is not part of orchestration/ML pipeline, it has to be run manually if required.

## Things to remember
*   Current level of logging is set to 'INFO', hence only info & errors will be captured in logs.
    For debugging, set value of `level` to `logging.DEBUG` in file `art_forecasting/defined_variables.py`
    in *Logger* section.
*   For any change in database credentials or REST API request credentials make necessary
    changes in `art_forecasting/appdynamics-config.yml`
*   Jobs can be scheduled to run using any scheduler or frameworks like Airflow.
    
