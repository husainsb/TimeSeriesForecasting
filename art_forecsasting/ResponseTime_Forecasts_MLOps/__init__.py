import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
import math
import seaborn as sns
import warnings
import statsmodels.api as sm

import datetime
import os
import sys
import logging
from logging.config import dictConfig
from rich.logging import RichHandler

import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn import model_selection
from sklearn.metrics import mean_squared_error
import sklearn
from sklearn.model_selection import cross_val_score

import hyperopt
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials

import gc
import pickle
import multiprocessing

from .defined_variables import *
from .defined_functions import *

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)

dictConfig(logging_config)
logger = logging.getLogger("root")
logger.handlers[0] = RichHandler(markup=True)
