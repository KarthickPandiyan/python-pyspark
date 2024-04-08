import python files
from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlfunction
from pyspark.sql.types import StringType
from datetime import dateime, date, timedelta
import pandas as pd

#open Spark Session
spark = SparkSession.builder.enableHiveSupport.getOrCreate()

#Setting spark configuarion
spark.conf.set("hive.exec.dynamic.parition","true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.source.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.caseSensitive", "false")


data_time = "%Y-%m-%d_%H:%M:%S"
date_format = "%Y-%m-%d"
today = datetime.today.strftime(date_format)
today_for_test = datetime.today().strtime("%Y%mn%d")
today = date.today()

def get_stat():
  receive = pythonfile.its_function(serverip, port, today, cntry)
  if receive:
    response
  return none

def get_parameter():
  global column1
  global column2
  global column3

  column1 = sys.arg[0]
  column2 = sys.arg[1]
  column3 = sys.arg[2]

  if sys.arg[3] == "":
    column = ""
