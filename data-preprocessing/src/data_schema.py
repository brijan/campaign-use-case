from pyspark.sql.types import *

###
# This module will contain all the schema required for 
# the given type of file
###
def get_clicks():
 return StructType([
    StructField("click_id", IntegerType()),
    StructField("banner_id", IntegerType()),
    StructField("campaign_id", IntegerType())
    ])

def get_impressions():
 return StructType([
    StructField("banner_id", IntegerType()),
    StructField("campaign_id", IntegerType())
    ])

def get_conversion():
 return  StructType([
    StructField("conversion_id", IntegerType()),
    StructField("click_id", IntegerType()),
    StructField("revenue", FloatType())
    ])


###
# A switcher fuction return the schema according to the field
###
def get_schema(data_about : str):

    get_schema_ = {
      'converions': get_conversion,
      'impressions': get_impressions,
      'clicks': get_clicks
    }
    return get_schema_.get(data_about)()   