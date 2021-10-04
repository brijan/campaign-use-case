
from data_schema import  get_schema
from configure_spark import configure_spark
from common_utils import read_data_with_file_name_csv
import os, sys
from data_transformations import get_quater_of_hour


import json


def main():
    #required to from from windows
    os.environ['HADOOP_HOME'] = "C:/Users/Birju/OneDrive/Desktop/DE_Dataset_Brijan/hadoop"
    sys.path.append("C:/Users/Birju/OneDrive/Desktop/DE_Dataset_Brijan/hadoop")
    # reading config
    with open("../config.json") as json_data_file:
        config = json.load(json_data_file)
    
    spark=configure_spark(config)
    clicks='clicks'
    conversions='conversions'
    impressions='impressions'
    clicks_df=read_data_with_file_name_csv(
        config['application_config']['input_path']+clicks+'*.csv',
        spark,
        get_schema(clicks),
        header=True
    ).transform(get_quater_of_hour)

    conversions_df=read_data_with_file_name_csv(
        config['application_config']['input_path']+conversions+'*.csv',
        spark,
        get_schema(conversions),
        header=True
    ).transform(get_quater_of_hour)

    impressions_df=read_data_with_file_name_csv(
        config['application_config']['input_path']+impressions+'*.csv',
        spark,
        get_schema(impressions),
        header=True
    ).transform(get_quater_of_hour)

         
    spark.sparkContext.stop()

main()

