
from data_schema import  get_schema
from configure_spark import configure_spark
from common_utils import read_data_with_file_name_csv
import os, sys



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
    )
         
    spark.sparkContext.stop()

main()

