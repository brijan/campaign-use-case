from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, element_at, split

def get_quater_of_hour(input_df: DataFrame):
    return input_df.withColumn('quater_of_hour',
                               element_at(
                                    split(
                                        col('file_name'),
                                        '_'
                                    ),
                                    -1
                               )
    )