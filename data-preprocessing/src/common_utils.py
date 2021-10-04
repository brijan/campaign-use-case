
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, element_at, input_file_name, regexp_extract, split
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType



# Add a new column and extract a string as per the regular expression from an existing column
def extract_value_from_text(data_frame :DataFrame, new_column_name : str, from_column_name : str, reg_ex : str) :
    return data_frame.withColumn(
        new_column_name,
        regexp_extract(
            col(from_column_name),
            reg_ex,
            1
        )
    )

def read_from_csv(file_path : str, spark : SparkSession, schema : StructType,header=False):
    return spark.read \
        .option("header", header) \
        .schema(schema)\
        .csv(file_path)
    
def read_file_as_whole_text_file(all_file_Path : str, spark : SparkSession):
    return spark.sparkContext.\
        wholeTextFiles(all_file_Path)

# This function reads the csv file and add a column called filename
def read_data_with_file_name_csv(file_path : str, spark : SparkSession, schema : StructType, header=False):
    return read_from_csv(file_path, spark, schema, header)\
        .withColumn("file_name",
            input_file_name()) \
        .withColumn(
            "file_name",
            element_at(
                split(
                    element_at(
                        split(
                            col("file_name"),
                            '/'
                        ),
                        -1
                    ),
                    '\.'
                ),
                1
            )
    )

def write_single_file(df: DataFrame, column_name : str):
        df.repartition(1)\
        .write\
        .mode("append")\
        .option("header", "true")\
        .partitionBy(column_name)\
        .csv('./output')
    

    