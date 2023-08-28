from pyspark.sql import SparkSession
import os

current_path =  os.getcwd()
staging_fact = current_path + '\..\staging\\fact'
file="USA_Presc_Medicare_Data_2021.csv"
file_dir = staging_fact + '\\' + file

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('testing') \
    .getOrCreate()

df = spark. \
    read. \
    format('csv'). \
    options(header=True). \
    options(inferSchema=True). \
    load(file_dir)

print(df.count())