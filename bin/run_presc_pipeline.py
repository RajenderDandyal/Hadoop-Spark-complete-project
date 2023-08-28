### Import all the necessary Modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import sys
import logging
import logging.config
import os
from subprocess import Popen, PIPE

from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
from presc_run_data_extraction import extract_files
from presc_run_data_persist import data_persist_hive, data_persist_postgre

### Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
        logging.info("main() is started ...")
        ### Get Spark Object
        spark = get_spark_object(gav.envn,gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        ### Initiate presc_run_data_ingest Script
        # Load the City File
        # for file in os.listdir(gav.staging_dim_city):
        #     print("File is "+ file)
        #     file_dir = gav.staging_dim_city + '/' + file
        #     print(file_dir)
        #     if file.split('.')[1] == 'csv' :
        #         file_format = 'csv'
        #         header=gav.header
        #         inferSchema=gav.inferSchema
        #     elif file.split('.')[1] == 'parquet' :
        #         file_format = 'parquet'
        #         header='NA'
        #         inferSchema='NA'
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', gav.staging_dim_city], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        print(out)
        if 'parquet' in out.decode():
            file_format = 'parquet'
            header='NA'
            inferSchema= 'NA'
        elif b'csv' in out:
            file_format = 'csv'
            header=gav.header
            inferSchema = gav.inferSchema

        df_city = load_files(spark = spark, file_dir = gav.staging_dim_city, file_format =file_format , header =header, inferSchema = inferSchema)

        # Load the Prescriber Fact File
        # for file in os.listdir(gav.staging_fact):
        #     print("File is "+ file)
        #     file_dir = gav.staging_fact + '/' + file
        #     print(file_dir)
        #     if file.split('.')[1] == 'csv' :
        #         file_format = 'csv'
        #         header=gav.header
        #         inferSchema=gav.inferSchema
        #     elif file.split('.')[1] == 'parquet' :
        #         file_format = 'parquet'
        #         header='NA'
        #         inferSchema='NA'
        proc2 = Popen(['hdfs', 'dfs', '-ls', '-C', gav.staging_fact], stdout=PIPE, stderr=PIPE)
        (out, err) = proc2.communicate()
        print(out)
        if 'parquet' in out.decode():
            file_format = 'parquet'
            header='NA'
            inferSchema= 'NA'
        elif 'csv' in out.decode():
            file_format = 'csv'
            header=gav.header
            inferSchema = gav.inferSchema

        df_fact = load_files(spark = spark, file_dir = gav.staging_fact, file_format =file_format , header =header, inferSchema = inferSchema)

        ### Validate run_data_ingest script for city Dimension & Prescriber Fact dataframe
        df_count(df_city,'df_city')
        df_top10_rec(df_city,'df_city')

        df_count(df_fact,'df_fact')
        df_top10_rec(df_fact,'df_fact')

        ### Initiate presc_run_data_preprocessing Script
        ## Perform data Cleaning Operations for df_city and df_fact
        df_city_sel,df_fact_sel = perform_data_clean(df_city,df_fact)

        #Validation for df_city and df_fact
        df_top10_rec(df_city_sel,'df_city_sel')
        df_top10_rec(df_fact_sel,'df_fact_sel')
        df_print_schema(df_fact_sel,'df_fact_sel')

        ### Initiate presc_run_data_transform Script
        df_city_final = city_report(df_city_sel,df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)

        #Validation for df_city_final
        df_top10_rec(df_city_final,'df_city_final')
        df_print_schema(df_city_final,'df_city_final')
        df_top10_rec(df_presc_final,'df_presc_final')
        df_print_schema(df_presc_final,'df_presc_final')

        ### Initiate run_data_extraction Script
        CITY_PATH=gav.output_city
        extract_files(df_city_final, "json", CITY_PATH,1,False,"bzip2")
        FACT_PATH=gav.output_fact
        extract_files(df_presc_final, "orc", FACT_PATH, 2, False, 'snappy')

        # persist data to Hive
        data_persist_hive(spark = spark, df = df_presc_final,dfName = 'df_presc_final',partitionBy = 'delivery_date',mode='append')
        data_persist_hive(spark = spark, df = df_city_final ,dfName = 'df_city_final' ,partitionBy = 'delivery_date',mode='append')        
# Persist data at Postgre
        data_persist_postgre(spark=spark, df=df_city_final, dfName='df_city_final', url="jdbc:postgresql://localhost:6432/prespipeline", driver="org.postgresql.Driver", dbtable='df_city_final', mode="append", user=gav.user, password=gav.password)

        data_persist_postgre(spark=spark, df=df_presc_final, dfName='df_presc_final', url="jdbc:postgresql://localhost:6432/prespipeline", driver="org.postgresql.Driver", dbtable='df_presc_final', mode="append", user=gav.user, password=gav.password)
        ### End of Application Part 1
        logging.info("presc_run_pipeline.py is Completed.")

    except Exception as exp:
        logging.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
              "and fix it." +str(exp),exc_info=True)
        sys.exit(1)

if __name__ == "__main__" :
    logging.info("run_presc_pipeline is Started ...")
    main()